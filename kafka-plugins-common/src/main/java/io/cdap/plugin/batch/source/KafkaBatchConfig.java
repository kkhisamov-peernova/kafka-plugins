package io.cdap.plugin.batch.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Config properties for the plugin.
 */
public class KafkaBatchConfig extends ReferencePluginConfig {

  @Description("Kafka topic to read from.")
  @Macro
  private String topic;

  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private String kafkaBrokers;

  @Description("A directory path to store the latest Kafka offsets. " +
    "A file named with the pipeline name will be created under the given directory.")
  @Macro
  @Nullable
  private String offsetDir;

  @Description("A comma separated list of topic partitions to read from. " +
    "If not specified, all partitions will be read.")
  @Macro
  @Nullable
  private String partitions;

  @Description("The initial offset for each topic partition in partition1:offset1,partition2:offset2 form. " +
    "These offsets will only be used for the first run of the pipeline. " +
    "Any subsequent run will read from the latest offset from previous run." +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. " +
    "If not specified, the initial run will start reading from the latest message in Kafka.")
  @Nullable
  @Macro
  private String initialPartitionOffsets;

  @Description("The maximum of messages the source will read from each topic partition. If the current topic " +
    "partition does not have this number of messages, the source will read to the latest offset. " +
    "Note that this is an estimation, the actual number of messages the source read may be smaller than this number.")
  @Nullable
  @Macro
  private Long maxNumberRecords;

  @Description("Output schema of the source, including the timeField and keyField. " +
    "The fields excluding keyField are used in conjunction with the format " +
    "to parse Kafka payloads.")
  private String schema;

  @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
    "If no format is given, Kafka message payloads will be treated as bytes.")
  @Nullable
  private String format;

  @Description("Optional name of the field containing the message key. " +
    "If this is not set, no key field will be added to output records. " +
    "If set, this field must be present in the schema property and must be bytes.")
  @Nullable
  private String keyField;

  @Description("Optional name of the field containing the kafka partition that was read from. " +
    "If this is not set, no partition field will be added to output records. " +
    "If set, this field must be present in the schema property and must be an integer.")
  @Nullable
  private String partitionField;

  @Description("Optional name of the field containing the kafka offset that the message was read from. " +
    "If this is not set, no offset field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private String offsetField;

  public KafkaBatchConfig() {
    super("");
  }

  public KafkaBatchConfig(String brokers, String partitions, String topic, String initialPartitionOffsets) {
    super(String.format("Kafka_%s", topic));
    this.kafkaBrokers = brokers;
    this.partitions = partitions;
    this.topic = topic;
    this.initialPartitionOffsets = initialPartitionOffsets;
  }

  // Accessors
  public String getTopic() {
    return topic;
  }

  public String getBrokers() {
    return kafkaBrokers;
  }

  @Nullable
  public String getOffsetDir() {
    return offsetDir;
  }

  public Set<Integer> getPartitions() {
    Set<Integer> partitionSet = new HashSet<>();
    if (partitions == null) {
      return partitionSet;
    }
    for (String partition : Splitter.on(',').trimResults().split(partitions)) {
      try {
        partitionSet.add(Integer.parseInt(partition));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          String.format("Invalid partition '%s'. Partitions must be integers.", partition));
      }
    }
    return partitionSet;
  }

  @Nullable
  public String getKeyField() {
    return Strings.isNullOrEmpty(keyField) ? null : keyField;
  }

  @Nullable
  public String getPartitionField() {
    return Strings.isNullOrEmpty(partitionField) ? null : partitionField;
  }

  @Nullable
  public String getOffsetField() {
    return Strings.isNullOrEmpty(offsetField) ? null : offsetField;
  }

  public long getMaxNumberRecords() {
    return maxNumberRecords == null ? -1 : maxNumberRecords;
  }

  @Nullable
  public String getFormat() {
    return Strings.isNullOrEmpty(format) ? null : format;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
    }
  }

  /**
   * Gets the message schema from the schema field.
   * If the time, key, partition, or offset fields are in the configured schema, they will be removed.
   */
  public Schema getMessageSchema() {
    Schema schema = getSchema();
    List<Schema.Field> messageFields = new ArrayList<>();
    boolean keyFieldExists = false;
    boolean partitionFieldExists = false;
    boolean offsetFieldExists = false;

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable()
        ? fieldSchema.getNonNullable().getType()
        : fieldSchema.getType();
      // if the field is not the time field and not the key field, it is a message field.
      if (fieldName.equals(keyField)) {
        if (fieldType != Schema.Type.BYTES) {
          throw new IllegalArgumentException("The key field must be of type bytes or nullable bytes.");
        }
        keyFieldExists = true;
      } else if (fieldName.equals(partitionField)) {
        if (fieldType != Schema.Type.INT) {
          throw new IllegalArgumentException("The partition field must be of type int.");
        }
        partitionFieldExists = true;
      } else if (fieldName.equals(offsetField)) {
        if (fieldType != Schema.Type.LONG) {
          throw new IllegalArgumentException("The offset field must be of type long.");
        }
        offsetFieldExists = true;
      } else {
        messageFields.add(field);
      }
    }
    if (messageFields.isEmpty()) {
      throw new IllegalArgumentException(
        "Schema must contain at least one other field besides the time and key fields.");
    }
    if (getKeyField() != null && !keyFieldExists) {
      throw new IllegalArgumentException(String.format(
        "keyField '%s' does not exist in the schema. Please add it to the schema.", keyField));
    }
    if (getPartitionField() != null && !partitionFieldExists) {
      throw new IllegalArgumentException(String.format(
        "partitionField '%s' does not exist in the schema. Please add it to the schema.", partitionField));
    }
    if (getOffsetField() != null && !offsetFieldExists) {
      throw new IllegalArgumentException(String.format(
        "offsetField '%s' does not exist in the schema. Please add it to the schema.", offsetField));
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  /**
   * @return broker host to broker port mapping.
   */
  public Map<String, Integer> getBrokerMap() {
    return parseBrokerMap(kafkaBrokers);
  }

  /**
   * Parses a given Kafka broker string, which is in comma separate host:port format, into a Map of host to port.
   */
  public static Map<String, Integer> parseBrokerMap(String kafkaBrokers) {
    Map<String, Integer> brokerMap = new HashMap<>();
    for (KeyValue<String, String> hostAndPort : KeyValueListParser.DEFAULT.parse(kafkaBrokers)) {
      String host = hostAndPort.getKey();
      String portStr = hostAndPort.getValue();
      try {
        brokerMap.put(host, Integer.parseInt(portStr));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid port '%s' for host '%s'.", portStr, host));
      }
    }
    if (brokerMap.isEmpty()) {
      throw new IllegalArgumentException("Must specify kafka brokers.");
    }
    return brokerMap;
  }

  /**
   * Gets the partition offsets as specified by the {@link #initialPartitionOffsets} field.
   */
  public KafkaPartitionOffsets getInitialPartitionOffsets() {
    KafkaPartitionOffsets partitionOffsets = new KafkaPartitionOffsets(Collections.emptyMap());

    if (initialPartitionOffsets == null) {
      return partitionOffsets;
    }

    for (KeyValue<String, String> partitionAndOffset : KeyValueListParser.DEFAULT.parse(initialPartitionOffsets)) {
      String partitionStr = partitionAndOffset.getKey();
      String offsetStr = partitionAndOffset.getValue();

      int partition;
      try {
        partition = Integer.parseInt(partitionStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid partition '%s' in initialPartitionOffsets.", partitionStr));
      }

      long offset;
      try {
        offset = Long.parseLong(offsetStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid offset '%s' in initialPartitionOffsets for partition %d.", partitionStr, partition));
      }

      partitionOffsets.setPartitionOffset(partition, offset);
    }

    return partitionOffsets;
  }

  public void validate() {
    // brokers can be null since it is macro enabled.
    if (kafkaBrokers != null) {
      getBrokerMap();
    }
    getPartitions();
    getInitialPartitionOffsets();

    Schema messageSchema = getMessageSchema();
    // if format is empty, there must be just a single message field of type bytes or nullable types.
    if (Strings.isNullOrEmpty(format)) {
      List<Schema.Field> messageFields = messageSchema.getFields();
      if (messageFields.size() > 1) {
        String fieldNames = messageFields.stream().map(Schema.Field::getName).collect(Collectors.joining(","));

        throw new IllegalArgumentException(String.format(
          "Without a format, the schema must contain just a single message field of type bytes or nullable bytes. " +
            "Found %s message fields (%s).", messageFields.size(), fieldNames));
      }

      Schema.Field messageField = messageFields.get(0);
      Schema messageFieldSchema = messageField.getSchema();
      Schema.Type messageFieldType = messageFieldSchema.isNullable() ?
        messageFieldSchema.getNonNullable().getType() : messageFieldSchema.getType();
      if (messageFieldType != Schema.Type.BYTES) {
        throw new IllegalArgumentException(String.format(
          "Without a format, the message field must be of type bytes or nullable bytes, but field %s is of type %s.",
          messageField.getName(), messageField.getSchema()));
      }
    } else {
      // otherwise, if there is a format, make sure we can instantiate it.
      FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<>());

      try {
        RecordFormats.createInitializedFormat(formatSpec);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format(
          "Unable to instantiate a message parser from format '%s' and message schema '%s': %s",
          format, messageSchema, e.getMessage()), e);
      }
    }
  }

  @VisibleForTesting
  public static Path getOffsetFilePath(Path dir, String namespace, String pipelineName) {
    return new Path(dir, String.format("%s.%s.offsets.json", namespace, pipelineName));
  }
}
