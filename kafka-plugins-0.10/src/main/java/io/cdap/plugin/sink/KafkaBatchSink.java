/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.KafkaHelpers;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.io.BytesWritable;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kafka sink to write to Kafka
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Kafka bytes")
@Description("KafkaSink to write bytes events to kafka")
public class KafkaBatchSink extends ReferenceBatchSink<StructuredRecord, BytesWritable, BytesWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBatchSink.class);

  // Configuration for the plugin.
  private final Config producerConfig;

  private final KafkaOutputFormatProvider kafkaOutputFormatProvider;

  // Static constants for configuring Kafka producer.
  private static final String ACKS_REQUIRED = "acks";

  public KafkaBatchSink(Config producerConfig) {
    super(producerConfig);
    this.producerConfig = producerConfig;
    this.kafkaOutputFormatProvider = new KafkaOutputFormatProvider(producerConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    producerConfig.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LineageRecorder lineageRecorder = new LineageRecorder(context, producerConfig.referenceName);
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null) {
      lineageRecorder.createExternalDataset(inputSchema);
      if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
        lineageRecorder.recordWrite("Write", "Wrote to Kafka topic.", inputSchema.getFields().stream().map
          (Schema.Field::getName).collect(Collectors.toList()));
      }
    }
    context.addOutput(Output.of(producerConfig.referenceName, kafkaOutputFormatProvider));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<BytesWritable, BytesWritable>> emitter) {
    List<Schema.Field> fields = input.getSchema().getFields();
    if (fields == null || fields.size() == 0) throw new IllegalArgumentException("Invalid input record," + input);
    byte[] body = input.get(fields.get(0).getName());
    if (body == null) throw new IllegalArgumentException("Body is null");
    if (Strings.isNullOrEmpty(producerConfig.key)) {
      emitter.emit(new KeyValue<>((BytesWritable) null, new BytesWritable(body)));
    } else {
      String key = input.get(producerConfig.key);
      emitter.emit(new KeyValue<>(new BytesWritable(key.getBytes()), new BytesWritable(body)));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends ReferencePluginConfig {

    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    @Macro
    private String brokers;

    @Name("async")
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    @Macro
    private String async;

    @Name("key")
    @Description("Specify the key field to be used in the message. Only String Partitioner is supported.")
    @Macro
    @Nullable
    private String key;

    @Name("topic")
    @Description("Topic to which message needs to be published")
    @Macro
    private String topic;

    @Name("kafkaProperties")
    @Description("Additional kafka producer properties to set")
    @Macro
    @Nullable
    private String kafkaProperties;

    @Description("The kerberos principal used for the source.")
    @Macro
    @Nullable
    private String principal;

    @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String keytabLocation;

    @Name("compressionType")
    @Description("Compression type to be applied on message")
    @Macro
    private String compressionType;

    public Config(String brokers, String async, String key, String topic, String kafkaProperties,
                  String compressionType) {
      super(String.format("Kafka_%s", topic));
      this.brokers = brokers;
      this.async = async;
      this.key = key;
      this.topic = topic;
      this.kafkaProperties = kafkaProperties;
      this.compressionType = compressionType;
    }

    private void validate() {
      if (!async.equalsIgnoreCase("true") && !async.equalsIgnoreCase("false")) {
        throw new IllegalArgumentException("Async flag has to be either TRUE or FALSE.");
      }

      KafkaHelpers.validateKerberosSetting(principal, keytabLocation);
    }
  }

  private static class KafkaOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    KafkaOutputFormatProvider(Config kafkaSinkConfig) {
      this.conf = new HashMap<>();
      conf.put("topic", kafkaSinkConfig.topic);

      conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.brokers);
      conf.put("compression.type", kafkaSinkConfig.compressionType);
      conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getCanonicalName());
      conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getCanonicalName());

      KafkaHelpers.setupKerberosLogin(conf, kafkaSinkConfig.principal, kafkaSinkConfig.keytabLocation);
      addKafkaProperties(kafkaSinkConfig.kafkaProperties);

      conf.put("async", kafkaSinkConfig.async);
      if (kafkaSinkConfig.async.equalsIgnoreCase("true")) {
        conf.put(ACKS_REQUIRED, "1");
      }

      if (!Strings.isNullOrEmpty(kafkaSinkConfig.key)) {
        conf.put("hasKey", kafkaSinkConfig.key);
      }
    }

    private void addKafkaProperties(String kafkaProperties) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      if (!Strings.isNullOrEmpty(kafkaProperties)) {
        for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
          // add prefix to each property
          String key = "additional." + keyVal.getKey();
          String val = keyVal.getValue();
          conf.put(key, val);
        }
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return KafkaOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
