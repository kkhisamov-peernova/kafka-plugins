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

package io.cdap.plugin;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.sink.KafkaBatchSink;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Sink and Alerts Publisher test
 */
public class KafkaSinkAndAlertsPublisherTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      KafkaBatchSink.class,
                      RangeAssignor.class,
                      ByteArraySerializer.class);

    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(),
                                                              kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void cleanup() {
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testKafkaSink() throws Exception {
    Schema schema = Schema.recordOf("test", Schema.Field.of("data", Schema.of(Schema.Type.BYTES)));
    // create the pipeline config
    String inputName = "sinkTestInput";

    String dataTopic = "records";
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("brokers", "localhost:" + kafkaPort);
    sinkProperties.put("referenceName", "kafkaTest");
    sinkProperties.put("topic", dataTopic);
    sinkProperties.put("schema", schema.toString());
    sinkProperties.put("async", "FALSE");
    sinkProperties.put("compressionType", "none");

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    ETLStage sink =
      new ETLStage("sink", new ETLPlugin("Kafka", KafkaBatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testKafkaSink");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));


    Set<ByteBuffer> expected = ImmutableSet.of(
            ByteBuffer.wrap(new byte[]{0, 1, 2}),
            ByteBuffer.wrap(new byte[]{3, 4, 5}),
            ByteBuffer.wrap(new byte[]{6, 7, 8})
    );

    List<StructuredRecord> records = new ArrayList<>();
    for (ByteBuffer e : expected) {
      StructuredRecord record =
        StructuredRecord.builder(schema)
          .set("data", e.array())
          .build();
      records.add(record);
    }

    DataSetManager<Table> sourceTable = getDataset(inputName);
    MockSource.writeInput(sourceTable, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // Assert users
    Set<ByteBuffer> actual = readKafkaRecords(dataTopic, expected.size());
    Assert.assertEquals(expected, actual);
  }

  private Set<ByteBuffer> readKafkaRecords(String topic, final int maxMessages) throws InterruptedException {
    KafkaConsumer kafkaConsumer = kafkaClient.getConsumer();

    final Set<ByteBuffer> kafkaMessages = new HashSet<>();
    KafkaConsumer.Preparer preparer = kafkaConsumer.prepare();
    preparer.addFromBeginning(topic, 0);

    final CountDownLatch stopLatch = new CountDownLatch(1);
    Cancellable cancellable = preparer.consume(new KafkaConsumer.MessageCallback() {
      @Override
      public long onReceived(Iterator<FetchedMessage> messages) {
        long nextOffset = 0;
        while (messages.hasNext()) {
          FetchedMessage message = messages.next();
          nextOffset = message.getNextOffset();
          kafkaMessages.add(message.getPayload());
        }
        // We are done when maxMessages are received
        if (kafkaMessages.size() >= maxMessages) {
          stopLatch.countDown();
        }
        return nextOffset;
      }

      @Override
      public void finished() {
        // nothing to do
      }
    });

    stopLatch.await(30, TimeUnit.SECONDS);
    cancellable.cancel();
    return kafkaMessages;
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }

}
