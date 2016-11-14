package com.strawberry.engine.bootstrap;

import com.strawberry.engine.bolts.*;
import com.strawberry.engine.config.StrawberryConfigHolder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.UUID;

/**
 * Created by saipkri on 11/11/16.
 */
public class StreamingEventProcessor {

    @Option(name = "-h", aliases = "-help", usage = "print this message")
    private boolean help = false;

    @Option(name = "-c", aliases = {"-configid"}, usage = "event stream config id", required = true)
    private String configid;

    @Option(name = "-f", aliases = {"-file"}, usage = "Properties file location containing the various application properties (eg: mongo url, zookeeper url etc)", required = true)
    private String propsFileLocation;

    @Option(name = "-m", aliases = {"-mode"}, usage = "test mode or real mode (defaulted to test mode). Possible values are: test|real ", required = false)
    private String mode = "test";

    @Option(name = "-p", aliases = {"-p"}, usage = "Parallelism (defaults to the number of processors available on the system)", required = false)
    private int parallelism = Runtime.getRuntime().availableProcessors();

    private StormTopology buildTopology(final ZkHosts kafkaZookeeperHosts) {
        SpoutConfig kafkaConfig = new SpoutConfig(kafkaZookeeperHosts, configid, "", UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), parallelism);
        builder.setBolt("FieldsExtractorBolt", new FieldsExtractorBolt(), parallelism).shuffleGrouping("KafkaSpout");
        builder.setBolt("TransformationBolt", new TransformationBolt(), parallelism).shuffleGrouping("FieldsExtractorBolt");
        builder.setBolt("CacheHandlerBolt", new CacheHandlerBolt(), parallelism).shuffleGrouping("TransformationBolt");
        builder.setBolt("ESPercolationBolt", new ESPercolationBolt(), parallelism).shuffleGrouping("TransformationBolt");
        builder.setBolt("PersistenceBolt", new PersistenceBolt(), parallelism).shuffleGrouping("TransformationBolt");
        builder.setBolt("BatchSetupBolt", new BatchSetupBolt(), parallelism).shuffleGrouping("TransformationBolt");
        builder.setBolt("BatchQueryBolt", new BatchQueryBolt(), parallelism).shuffleGrouping("BatchSetupBolt");
        builder.setBolt("NotificationBolt", new NotificationBolt(), parallelism).shuffleGrouping("ESPercolationBolt");

        // Spout that listens from ES input and indexes the data to ES.
        SpoutConfig kafkaConfig1 = new SpoutConfig(kafkaZookeeperHosts, StrawberryConfigHolder.getEsInputTopicName(), "", UUID.randomUUID().toString());
        kafkaConfig1.scheme = new SchemeAsMultiScheme(new StringScheme());
        builder.setSpout("ESInputKafkaSpout", new KafkaSpout(kafkaConfig1), parallelism);
        builder.setBolt("ESIndexBolt", new ESIndexBolt(), parallelism).shuffleGrouping("ESInputKafkaSpout");

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        StreamingEventProcessor instance = new StreamingEventProcessor();
        validateCommandlineArgs(args, instance);
        StrawberryConfigHolder.init(instance.propsFileLocation);

        // ES related configs.
        String esUrl = StrawberryConfigHolder.getEsUrl();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(esUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
        StrawberryConfigHolder.setJestClient(client);

        // Zookeeper.
        ZkHosts zookeeperHosts = new ZkHosts(StrawberryConfigHolder.getZookeeperUrls());

        // All configs set up done.

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);

        StormTopology stormTopology = instance.buildTopology(zookeeperHosts);

        if (instance.mode.trim().equals("real")) {
            StormSubmitter.submitTopology(instance.getClass().getSimpleName(), config, stormTopology);
        } else {
            System.out.println("Local cluster: ");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(instance.getClass().getSimpleName(), config, stormTopology);
            System.out.println("Submitted to Local cluster");

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    cluster.shutdown();
                    System.out.println("Storm Cluster shut down.");
                }
            });
        }
    }

    private static void validateCommandlineArgs(String[] args, StreamingEventProcessor instance) {
        final CmdLineParser parser = new CmdLineParser(instance);
        try {
            parser.parseArgument(args);
        } catch (Exception ex) {
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

}
