package org.sai.rts.topologies;

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
import org.apache.storm.tuple.Fields;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.sai.rts.bolts.ESPercolatedSearchBolt;
import org.sai.rts.bolts.FieldsExtractorBolt;
import org.sai.rts.bolts.KafkaProducerBolt;
import org.sai.rts.bolts.TopicSpecificBolt;
import org.sai.rts.es.ConfigFactory;

import java.util.UUID;

/**
 * Created by saipkri on 26/10/16.
 */
public class StreamingSearchTopology {

    @Option(name = "-h", aliases = "-help", usage = "print this message")
    private boolean help = false;

    @Option(name = "-z", aliases = {"-zookeeper"}, usage = "zookeeper url that Kafka uses eg: localhost:2181", required = true)
    private String kafkaZookeeperUrl;

    @Option(name = "-c", aliases = {"-configid"}, usage = "event stream config id", required = true)
    private String kafkaTopicName;

    @Option(name = "-m", aliases = {"-mode"}, usage = "test mode or real mode (defaulted to test mode). Possible values are: test|real ", required = false)
    private String mode = "test";

    @Option(name = "-p", aliases = {"-p"}, usage = "Parallelism (defaults to the number of processors available on the system)", required = false)
    private int parallelism = Runtime.getRuntime().availableProcessors();

    private StormTopology buildTopology(final ZkHosts kafkaZookeeperHosts) {
        SpoutConfig kafkaConfig = new SpoutConfig(kafkaZookeeperHosts, kafkaTopicName, "", UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), parallelism);
        builder.setBolt("FieldsExtractorBolt", new FieldsExtractorBolt(), parallelism).shuffleGrouping("KafkaSpout");
        builder.setBolt("TopicSpecificBolt", new TopicSpecificBolt(), parallelism).fieldsGrouping("FieldsExtractorBolt", new Fields("topic"));
        builder.setBolt("ESPercolationBolt", new ESPercolatedSearchBolt(), parallelism).shuffleGrouping("TopicSpecificBolt");
        builder.setBolt("KafkaProducerBolt", new KafkaProducerBolt(), parallelism).shuffleGrouping("ESPercolationBolt");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        String esUrl = ConfigFactory.getEsUrl();

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(esUrl)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
        ConfigFactory.setJestClient(client);

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);
        StreamingSearchTopology instance = new StreamingSearchTopology();

        final CmdLineParser parser = new CmdLineParser(instance);
        try {
            parser.parseArgument(args);
        } catch (Exception ex) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        StormTopology stormTopology = instance.buildTopology(new ZkHosts(instance.kafkaZookeeperUrl));


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
}
