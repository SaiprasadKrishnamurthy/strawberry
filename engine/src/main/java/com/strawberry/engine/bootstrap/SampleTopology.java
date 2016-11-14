package com.strawberry.engine.bootstrap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by saipkri on 11/11/16.
 */
public class SampleTopology {


    public static void main(String[] args) throws Exception {

        SampleTopology instance = new SampleTopology();

        BaseRichSpout spout = new BaseRichSpout() {
            private SpoutOutputCollector collector;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("city"));
            }

            @Override
            public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
                this.collector = spoutOutputCollector;
            }

            @Override
            public void nextTuple() {
                String[] cities = {"chennai", "bangalore", "mumbai", "delhi"};
                Random r = new Random();
                for (int i = 0; i < 10000; i++) {
                    collector.emit(new Values(cities[r.nextInt(4)]));
                }
            }
        };

        BaseRichBolt loggingBolt = new BaseRichBolt() {
            private int boltId;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

            }

            @Override
            public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                boltId = topologyContext.getThisTaskId();
            }

            @Override
            public void execute(Tuple tuple) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("\t\t [" + boltId + "] -- " + tuple.getValue(0));
            }
        };

        // All configs set up done.

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);

        System.out.println("Local cluster: ");
        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CitiesSpout", spout);
        builder.setBolt("Logger", loggingBolt, 2).fieldsGrouping("CitiesSpout", new Fields("city"));//.setNumTasks(8);
//        builder.setBolt("Logger", loggingBolt).fieldsGrouping("CitiesSpout", new Fields("city"));

        cluster.submitTopology(instance.getClass().getSimpleName(), config, builder.createTopology());
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
