package com.strawberry.engine.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.ITopic;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class NotificationBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            ObjectMapper mapper = StrawberryConfigHolder.getJsonParser();
            Map doc = (Map) tuple.getValueByField("doc");
            List<String> matchedQueryNames = (List<String>) tuple.getValueByField("matchedQueryNames");
            for (String matchedQueryName : matchedQueryNames) {
                if (StrawberryConfigHolder.getEventStreamConfig().isDurableNotification()) {
                    // Goes to a Kafka Topic.
                    StrawberryConfigHolder.getKafkaProducer().send(new ProducerRecord<>(matchedQueryName, mapper.writeValueAsString(doc)));
                } else {
                    // Goes to Hazelcast Memory-Grid Topic.
                    ITopic<String> topic = StrawberryConfigHolder.hazelcastInstance().getTopic(StrawberryConfigHolder.getEventStreamConfig().getConfigId());
                    topic.publish(mapper.writeValueAsString(doc));
                }
            }
            outputCollector.emit(tuple, new Values(doc, matchedQueryNames));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "matchedQueryNames"));
    }
}
