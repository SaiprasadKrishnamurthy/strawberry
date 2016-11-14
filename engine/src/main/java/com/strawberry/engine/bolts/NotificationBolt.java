package com.strawberry.engine.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.ITopic;
import com.sai.strawberry.api.EventStreamConfig;
import com.sai.strawberry.api.PostNotificationCallback;
import com.sai.strawberry.api.PreNotificationCallback;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.commons.lang3.StringUtils;
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
            String docAsString = mapper.writeValueAsString(doc);
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");

            List<String> matchedQueryNames = (List<String>) tuple.getValueByField("matchedQueryNames");
            for (String matchedQueryName : matchedQueryNames) {
                if (StringUtils.isNotBlank(eventStreamConfig.getPreNotificationCallback())) {
                    docAsString = invokePreNotificationHooks(eventStreamConfig.getPreNotificationCallback(), docAsString);
                    doc = StrawberryConfigHolder.getJsonParser().readValue(docAsString, Map.class);
                }
                if (eventStreamConfig.isDurableNotification()) {
                    // Goes to a Kafka Topic.
                    StrawberryConfigHolder.getKafkaProducer().send(new ProducerRecord<>(matchedQueryName, docAsString));
                } else {
                    // Goes to Hazelcast Memory-Grid Topic.
                    ITopic<String> topic = StrawberryConfigHolder.hazelcastInstance().getTopic(matchedQueryName);
                    topic.publish(docAsString);
                }
                if (StringUtils.isNotBlank(eventStreamConfig.getPostNotificationCallback())) {
                    invokePostNotificationHooks(eventStreamConfig.getPostNotificationCallback(), docAsString);
                    doc = StrawberryConfigHolder.getJsonParser().readValue(docAsString, Map.class);
                }
            }
            outputCollector.emit(tuple, new Values(doc, matchedQueryNames, eventStreamConfig));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    private String invokePreNotificationHooks(final String clazzName, final String jsonIn) throws Exception {
        Class<PreNotificationCallback> clazz = (Class<PreNotificationCallback>) Class.forName(clazzName);
        PreNotificationCallback transformer = clazz.newInstance();
        return transformer.call(jsonIn);
    }

    private String invokePostNotificationHooks(final String clazzName, final String jsonIn) throws Exception {
        Class<PostNotificationCallback> clazz = (Class<PostNotificationCallback>) Class.forName(clazzName);
        PostNotificationCallback transformer = clazz.newInstance();
        return transformer.call(jsonIn);
    }


    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "matchedQueryNames", "eventStreamConfig"));
    }
}
