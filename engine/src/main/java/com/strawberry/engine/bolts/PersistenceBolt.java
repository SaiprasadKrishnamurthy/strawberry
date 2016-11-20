package com.strawberry.engine.bolts;

import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class PersistenceBolt extends BaseRichBolt {
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
            Map doc = (Map) tuple.getValueByField("doc");
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");
            String topic = eventStreamConfig.getConfigId();
            String docAsString = StrawberryConfigHolder.getJsonParser().writeValueAsString(doc);

            if (eventStreamConfig.isPersistEvent()) {
                StrawberryConfigHolder.getMongoTemplate().save(doc, topic);
                // inject the natural id in the doc for indexing in ES.
                StrawberryConfigHolder.getKafkaProducer().send(new ProducerRecord<>(StrawberryConfigHolder.getEsInputTopicName(), docAsString));
            }
            outputCollector.emit(tuple, new Values(doc));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "matchedQueryNames", "eventStreamConfig"));
    }
}
