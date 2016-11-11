package com.strawberry.engine.bolts;

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
            String topic = StrawberryConfigHolder.getEventStreamConfig().getConfigId();
            Map doc = (Map) tuple.getValueByField("doc");
            if (StrawberryConfigHolder.getEventStreamConfig().isPersistEvent()) {
                StrawberryConfigHolder.getMongoTemplate().save(doc, topic);
                StrawberryConfigHolder.getKafkaProducer().send(new ProducerRecord<>(StrawberryConfigHolder.getEsInputTopicName(), StrawberryConfigHolder.getJsonParser().writeValueAsString(doc)));
            } else {
                outputCollector.emit(tuple, new Values(doc));
            }
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "matchedQueryNames"));
    }
}
