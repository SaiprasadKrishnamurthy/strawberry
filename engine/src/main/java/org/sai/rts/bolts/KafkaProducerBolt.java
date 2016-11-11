package org.sai.rts.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.sai.rts.es.ConfigFactory;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class KafkaProducerBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;
    private KeyValueState<String, Long> kvState;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map results = (Map) tuple.getValueByField("results");
            String resultsTopic = tuple.getStringByField("resultsTopic");
            ConfigFactory.getKafkaProducer().send(new ProducerRecord<>(resultsTopic,
                    mapper.writeValueAsString(results)));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
