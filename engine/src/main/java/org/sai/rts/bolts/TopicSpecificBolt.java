package org.sai.rts.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sai.rts.es.ConfigFactory;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by saipkri on 18/08/16.
 */
public class TopicSpecificBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;
    private CopyOnWriteArraySet<Map> payloadsTobeIndexedToEs = new CopyOnWriteArraySet<>();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map doc = objectMapper.readValue(tuple.getStringByField("doc"), Map.class);
            Map outerPayload = new ObjectMapper().readValue(doc.get("payload").toString(), Map.class);

            Map payload = (Map) outerPayload.get("payload");
            System.out.println("Payload: " + payload);
            String documentPartitionKey = doc.get("partitionKey").toString();
            String dataIdentifier = doc.get("dataIdentifier").toString();
            String topic = doc.get("topicName").toString();
            System.out.println(boltId + " - Tuple Reached in the Topic Specific bolt: " + doc + " --> " + documentPartitionKey + " ---> Payload " + payload);
            payloadsTobeIndexedToEs.add(payload);

            // Hard coded batch size. Send it off to ES in the size of 100.
            if (payloadsTobeIndexedToEs.size() > 0) {
                Bulk.Builder bulkBuilder = new Bulk.Builder();
                for (Map payloadDoc : payloadsTobeIndexedToEs) {
                    Index index = new Index.Builder(payloadDoc).index(topic).type(topic).id(dataIdentifier).build();
                    bulkBuilder.addAction(index);
                }
                ConfigFactory.getJestClient().execute(bulkBuilder.build());
                payloadsTobeIndexedToEs.clear();
            }
            outputCollector.emit(tuple, new Values(documentPartitionKey, topic, doc, outerPayload, payload));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("partitionKey", "topic", "doc", "outerPayload", "payload"));
    }
}
