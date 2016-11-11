package com.strawberry.engine.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.strawberry.engine.config.StrawberryConfigHolder;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Created by saipkri on 18/08/16.
 */
public class ESIndexBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;
    private List<Map> payloadsTobeIndexedToEs = new ArrayList<>();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            Map doc = new ObjectMapper().readValue(tuple.getValue(0).toString(), Map.class);
            payloadsTobeIndexedToEs.add(doc);
            if (payloadsTobeIndexedToEs.size() > StrawberryConfigHolder.getEsIndexBatchSize()) {
                Lock lock = StrawberryConfigHolder.lockForEsIndexing();
                try {
                    lock.lock();
                    Bulk.Builder bulkBuilder = new Bulk.Builder();
                    for (Map payloadDoc : payloadsTobeIndexedToEs) {
                        Index index = new Index.Builder(payloadDoc).index(payloadDoc.get("__configId__").toString()).type(payloadDoc.get("__configId__").toString()).id(payloadDoc.get("dataId").toString()).build();
                        bulkBuilder.addAction(index);
                    }
                    StrawberryConfigHolder.getJestClient().execute(bulkBuilder.build());
                    payloadsTobeIndexedToEs.clear();
                } finally {
                    lock.unlock();
                }
            }
            outputCollector.emit(tuple, new Values(doc));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc"));
    }
}
