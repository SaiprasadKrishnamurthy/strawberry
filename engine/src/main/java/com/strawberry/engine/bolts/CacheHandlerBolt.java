package com.strawberry.engine.bolts;

import com.hazelcast.core.IMap;
import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
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

/**
 * Created by saipkri on 18/08/16.
 */
public class CacheHandlerBolt extends BaseRichBolt {
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
            Map doc = (Map) tuple.getValue(0);
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");

            if (eventStreamConfig.isCacheEnabled()) {
                IMap<String, List<Map>> cache = StrawberryConfigHolder.hazelcastInstance().getMap(eventStreamConfig.getConfigId());
                cache.compute(eventStreamConfig.getConfigId(), (k, v) -> {
                    if (v == null) {
                        return new ArrayList<>();
                    } else {
                        v.add(doc);
                        return v;
                    }
                });
                outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            } else {
                outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            }
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
