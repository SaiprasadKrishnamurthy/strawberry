package com.strawberry.engine.bolts;

import com.sai.strawberry.api.CacheContext;
import com.sai.strawberry.api.CacheHandler;
import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
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
                String currDoc = StrawberryConfigHolder.getJsonParser().writeValueAsString(doc);
                CacheHandler cacheHandler = cacheHandlerInstance(eventStreamConfig.getCacheHandlerClass());
                CacheContext ctx = () -> StrawberryConfigHolder.hazelcastInstance().getMap(eventStreamConfig.getConfigId());
                cacheHandler.handleCache(ctx, currDoc);
                outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            } else {
                outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            }
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    private CacheHandler cacheHandlerInstance(final String cacheHandlerClass) throws Exception {
        Class<CacheHandler> clazz = (Class<CacheHandler>) Class.forName(cacheHandlerClass);
        return clazz.newInstance();
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
