package com.strawberry.engine.bolts;

import com.sai.strawberry.api.EventStreamConfig;
import com.sai.strawberry.api.StatisticalOperationsHandler;
import com.sai.strawberry.api.StatisticalOperationsResut;
import com.sai.strawberry.api.Stats;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class StatisticalOperationsBolt extends BaseRichBolt {
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
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValue(1);
            System.out.println(boltId + " - Tuple Reached in the Stats Handler bolt: " + doc);
            List<String> configuredStatisticalOpsHandlers = eventStreamConfig.getStreamingStatisticalOperationHandlers();
            Map<String, List<StatisticalOperationsResut>> statResults = new HashMap<>();
            if (configuredStatisticalOpsHandlers != null && !configuredStatisticalOpsHandlers.isEmpty()) {
                for (String statsOperation : configuredStatisticalOpsHandlers) {
                    StatisticalOperationsHandler statisticalOperationsHandler = getStatisticalOperationsHandler(statsOperation);
                    List<StatisticalOperationsResut> jsonOut = invoke(statisticalOperationsHandler, doc, eventStreamConfig.getConfigId());
                    statResults.put(statisticalOperationsHandler.getName(), jsonOut);
                }
            }
            outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    private List<StatisticalOperationsResut> invoke(final StatisticalOperationsHandler transformer, final Map doc, String configId) throws Exception {
        Map<String, Object> statsCache = StrawberryConfigHolder.hazelcastInstance().getMap(configId);
        Stats stats = (Stats) statsCache.compute(transformer.getName(), (k, v) -> v == null ? new Stats(2) : v);
        stats.recordValue(transformer.getValueToRecord(doc).longValue());
        if (transformer.reset(stats)) {
            stats.reset();
            return Collections.emptyList();
        } else {
            return transformer.apply(stats);
        }
    }

    private StatisticalOperationsHandler getStatisticalOperationsHandler(final String statsOpHandler) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<StatisticalOperationsHandler> clazz = (Class<StatisticalOperationsHandler>) Class.forName(statsOpHandler);
        return clazz.newInstance();
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
