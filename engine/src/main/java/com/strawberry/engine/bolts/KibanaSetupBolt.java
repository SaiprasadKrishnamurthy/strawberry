package com.strawberry.engine.bolts;

import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * Created by saipkri on 18/08/16.
 */
public class KibanaSetupBolt extends BaseRichBolt {
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
            Map doc = (Map) tuple.getValueByField("doc");


            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");

            if (eventStreamConfig.isEnableVisualization()) {
                // Find if the index pattern is already defined in Kibana.
                RestTemplate rt = new RestTemplate();
                String indexCheckUrl = StrawberryConfigHolder.getEsUrl() + "/.kibana/index-pattern/" + eventStreamConfig.getConfigId();
                Map response = rt.getForObject(indexCheckUrl, Map.class, Collections.emptyMap());
                if (!(Boolean) response.get("found")) {
                    String endpoint = ".kibana/index-pattern/" + eventStreamConfig.getConfigId();
                    Map<String, String> in = new LinkedHashMap<>();
                    in.put("title", eventStreamConfig.getConfigId());
                    Map<String, Map> props = (Map<String, Map>) eventStreamConfig.getIndexDefinition().get("properties");
                    Optional<Map.Entry<String, Map>> timestampField = props.entrySet().stream().filter(entry -> entry.getValue().containsValue("date")).findFirst();
                    if (timestampField.isPresent()) {
                        in.put("timeFieldName", timestampField.get().getKey().trim());
                    }
                    rt.exchange(StrawberryConfigHolder.getEsUrl() + "/" + endpoint, HttpMethod.PUT, new HttpEntity<Object>(StrawberryConfigHolder.getJsonParser().writeValueAsString(in)), Map.class, Collections.emptyMap());
                    endpoint = ".kibana/config/4.6.1";
                    in.clear();
                    in.put("defaultIndex", eventStreamConfig.getConfigId());
                    rt.exchange(StrawberryConfigHolder.getEsUrl() + "/" + endpoint, HttpMethod.PUT, new HttpEntity<Object>(StrawberryConfigHolder.getJsonParser().writeValueAsString(in)), Map.class, Collections.emptyMap());
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
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
