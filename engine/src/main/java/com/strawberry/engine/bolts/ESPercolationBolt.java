package com.strawberry.engine.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * Created by saipkri on 18/08/16.
 */
public class ESPercolationBolt extends BaseRichBolt {
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
            RestTemplate rt = new RestTemplate();
            ObjectMapper mapper = StrawberryConfigHolder.getJsonParser();
            Map doc = (Map) tuple.getValueByField("doc");
            EventStreamConfig eventStreamConfig = (EventStreamConfig) tuple.getValueByField("eventStreamConfig");
            String topic = eventStreamConfig.getConfigId();

            List<String> matchedQueryNames = new ArrayList<>();

            // Percolate the query.
            Map docTobePercolated = new HashMap<>();
            docTobePercolated.put("doc", doc);

            Map percolationResponse = rt.postForObject(StrawberryConfigHolder.getEsUrl() + "/" + topic + "/" + topic + "/_percolate", mapper.writeValueAsString(docTobePercolated), Map.class, Collections.emptyMap());
            List<Map> matches = (List<Map>) percolationResponse.get("matches");

            System.out.println("\t\t Percolation matches: "+matches);
            // Get the individual matched query
            for (Map matchedQuery : matches) {
                String queryId = matchedQuery.get("_id").toString();
                Map percolationQueryObject = rt.getForObject(StrawberryConfigHolder.getEsUrl() + "/" + topic + "/.percolator/" + queryId, Map.class);
                String queryName = ((Map) percolationQueryObject.get("_source")).get("queryName").toString();
                matchedQueryNames.add(queryName);
            }
            outputCollector.emit(tuple, new Values(doc, matchedQueryNames, eventStreamConfig));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "matchedQueryNames", "eventStreamConfig"));
    }
}
