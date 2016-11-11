package org.sai.rts.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sai.rts.es.ConfigFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by saipkri on 18/08/16.
 */
public class ESPercolatedSearchBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
    private OutputCollector outputCollector;
    private int boltId;
    private KeyValueState<String, Long> kvState;


    @Override
    public void initState(final KeyValueState<String, Long> kvState) {
        this.kvState = kvState;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            RestTemplate rt = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();
            String topic = tuple.getStringByField("topic");
            Map payload = (Map) tuple.getValueByField("payload");
            // Percolate the query.
            Map docTobePercolated = new HashMap<>();
            docTobePercolated.put("doc", payload);
            Map percolationResponse = rt.postForObject(ConfigFactory.getEsUrl() + "/" + topic + "/" + topic + "/_percolate", mapper.writeValueAsString(docTobePercolated), Map.class, Collections.emptyMap());
            List<Map> matches = (List<Map>) percolationResponse.get("matches");

            // For every map, react to the query now.
            List<Map> resultsReacted = matches.parallelStream().flatMap(match -> {
                try {
                    String objectId = match.get("_id").toString();
                    Map percolationQueryObject = rt.getForObject(ConfigFactory.getEsUrl() + "/" + topic + "/.percolator/" + objectId, Map.class);
                    List<Map> metas = (List<Map>) ((Map) percolationQueryObject.get("_source")).get("meta");

                    return metas.parallelStream().flatMap(meta -> {
                        try {
                            String indexName = meta.get("index").toString();
                            Map query = (Map) meta.get("query");
                            Map results = rt.postForObject(ConfigFactory.getEsUrl() + "/" + indexName + "/" + indexName + "/_search", mapper.writeValueAsString(query), Map.class);
                            List<Map> hits = (List<Map>) ((Map) results.get("hits")).get("hits");
                            return hits.stream();
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                            return Stream.empty();
                        }
                    }).collect(toList()).stream();
                } catch (Exception ex) {
                    return Stream.empty();
                }
            }).map(matched -> (Map) matched.get("_source"))
                    .collect(toList());

            Map<String, Object> finalDoc = new HashMap<>();
            finalDoc.put("results", resultsReacted);
            finalDoc.put("inputData", payload);
            outputCollector.emit(tuple, new Values("results-" + topic, finalDoc));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            outputCollector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("resultsTopic", "results"));
    }
}
