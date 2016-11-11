package com.strawberry.engine.bolts;

import com.sai.strawberry.api.DataTransformer;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class TransformationBolt extends BaseRichBolt {
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
            System.out.println(boltId + " - Tuple Reached in the transformer bolt: " + doc);
            List<String> configuredTransformers = StrawberryConfigHolder.getEventStreamConfig().getDataTransformers();
            if (configuredTransformers != null) {
                String currDoc = StrawberryConfigHolder.getJsonParser().writeValueAsString(doc);
                for (String dataTransformer : configuredTransformers) {
                    String jsonOut = invoke(dataTransformer, currDoc);
                    currDoc = jsonOut;
                }
            }
            outputCollector.emit(tuple, new Values(doc));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    private String invoke(final String dataTransformer, final String jsonIn) throws Exception {
        Class<DataTransformer> clazz = (Class<DataTransformer>) Class.forName(dataTransformer);
        DataTransformer transformer = clazz.newInstance();
        return transformer.transform(jsonIn);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc"));
    }
}
