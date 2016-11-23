package com.strawberry.engine.bolts;

import com.sai.strawberry.api.CustomProcessorHook;
import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class CustomProcessorHooksBolt extends BaseRichBolt {
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
            System.out.println(boltId + " - Tuple Reached in the CustomProcessorHooksBolt bolt: " + doc);
            String customProcessor = eventStreamConfig.getCustomProcessingHookClassName();
            if (customProcessor != null) {
                doc = invoke(eventStreamConfig, customProcessor, doc);
            }
            if (eventStreamConfig.getCustomProcessingHookScript() != null) {
                doc = invokeScript(eventStreamConfig, doc);
            }
            outputCollector.emit(tuple, new Values(doc, eventStreamConfig));
            outputCollector.ack(tuple);
        } catch (Exception ex) {
            outputCollector.reportError(ex);
        }
    }

    private Map invoke(final EventStreamConfig eventStreamConfig, final String processor, final Map jsonIn) throws Exception {
        Map<String, List<Map>> cache = StrawberryConfigHolder.hazelcastInstance().getMap(eventStreamConfig.getConfigId());
        Class<CustomProcessorHook> clazz = (Class<CustomProcessorHook>) Class.forName(processor);
        CustomProcessorHook processorHook = clazz.newInstance();
        return processorHook.execute(eventStreamConfig, jsonIn, StrawberryConfigHolder.getMongoTemplate(), StrawberryConfigHolder.getMongoTemplateForBatch(), cache);
    }

    private Map invokeScript(final EventStreamConfig eventStreamConfig, final Map jsonIn) throws Exception {
        Binding binding = new Binding();
        binding.setVariable("config", eventStreamConfig);
        binding.setVariable("jsonIn", jsonIn);
        binding.setVariable("slowZoneMongoTemplate", StrawberryConfigHolder.getMongoTemplate());
        binding.setVariable("fastZoneMongoTemplate", StrawberryConfigHolder.getMongoTemplateForBatch());
        binding.setVariable("cache", StrawberryConfigHolder.hazelcastInstance().getMap(eventStreamConfig.getConfigId()));
        GroovyShell shell = new GroovyShell(binding);
        Object returnDoc = shell.evaluate(eventStreamConfig.getCustomProcessingHookScript());
        Map jsonCopy = new LinkedHashMap<>(jsonIn);
        jsonCopy.put("custom__", returnDoc);
        return jsonCopy;
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("doc", "eventStreamConfig"));
    }
}
