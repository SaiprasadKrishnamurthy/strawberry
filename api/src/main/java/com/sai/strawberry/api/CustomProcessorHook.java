package com.sai.strawberry.api;

import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Sai
 */
public interface CustomProcessorHook {
    Map process(EventStreamConfig config, Map jsonIn, MongoTemplate slowZoneMongoTemplate, MongoTemplate fastZoneMongoTemplate);

    default Map execute(final EventStreamConfig config, final Map jsonIn, final MongoTemplate slowZoneMongoTemplate, final MongoTemplate fastZoneMongoTemplate) {
        Map custom = process(config, jsonIn, slowZoneMongoTemplate, fastZoneMongoTemplate);
        Map jsonCopy = new LinkedHashMap<>(jsonIn);
        jsonCopy.put("_custom", custom);
        return jsonCopy;
    }
}
