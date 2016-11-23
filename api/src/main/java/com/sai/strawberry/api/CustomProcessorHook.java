package com.sai.strawberry.api;

import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sai
 */
public interface CustomProcessorHook {
    Map process(EventStreamConfig config, Map jsonIn, MongoTemplate slowZoneMongoTemplate, MongoTemplate fastZoneMongoTemplate, Map<String, List<Map>> cache);

    default Map execute(final EventStreamConfig config, final Map jsonIn, final MongoTemplate slowZoneMongoTemplate, final MongoTemplate fastZoneMongoTemplate, final Map<String, List<Map>> streamCache) {
        Map custom = process(config, jsonIn, slowZoneMongoTemplate, fastZoneMongoTemplate, streamCache);
        Map jsonCopy = new LinkedHashMap<>(jsonIn);
        jsonCopy.put("custom__", custom);
        return jsonCopy;
    }
}
