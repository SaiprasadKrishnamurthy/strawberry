package com.sai.strawberry.api;

import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * Created by saipkri on 12/11/16.
 */
public interface BatchQueryProcessor {
    String query(MongoTemplate mongoTemplate);
}
