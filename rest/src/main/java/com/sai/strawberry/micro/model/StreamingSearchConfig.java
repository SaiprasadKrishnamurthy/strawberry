package com.sai.strawberry.micro.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 24/10/16.
 */
@Data
public class StreamingSearchConfig {
    private String dataCategoryName;
    private Map<String, Object> esIndexMappings;
    private List<ReactionRule> reactionRules;
}
