package org.sai.rts.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 24/10/16.
 */
@Data
public class StreamingSearchConfig {
    private String dataCategoryName;
    private String dataName;
    private Map<String, Object> esIndexMappings;
    private List<ReactionRule> reactionRules;
}
