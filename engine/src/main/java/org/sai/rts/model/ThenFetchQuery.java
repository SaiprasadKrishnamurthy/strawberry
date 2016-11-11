package org.sai.rts.model;

import lombok.Data;

import java.util.Map;

/**
 * Created by saipkri on 24/10/16.
 */
@Data
public class ThenFetchQuery {
    private String index;
    private Map<String, Object> query;
}
