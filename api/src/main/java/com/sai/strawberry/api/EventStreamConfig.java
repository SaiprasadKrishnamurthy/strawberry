package com.sai.strawberry.api;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 11/11/16.
 */
@Data
public class EventStreamConfig {
    private String configId;
    private List<String> dataTransformers;
    private List<String> prePersistenceCallback;
    private List<String> postPersistenceCallback;
    private List<String> preNotificationCallback;
    private List<String> postNotificationCallback;
    private boolean durableNotification;
    private Map<String, Object> indexDefinition;
    private Map<String, Map<String, Object>> watchQueries;
    private boolean persistEvent;
    private boolean enableVisualization;
    private BatchQueryConfig batchQueryConfig;
}
