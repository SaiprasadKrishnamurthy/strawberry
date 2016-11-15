package com.sai.strawberry.api;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 15/11/16.
 */
public interface StatisticalOperationsHandler {
    String getName();
    Number getValueToRecord(Map<String, Object> payload);
    List<StatisticalOperationsResut> apply(Stats statsWrapper);
    boolean reset(Stats statsWrapper);
}
