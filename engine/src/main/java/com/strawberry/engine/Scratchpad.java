package com.strawberry.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.api.BatchQueryConfig;
import com.sai.strawberry.api.EventStreamConfig;
import com.strawberry.engine.config.StrawberryConfigHolder;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by saipkri on 11/11/16.
 */
public class Scratchpad {

    public static void main(String[] args) throws Exception {
        StrawberryConfigHolder.init("/Users/saipkri/learning/strawberry/engine/src/main/resources/application.properties");
        ObjectMapper o = new ObjectMapper();

       /* EventStreamConfig config = new EventStreamConfig();
        config.setConfigId("vehicle-camera-sensor-events");
        config.setDurableNotification(true);
        config.setPersistEvent(true);
        config.setEnableVisualization(true);

        BatchQueryConfig batch = new BatchQueryConfig();
        batch.setMaxNumberOfDocs(49);
        batch.setMaxBatchSizeInBytes(Integer.MAX_VALUE);

        config.setBatchQueryConfig(batch);
        Map<String, Object> esQuery = o.readValue(new File("/Users/saipkri/learning/strawberry/engine/src/main/resources/esquery.json"), Map.class);
        Map<String, Map<String, Object>> registeredQueries = new HashMap<>();
        registeredQueries.put("nh45-clockwise", esQuery);
        config.setWatchQueries(registeredQueries);
        StrawberryConfigHolder.getMongoTemplate().save(config);
        StrawberryConfigHolder.initStreamConfig("vehicle-camera-sensor-events");
        System.out.println(StrawberryConfigHolder.getEventStreamConfig());*/

        for(int i=0; i<100; i++) {
            StrawberryConfigHolder.getKafkaProducer().send(new ProducerRecord<>("vehicle-camera-sensor-events", IOUtils.toString(new FileInputStream("/Users/saipkri/learning/strawberry/engine/src/main/resources/vehicle-camera-feed.json"))));
        }
    }
}
