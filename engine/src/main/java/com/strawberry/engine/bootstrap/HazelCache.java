package com.strawberry.engine.bootstrap;

import com.hazelcast.core.HazelcastInstance;
import com.strawberry.engine.config.StrawberryConfigHolder;

import java.util.Map;

public class HazelCache {
    public static void main(String[] args) throws Exception {
        StrawberryConfigHolder.init("/Users/saipkri/learning/strawberry/engine/src/main/resources/application.properties");
        HazelcastInstance instance = StrawberryConfigHolder.hazelcastInstance();
        Map<String, String> cache = instance.getMap("vehicle-stream");
        cache.put("a", "{a}");
        cache.put("b", "{b}");

        System.out.println("Cache here: "+cache);

        cache.forEach((k,v) -> System.out.println("Key: "+k+", Value: "+v));

    }
}

