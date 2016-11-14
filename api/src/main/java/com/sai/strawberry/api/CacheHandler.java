package com.sai.strawberry.api;

/**
 * Created by saipkri on 14/11/16.
 */
public interface CacheHandler {
    void handleCache(CacheContext cacheContext, String currJson);
}
