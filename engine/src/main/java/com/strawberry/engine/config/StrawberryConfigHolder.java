package com.strawberry.engine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.MongoClient;
import com.sai.strawberry.api.EventStreamConfig;
import io.searchbox.client.JestClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Created by saipkri on 27/10/16.
 */
public final class StrawberryConfigHolder {

    private static JestClient _jestClient;
    private static Properties _props = new Properties();
    private static KafkaProducer<String, String> kafkaProducer;
    private static MongoTemplate mongoTemplate;
    private static MongoTemplate mongoTemplateForBatch;
    private static EventStreamConfig eventStreamConfig;
    private static ObjectMapper JSONSERIALIZER = new ObjectMapper();
    private static HazelcastInstance instance;

    public static void init(final String propertiesFileLocation) {
        try {
            _props.load(new FileInputStream(propertiesFileLocation));
            initKafkaProducer();
            initMongo();
            initMongoForBatch();
            initHazelcast();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static void initHazelcast() {
        String[] memoryGridHosts = _props.getProperty("memoryGridHostsCsv").split(",");
        Config config = new Config();
        config.getNetworkConfig().setPort(Integer.parseInt(_props.getProperty("memoryGridPort")));
        config.getNetworkConfig().setPortAutoIncrement(true);
        NetworkConfig network = config.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        Stream.of(memoryGridHosts).forEach(host -> join.getTcpIpConfig().addMember(host).setEnabled(true));
        instance = Hazelcast.newHazelcastInstance(config);
    }

    public static void setJestClient(final JestClient jestClient) {
        _jestClient = jestClient;
    }

    public static JestClient getJestClient() {
        return _jestClient;
    }

    public static String getEsUrl() {
        return _props.getProperty("esUrl");
    }

    public static KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    private static void initKafkaProducer() {
        kafkaProducer = new KafkaProducer<>(senderProps());
    }

    private static Map<String, Object> senderProps() {
        // OK to hard code for now. May be to move it to appProperties later.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _props.get("kafkaBrokersCsv"));
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static ObjectMapper getJsonParser() {
        return JSONSERIALIZER;
    }

    public static MongoTemplate getMongoTemplate() {
        return mongoTemplate;
    }

    public static MongoTemplate getMongoTemplateForBatch() {
        return mongoTemplateForBatch;
    }

    public static EventStreamConfig getEventStreamConfig() {
        return eventStreamConfig;
    }

    public static String getZookeeperUrls() {
        return _props.get("zookeeperUrls").toString();
    }

    public static void initMongo() throws Exception {
        String mongoHost = _props.get("mongoHost").toString();
        String mongoDbPort = _props.get("mongoPort").toString();
        String mongoDbName = _props.get("mongoDb").toString();
        MongoClient mongoClient = new MongoClient(mongoHost, Integer.parseInt(mongoDbPort.trim()));
        MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient, mongoDbName);
        mongoTemplate = new MongoTemplate(mongoDbFactory);
    }

    public static void initMongoForBatch() throws Exception {
        String mongoHost = _props.get("mongoHost").toString();
        String mongoDbPort = _props.get("mongoPort").toString();
        String mongoDbName = _props.get("mongoDbForBatch").toString();
        MongoClient mongoClient = new MongoClient(mongoHost, Integer.parseInt(mongoDbPort.trim()));
        MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient, mongoDbName);
        mongoTemplateForBatch = new MongoTemplate(mongoDbFactory);
    }

    public static void initStreamConfig(final String id) {
        Query query = new Query();
        query.addCriteria(Criteria.where("configId").is(id));
        eventStreamConfig = mongoTemplate.find(query, EventStreamConfig.class).get(0);
    }

    public static String getEsInputTopicName() {
        return _props.get("esInputTopic").toString();
    }

    public static int getEsIndexBatchSize() {
        return Integer.parseInt(_props.get("esIndexBatchSize").toString());
    }

    public static HazelcastInstance hazelcastInstance() {
        return instance;
    }

    public static Lock lockForEsIndexing() {
        return new ReentrantLock(true);
    }
}
