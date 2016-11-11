package org.sai.rts.es;

import io.searchbox.client.JestClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by saipkri on 27/10/16.
 */
public final class ConfigFactory {

    private static JestClient _jestClient;
    private static Properties _props = new Properties();
    private static KafkaProducer<String, String> kafkaProducer;


    public static void init(final String propertiesFileLocation) {
        try {
            _props.load(new FileInputStream(propertiesFileLocation));
            initKafkaProducer();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _props.get("rts.kafkaBrokersCsv"));
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
