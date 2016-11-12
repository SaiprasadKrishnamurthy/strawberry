package com.sai.strawberry.micro;

import akka.actor.ActorSystem;
import com.google.common.base.Predicates;
import com.mongodb.MongoClient;
import com.sai.strawberry.micro.config.ActorFactory;
import com.sai.strawberry.micro.config.AppProperties;
import com.sai.strawberry.micro.es.ESFacade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by saipkri on 07/09/16.
 */
@ComponentScan("com.sai.strawberry.micro")
@EnableAutoConfiguration
@EnableAsync
@PropertySource("classpath:application.properties")
@EnableSwagger2
@Configuration
public class Main {

    @Inject
    private AppProperties appProperties;

    private ActorSystem actorSystem() {
        return ActorSystem.create("RtsActorSystem");
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public ActorFactory actorFactory() throws Exception {
        return new ActorFactory(actorSystem(), appProperties, kafkaProducer(), esinit(), mongoTemplate());
    }

    @Bean
    public ESFacade esinit() throws Exception {
        return new ESFacade(appProperties);
    }

    @Bean
    public MongoTemplate mongoTemplate() {
        MongoClient mongoClient = new MongoClient(appProperties.getMongoHost(), appProperties.getMongoPort());
        MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient, appProperties.getMongoDb());
        return new MongoTemplate(mongoDbFactory);
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(senderProps());
    }

    private Map<String, Object> senderProps() {
        // OK to hard code for now. May be to move it to appProperties later.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getKafkaBrokersCsv());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    /**
     * Swagger 2 docket bean configuration.
     *
     * @return swagger 2 Docket.
     */
    @Bean
    public Docket configApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("config")
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(Predicates.not(PathSelectors.regex("/error"))) // Exclude Spring error controllers
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Strawberry REST API")
                .contact("sai@concordesearch.co.uk")
                .version("1.0")
                .build();
    }

    public static void main(String[] args) {
        SpringApplicationBuilder application = new SpringApplicationBuilder();
        application //
                .headless(true) //
                .addCommandLineProperties(true) //
                .sources(Main.class) //
                .main(Main.class) //
                .registerShutdownHook(true)
                .run(args);
    }


}
