package com.sai.strawberry.micro.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by saipkri on 07/09/16.
 */
@Component
@ConfigurationProperties("rts")
@Data
public class AppProperties {
    private String esUrl;
    private int concurrencyFactor;
    private String kafkaBrokersCsv;
}
