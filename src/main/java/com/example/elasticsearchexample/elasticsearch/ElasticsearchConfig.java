package com.example.elasticsearchexample.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port}")
    private int port;
    @Bean
    public RestClient restClient() {
        return RestClient.builder(new HttpHost(host, port)).setRequestConfigCallback(builder -> builder
                .setConnectTimeout(1000)
                .setSocketTimeout(5000)).build();
    }



}