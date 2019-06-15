package io.ppatierno.kafka;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);
    
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions envStore = new ConfigStoreOptions()
                .setType("env");
        
        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(envStore);
        
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            Map<String, Object> envConfig = ar.result().getMap();
            HttpKafkaConsumerConfig httpKafkaConsumerConfig = HttpKafkaConsumerConfig.fromMap(envConfig);

            HttpKafkaConsumer httpKafkaConsumer = new HttpKafkaConsumer(httpKafkaConsumerConfig);

            vertx.deployVerticle(httpKafkaConsumer, done -> {
                if (done.succeeded()) {
                    log.info("HTTP Kafka consumer started successfully");
                } else {
                    log.error("Failed to deploy HTTP Kafka consumer", done.cause());
                }
            });
        });
    }
}
