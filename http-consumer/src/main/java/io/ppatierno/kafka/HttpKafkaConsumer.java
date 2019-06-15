package io.ppatierno.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

/**
 * HttpKafkaConsumer
 */
public class HttpKafkaConsumer extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(HttpKafkaConsumer.class);

    private final HttpKafkaConsumerConfig config;

    /**
     * Constructor
     * 
     * @param config configuration
     */
    public HttpKafkaConsumer(HttpKafkaConsumerConfig config) {
        this.config = config;
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        log.info("HTTP Kafka consumer starting with config {}", this.config);
        super.start(startFuture);
    }
}