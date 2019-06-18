package io.ppatierno.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

/**
 * HttpKafkaProducer
 */
public class HttpKafkaProducer extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(HttpKafkaProducer.class);

    private final HttpKafkaProducerConfig config;

    private WebClient client;
    private long sendTimer;

    /**
     * Constructor
     * 
     * @param config configuration
     */
    public HttpKafkaProducer(HttpKafkaProducerConfig config) {
        this.config = config;
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        log.info("HTTP Kafka producer starting with config {}", this.config);

        WebClientOptions options = new WebClientOptions()
                .setDefaultHost(this.config.getHostname())
                .setDefaultPort(this.config.getPort());
        this.client = WebClient.create(vertx, options);

        this.sendTimer = vertx.setPeriodic(this.config.getSendInterval(), t -> {
            // TODO: send method
        });

        // TODO
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        log.info("HTTP Kafka producer stopping");

        // TODO
        this.vertx.cancelTimer(this.sendTimer);
        stopFuture.complete();
    }
}