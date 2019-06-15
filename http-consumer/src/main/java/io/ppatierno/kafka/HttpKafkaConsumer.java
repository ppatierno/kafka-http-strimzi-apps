package io.ppatierno.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

/**
 * HttpKafkaConsumer
 */
public class HttpKafkaConsumer extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(HttpKafkaConsumer.class);

    private final HttpKafkaConsumerConfig config;

    private WebClient client;
    private CreatedConsumer consumer;

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
        WebClientOptions options = new WebClientOptions()
                .setDefaultHost(this.config.getHostname())
                .setDefaultPort(this.config.getPort());
        this.client = WebClient.create(vertx, options);
        
        this.createConsumer()
        .compose(consumer -> this.subscribe(consumer, this.config.getTopic()))
        .compose(startFuture::complete, startFuture);
    }

    private Future<CreatedConsumer> createConsumer() {
        Future<CreatedConsumer> fut = Future.future();

        JsonObject json = new JsonObject()
            .put("name", "my-consumer")
            .put("format", "json");

        this.client.post("/consumers/" + this.config.getGroupid())
            .putHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(json.toBuffer().length()))
            .putHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "application/vnd.kafka.v2+json")
            .as(BodyCodec.jsonObject())
            .sendJsonObject(json, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<JsonObject> response = ar.result();
                    if (response.statusCode() == HttpResponseStatus.OK.code()) {
                        JsonObject body = response.body();
                        this.consumer = new CreatedConsumer(body.getString("instance_id"), body.getString("base_uri"));
                        log.info("Consumer created as {}", this.consumer);
                        fut.complete(consumer);
                    } else {
                        fut.fail(new RuntimeException("Got HTTP status code " + response.statusCode()));
                    }
                } else {
                    fut.fail(ar.cause());
                }
            });
        return fut;
    }

    private Future<Void> subscribe(CreatedConsumer consumer, String topic) {
        Future<Void> fut = Future.future();

        JsonObject topics = new JsonObject()
            .put("topics", new JsonArray().add(topic));
        
        this.client.post(consumer.baseUri + "/subscription")
            .putHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(topics.toBuffer().length()))
            .putHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "application/vnd.kafka.v2+json")
            .as(BodyCodec.jsonObject())
            .sendJsonObject(topics, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<JsonObject> response = ar.result();
                    if (response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
                        log.info("Subscribed to {}", topic);
                        fut.complete();
                    } else {
                        fut.fail(new RuntimeException("Got HTTP status code " + response.statusCode()));
                    }
                } else {
                    fut.fail(ar.cause());
                }
            });
        return fut;
    }

    /**
     * Information about using the consumer on the bridge
     */
    class CreatedConsumer {
    
        private final String instanceId;
        private final String baseUri;
        
        CreatedConsumer(String instanceId, String baseUri) {
            this.instanceId = instanceId;
            this.baseUri = baseUri;
        }

        /**
         * @return consumer instance-id/name
         */
        public String getInstanceId() {
            return instanceId;
        }

        /**
         * @return consumer URI to use for all next calls
         */
        public String getBaseUri() {
            return baseUri;
        }

        @Override
        public String toString() {
            return "CreatedConsumer(" +
                    "instanceId=" + this.instanceId +
                    ",baseUri=" + this.baseUri +
                    ")";
        }
    }
}