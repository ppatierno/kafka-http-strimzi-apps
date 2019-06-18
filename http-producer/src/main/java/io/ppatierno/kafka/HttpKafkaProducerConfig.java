package io.ppatierno.kafka;

import java.util.Map;

/**
 * HttpKafkaProducerConfig
 */
public class HttpKafkaProducerConfig {

    private static final String ENV_HOSTNAME = "HOSTNAME";
    private static final String ENV_PORT = "PORT";
    private static final String ENV_TOPIC = "TOPIC";
    private static final String ENV_SEND_INTERVAL = "SEND_INTERVAL";

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_TOPIC = "test";
    private static final int DEFAULT_SEND_INTERVAL = 1000;

    private final String hostname;
    private final int port;
    private final String topic;
    private final int sendInterval;

    /**
     * Constructor
     * 
     * @param hostname hostname to which connect to
     * @param port host port to which connect to
     * @param topic Kafka topic from which consume messages
     * @param sendInterval interval (in ms) for sending messages
     */
    private HttpKafkaProducerConfig(String hostname, int port, 
                                    String topic, int sendInterval) {
        this.hostname = hostname;
        this.port = port;
        this.topic = topic;
        this.sendInterval = sendInterval;
    }

    /**
     * @return hostname to which connect to
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @return host port to which connect to
     */
    public int getPort() {
        return port;
    }

    /**
     * @return Kafka topic from which consume messages
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return interval (in ms) for sending messages
     */
    public int getSendInterval() {
        return sendInterval;
    }

    /**
     * Load all HTTP Kafka producer configuration parameters from a related map
     * 
     * @param map map from which loading configuration parameters
     * @return HTTP Kafka producer configuration
     */
    public static HttpKafkaProducerConfig fromMap(Map<String, Object> map) {
        String hostname = (String) map.getOrDefault(ENV_HOSTNAME, DEFAULT_HOSTNAME);
        int port = Integer.parseInt(map.getOrDefault(ENV_PORT, DEFAULT_PORT).toString());
        String topic = (String) map.getOrDefault(ENV_TOPIC, DEFAULT_TOPIC);
        int sendInterval = Integer.parseInt(map.getOrDefault(ENV_SEND_INTERVAL, DEFAULT_SEND_INTERVAL).toString());
        return new HttpKafkaProducerConfig(hostname, port, topic, sendInterval);
    }

    @Override
    public String toString() {
        return "HttpKafkaProducerConfig(" +
                "hostname=" + this.hostname +
                ",port=" + this.port +
                ",topic=" + this.topic +
                ",sendInterval=" + this.sendInterval +
                ")";
    }
}