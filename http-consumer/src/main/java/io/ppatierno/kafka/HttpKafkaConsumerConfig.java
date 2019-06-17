package io.ppatierno.kafka;

import java.util.Map;

/**
 * HttpKafkaConsumerConfig
 */
public class HttpKafkaConsumerConfig {

    private static final String ENV_HOSTNAME = "HOSTNAME";
    private static final String ENV_PORT = "PORT";
    private static final String ENV_TOPIC = "TOPIC";
    private static final String ENV_GROUPID = "GROUPID";
    private static final String ENV_POLL_INTERVAL = "POLL_INTERVAL";

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_TOPIC = "test";
    private static final String DEFAULT_GROUPID = "my-group";
    private static final int DEFAULT_POLL_INTERVAL = 1000;

    private final String hostname;
    private final int port;
    private final String topic;
    private final String groupid;
    private final int pollInterval;

    /**
     * Constructor
     * 
     * @param hostname hostname to which connect to
     * @param port host port to which connect to
     * @param topic Kafka topic from which consume messages
     * @param groupid consumer group name the consumer belong to
     * @param pollInterval interval (in ms) for polling to get messages
     */
    private HttpKafkaConsumerConfig(String hostname, int port, String topic, String groupid, int pollInterval) {
        this.hostname = hostname;
        this.port = port;
        this.topic = topic;
        this.groupid = groupid;
        this.pollInterval = pollInterval;
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
     * @return consumer group name the consumer belong to
     */
    public String getGroupid() {
        return groupid;
    }

    /**
     * @return interval (in ms) for polling to get messages
     */
    public int getPollInterval() {
        return pollInterval;
    }

    /**
     * Load all HTTP Kafka consumer configuration parameters from a related map
     * 
     * @param map map from which loading configuration parameters
     * @return HTTP Kafka consumer configuration
     */
    public static HttpKafkaConsumerConfig fromMap(Map<String, Object> map) {
        String hostname = (String) map.getOrDefault(ENV_HOSTNAME, DEFAULT_HOSTNAME);
        int port = (Integer) map.getOrDefault(ENV_PORT, DEFAULT_PORT);
        String topic = (String) map.getOrDefault(ENV_TOPIC, DEFAULT_TOPIC);
        String groupid = (String) map.getOrDefault(ENV_GROUPID, DEFAULT_GROUPID);
        int pollInterval = (Integer) map.getOrDefault(ENV_POLL_INTERVAL, DEFAULT_POLL_INTERVAL);
        return new HttpKafkaConsumerConfig(hostname, port, topic, groupid, pollInterval);
    }

    @Override
    public String toString() {
        return "HttpKafkaConsumerConfig(" +
                "hostname=" + this.hostname +
                ",port=" + this.port +
                ",topic=" + this.topic +
                ",groupid=" + this.groupid +
                ",pollInterval=" + this.pollInterval +
                ")";
    }
}