package com.elasticsearch.distributed.config;

/**
 * Immutable configuration record for Elasticsearch connection settings.
 *
 * <p>
 * All values are sourced from environment variables with sensible defaults.
 *
 * @param host              ES hostname or IP (env: {@code ES_HOST})
 * @param port              ES HTTP port (env: {@code ES_PORT})
 * @param scheme            {@code http} or {@code https} (env:
 *                          {@code ES_SCHEME})
 * @param apiKey            Base64-encoded API key (env: {@code ES_API_KEY});
 *                          may be null for local dev
 * @param maxConnTotal      Maximum total HTTP connections
 * @param maxConnPerRoute   Maximum HTTP connections per route
 * @param socketTimeoutMs   Socket timeout in milliseconds
 * @param connectTimeoutMs  Connect timeout in milliseconds
 * @param responseTimeoutMs Response timeout in milliseconds
 */
public record ElasticsearchConfig(
        String host,
        int port,
        String scheme,
        String apiKey,
        int maxConnTotal,
        int maxConnPerRoute,
        int socketTimeoutMs,
        int connectTimeoutMs,
        int responseTimeoutMs) {

    /** Default/dev configuration built from environment variables. */
    public static ElasticsearchConfig fromEnv() {
        return new ElasticsearchConfig(
                env("ES_HOST", "localhost"),
                Integer.parseInt(env("ES_PORT", "9200")),
                env("ES_SCHEME", "http"),
                System.getenv("ES_API_KEY"), // null is acceptable for local dev
                Integer.parseInt(env("ES_MAX_CONN_TOTAL", "200")),
                Integer.parseInt(env("ES_MAX_CONN_PER_ROUTE", "20")),
                Integer.parseInt(env("ES_SOCKET_TIMEOUT_MS", "30000")),
                Integer.parseInt(env("ES_CONNECT_TIMEOUT_MS", "5000")),
                Integer.parseInt(env("ES_RESPONSE_TIMEOUT_MS", "30000")));
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
