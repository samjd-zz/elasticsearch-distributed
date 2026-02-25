package com.elasticsearch.distributed.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.elasticsearch.distributed.config.ElasticsearchConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Factory that owns the lifecycle of the single {@link ElasticsearchClient}.
 *
 * <p>
 * Create one instance at application start; call {@link #close()} at shutdown.
 * Implements {@link Closeable} so it can be used in try-with-resources.
 */
public final class ElasticsearchClientFactory implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchClientFactory.class);

    private final RestClient restClient;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    public ElasticsearchClientFactory(ElasticsearchConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        log.info("Initialising Elasticsearch client → {}://{}:{}", config.scheme(), config.host(), config.port());

        this.restClient = buildRestClient(config);
        var mapper = buildObjectMapper();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));
        this.client = new ElasticsearchClient(transport);
        log.info("Elasticsearch client ready");
    }

    /** Returns the singleton {@link ElasticsearchClient}. */
    public ElasticsearchClient client() {
        return client;
    }

    @Override
    public void close() {
        log.info("Closing Elasticsearch client");
        try {
            transport.close();
        } catch (IOException ex) {
            log.warn("Error closing ES transport", ex);
        }
        try {
            restClient.close();
        } catch (IOException ex) {
            log.warn("Error closing ES REST client", ex);
        }
    }

    // ── private helpers ───────────────────────────────────────────────────────

    private static RestClient buildRestClient(ElasticsearchConfig cfg) {
        var host = new HttpHost(cfg.host(), cfg.port(), cfg.scheme());
        RestClientBuilder builder = RestClient.builder(host)
                .setHttpClientConfigCallback(httpBuilder -> {
                    httpBuilder
                            .setMaxConnTotal(cfg.maxConnTotal())
                            .setMaxConnPerRoute(cfg.maxConnPerRoute());

                    if (cfg.apiKey() != null) {
                        var cp = new BasicCredentialsProvider();
                        // For API-key auth the header is added via RequestOptions;
                        // credentials provider is kept for future basic-auth support.
                        cp.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("", cfg.apiKey()));
                        httpBuilder.setDefaultCredentialsProvider(cp);
                    }
                    return httpBuilder;
                })
                .setRequestConfigCallback(reqBuilder -> reqBuilder
                        .setSocketTimeout(cfg.socketTimeoutMs())
                        .setConnectTimeout(cfg.connectTimeoutMs()));
        return builder.build();
    }

    private static ObjectMapper buildObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new ParameterNamesModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
