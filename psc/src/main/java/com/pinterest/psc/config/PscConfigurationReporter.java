package com.pinterest.psc.config;

import com.google.gson.stream.JsonWriter;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.StringSerializer;

import java.io.IOException;
import java.io.StringWriter;

public class PscConfigurationReporter implements Runnable {
    private static final PscLogger logger = PscLogger.getLogger(PscConfigurationReporter.class);
    private static final String PSC_CONFIGURATION_REPORTER_CLIENT_ID = "psc-config-producer-client";

    private String pscConfigTopicUri;
    private PscConfiguration pscConfiguration;

    public PscConfigurationReporter(String pscConfigTopicUri, PscConfiguration pscConfiguration) {
        this.pscConfigTopicUri = pscConfigTopicUri;
        this.pscConfiguration = pscConfiguration;
    }

    @Override
    public void run() {
        report();
    }

    public void report() {
        // avoid infinite recursion
        if (PscConfigurationReporter.isThisYou(pscConfiguration))
            return;

        PscConfiguration producerConfiguration = new PscConfiguration();
        producerConfiguration.setProperty(PscConfiguration.PSC_PROJECT, "psc");
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, PSC_CONFIGURATION_REPORTER_CLIENT_ID);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NullMetricsReporter.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        // add any discovery config that the psc client may be using.
        pscConfiguration.getKeys("psc.discovery").forEachRemaining(key ->
                producerConfiguration.setProperty(key, pscConfiguration.getProperty(key))
        );

        this.pscConfiguration.addProperty("host", PscCommon.getHostname());
        this.pscConfiguration.addProperty("timestamp", System.currentTimeMillis());

        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        try (PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration)) {
            jsonWriter.beginObject();
            pscConfiguration.getKeys().forEachRemaining(key ->
            {
                try {
                    jsonWriter.name(key).value(pscConfiguration.getString(key));
                } catch (IOException e) {
                    logger.warn("Failed to convert key/value to json: {}/{}", key, pscConfiguration.getString(key));
                }
            });
            jsonWriter.endObject();

            PscProducerMessage<String, String> pscProducerMessage = new PscProducerMessage<>(
                    pscConfigTopicUri,
                    stringWriter.toString()
            );
            pscProducer.send(
                    pscProducerMessage,
                    (messageId, exception) -> {
                        if (exception == null)
                            logger.info("PSC configuration emission was completed. Reference: {}", messageId);
                        else
                            logger.warn("Exception thrown as a result of emitting PSC configuration.", exception);
                    }
            );
        } catch (Exception e) {
            logger.error("Failed to emit PSC configs to {}: {}", pscConfigTopicUri, e.getMessage(), e);
        }
    }

    public static boolean isThisYou(PscConfiguration pscConfiguration) {
        return pscConfiguration != null &&
                pscConfiguration.containsKey(PscConfiguration.PSC_PROJECT) &&
                pscConfiguration.getProperty(PscConfiguration.PSC_PROJECT).equals("psc") &&
                pscConfiguration.containsKey(PscConfiguration.PSC_PRODUCER_CLIENT_ID) &&
                ((String) pscConfiguration.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID)).contains(PSC_CONFIGURATION_REPORTER_CLIENT_ID);
    }
}
