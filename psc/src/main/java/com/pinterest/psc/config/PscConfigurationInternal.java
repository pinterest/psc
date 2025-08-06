package com.pinterest.psc.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.listener.MessageListener;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.environment.EnvironmentProvider;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.metrics.OpenTSDBReporter;
import com.pinterest.psc.serde.Deserializer;
import com.pinterest.psc.serde.Serializer;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class PscConfigurationInternal {
    private static final PscLogger logger = PscLogger.getLogger(PscConfigurationInternal.class);

    private final static String PSC_CLIENT_TYPE = "psc.client.type";
    public final static String PSC_CLIENT_TYPE_CONSUMER = "consumer";
    public final static String PSC_CLIENT_TYPE_PRODUCER = "producer";
    public final static String PSC_CLIENT_TYPE_METADATA = "metadata";
    private final static String[] PSC_VALID_CLIENT_TYPES = {PSC_CLIENT_TYPE_CONSUMER, PSC_CLIENT_TYPE_PRODUCER, PSC_CLIENT_TYPE_METADATA};

    private PscConfiguration pscConfiguration;
    private Deserializer keyDeserializer, valueDeserializer;
    private Serializer keySerializer, valueSerializer;
    private MessageListener messageListener;
    private boolean configLoggingEnabled;
    private String configLoggingTopicUri;
    private boolean metricsReportingEnabled;
    private String metricsReporterClass;
    private Environment environment;
    private List<TypePreservingInterceptor> typedInterceptors = new ArrayList<>();
    private List<TypePreservingInterceptor<byte[], byte[]>> rawInterceptors = new ArrayList<>();
    private boolean autoResolutionEnabled;
    private int autoResolutionRetryCount;
    private MetricsReporterConfiguration metricsReporterConfiguration;
    private boolean proactiveSslResetEnabled;

    public PscConfigurationInternal() {
    }

    public PscConfigurationInternal(String configOverrideFile, String clientType) throws ConfigurationException {
        loadDefaultConfiguration();
        PscConfiguration overrideConfiguration = getConfiguration(configOverrideFile);
        PscConfigurationUtils.copy(overrideConfiguration, pscConfiguration);
        pscConfiguration.setProperty(PSC_CLIENT_TYPE, clientType);
        validate();
    }

    @Deprecated
    public PscConfigurationInternal(Configuration configuration, String clientType) throws ConfigurationException {
        loadDefaultConfiguration();
        PscConfigurationUtils.copy(configuration, this.pscConfiguration);
        this.pscConfiguration.setProperty(PSC_CLIENT_TYPE, clientType);
        validate();
    }

    public PscConfigurationInternal(PscConfiguration pscConfiguration, String clientType) throws ConfigurationException {
        this(pscConfiguration, clientType, false);
    }

    public PscConfigurationInternal(PscConfiguration pscConfiguration, String clientType, boolean isLenient) throws ConfigurationException {
        loadDefaultConfiguration();
        PscConfigurationUtils.copy(pscConfiguration, this.pscConfiguration);
        this.pscConfiguration.setProperty(PSC_CLIENT_TYPE, clientType);
        validate(isLenient, true);
    }

    public PscConfigurationInternal(PscConfiguration pscConfiguration, String clientType, boolean isLenient, boolean isLogConfiguration) throws ConfigurationException {
        loadDefaultConfiguration();
        PscConfigurationUtils.copy(pscConfiguration, this.pscConfiguration);
        this.pscConfiguration.setProperty(PSC_CLIENT_TYPE, clientType);
        validate(isLenient, isLogConfiguration);
    }

    public PscConfigurationInternal(PscConfigurationInternal otherConfiguration, String clientType) throws ConfigurationException {
        pscConfiguration = otherConfiguration.getConfiguration();
        pscConfiguration.setProperty(PSC_CLIENT_TYPE, clientType);
        validate();
    }

    private PscConfiguration getConfiguration(String filename) throws ConfigurationException {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
            if (inputStream == null)
                throw new ConfigurationException("Default PSC configuration file could not be found: " + filename);
            logger.info("Loading default PSC configuration file: {}", filename);
            PscConfiguration pscConfiguration = new PscConfiguration();
            pscConfiguration.read(new InputStreamReader(inputStream));
            return pscConfiguration;
        } catch (org.apache.commons.configuration2.ex.ConfigurationException | IOException e) {
            throw new ConfigurationException("Could not process PSC configuration file: " + filename, e);
        }
    }

    public void loadDefaultConfiguration() throws ConfigurationException {
        pscConfiguration = getConfiguration("psc.conf");
    }

    @VisibleForTesting
    public void overrideDefaultConfigurations(String key, String value) throws ConfigurationException {
        pscConfiguration.setProperty(key, value);
        validate();
    }

    protected void validate(boolean isLenient, boolean isLogConfiguration) throws ConfigurationException {
        // remove configuration properties with empty value
        Iterator<String> keyIterator = pscConfiguration.getKeys();
        String key;
        while (keyIterator.hasNext()) {
            key = keyIterator.next();
            if (pscConfiguration.getString(key).isEmpty())
                keyIterator.remove();
        }

        // validate client type
        switch (getClientType()) {
            case PSC_CLIENT_TYPE_CONSUMER:
                validateConsumerConfiguration(isLenient, isLogConfiguration);
                break;
            case PSC_CLIENT_TYPE_PRODUCER:
                validateProducerConfiguration(isLenient, isLogConfiguration);
                break;
            case PSC_CLIENT_TYPE_METADATA:
                validateMetadataClientConfiguration(isLenient, isLogConfiguration);
                break;
            default:
                throw new ConfigurationException("Valid client type expected: " + String.join(", ", PSC_VALID_CLIENT_TYPES));
        }

        configureMetrics();
        configureEnvironment();
    }

    protected void validate() throws ConfigurationException {
        validate(false, true);
    }

    private void configureMetrics() {
        metricsReporterConfiguration = new MetricsReporterConfiguration(
            isPscMetricsReportingEnabled(),
            getPscMetricsReporterClass(),
            getConfiguration().getInt(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM),
            getConfiguration().getString(PscConfiguration.PSC_METRICS_HOST),
            getConfiguration().getInt(PscConfiguration.PSC_METRICS_PORT),
            getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS)
        );
    }

    private void configureEnvironment() throws ConfigurationException {
        EnvironmentProvider provider = PscUtils.instantiateFromClass(
                pscConfiguration.getString(PscConfiguration.PSC_ENVIRONMENT_PROVIDER_CLASS), EnvironmentProvider.class
        );
        provider.configure(getSubsetConfiguration(PscConfiguration.PSC_ENVIRONMENT));
        environment = provider.getEnvironment();
    }

    private void validateGenericConfiguration(Map<String, Exception> invalidConfigs) {
        // config logging
        String configuredConfigLoggingEnabled = pscConfiguration.getString(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED);
        if (configuredConfigLoggingEnabled == null || configuredConfigLoggingEnabled.trim().isEmpty())
            this.configLoggingEnabled = PscUtils.isEc2Host() ? Boolean.TRUE : Boolean.FALSE;
        else {
            Boolean configLoggingEnabled =
                    verifyConfigHasValue(pscConfiguration, PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, Boolean.class, invalidConfigs);
            this.configLoggingEnabled = configLoggingEnabled != null ? configLoggingEnabled : Boolean.FALSE;
        }

        if (configLoggingEnabled)
            configLoggingTopicUri = verifyConfigHasValue(pscConfiguration, PscConfiguration.PSC_CONFIG_TOPIC_URI, String.class, invalidConfigs);

        // metric reporting
        String configuredMetricsReportingEnabled = pscConfiguration.getString(PscConfiguration.PSC_METRIC_REPORTING_ENABLED);
        if (configuredMetricsReportingEnabled == null || configuredMetricsReportingEnabled.trim().isEmpty())
            this.metricsReportingEnabled = true;
        else
            this.metricsReportingEnabled = Boolean.parseBoolean(configuredMetricsReportingEnabled);

        if (metricsReportingEnabled) {
            // auto configure metrics reporter class based on environment
            String configuredMetricsReporterClass = pscConfiguration.getString(PscConfiguration.PSC_METRICS_REPORTER_CLASS);
            this.metricsReporterClass = (configuredMetricsReporterClass == null || configuredMetricsReporterClass.trim().isEmpty()) ?
                    (PscUtils.isEc2Host() ? OpenTSDBReporter.class.getName() : NullMetricsReporter.class.getName()) :
                    configuredMetricsReporterClass;
        }

        // auto resolution
        Boolean autoResolutionEnabled = verifyConfigHasValue(pscConfiguration, PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, Boolean.class, invalidConfigs);
        this.autoResolutionEnabled = autoResolutionEnabled != null ? autoResolutionEnabled : true;
        if (autoResolutionEnabled) {
            Integer autoResolutionRetryCount = verifyConfigHasValue(pscConfiguration, PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, Integer.class, invalidConfigs);
            this.autoResolutionRetryCount = autoResolutionRetryCount != null ? autoResolutionRetryCount : 5;
        }

        // SSL reset
        Boolean proactiveSslResetEnabled = verifyConfigHasValue(pscConfiguration, PscConfiguration.PSC_PROACTIVE_SSL_RESET_ENABLED, Boolean.class, invalidConfigs);
        this.proactiveSslResetEnabled = proactiveSslResetEnabled != null ? proactiveSslResetEnabled : false;    // false by default
    }

    public void logConfiguration() {
        // do not log configuration for the internal config reporter
        if (getClientType().equals(PSC_CLIENT_TYPE_PRODUCER) && PscConfigurationReporter.isThisYou(pscConfiguration))
            return;

        StringBuilder stringBuilder = new StringBuilder(
                String.format("PSC %s configuration:\n", pscConfiguration.getProperty(PSC_CLIENT_TYPE))
        );
        Set<String> inactiveClientTypes = Sets.newHashSet(PSC_VALID_CLIENT_TYPES);
        inactiveClientTypes.remove(pscConfiguration.getProperty(PSC_CLIENT_TYPE));
        PscConfiguration confToReport = new PscConfiguration();

        // overwrite potential dynamic environment specific configs
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, getPscMetricsReporterClass());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, Boolean.toString(isConfigLoggingEnabled()));
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_TOPIC_URI, getConfigLoggingTopicUri());

        pscConfiguration.getKeys().forEachRemaining(key -> {
            if (
                    !key.startsWith("psc.") ||
                            key.split("\\.").length <= 1 ||
                            !inactiveClientTypes.contains(key.split("\\.")[1])
            ) {
                confToReport.setProperty(key, pscConfiguration.getString(key));
                stringBuilder.append(
                        String.format("\t%s: %s\n", key,
                                key.endsWith(".password") ? "[hidden]" : pscConfiguration.getString(key)
                        )
                );
            }
        });
        logger.info(stringBuilder.toString());

        if (!configLoggingEnabled) {
            logger.info("Skipping config logging because 'psc.config.logging.enabled' is not set to true. " +
                    "For staging/production applications enabling this config is highly recommended.");
            return;
        }

        emitConfiguration(confToReport);
    }

    private void emitConfiguration(PscConfiguration confToReport) {
        // emit config if a target topic uri is provided
        String pscConfigTopicUri = confToReport.getString(PscConfiguration.PSC_CONFIG_TOPIC_URI);
        if (pscConfigTopicUri == null || pscConfigTopicUri.isEmpty())
            return;

        PscConfigurationReporter pscConfigurationReporter = new PscConfigurationReporter(
                pscConfigTopicUri,
                confToReport
        );
        new Thread(pscConfigurationReporter).start();
    }

    private void validateConsumerConfiguration(boolean isLenient, boolean isLogConfiguration) throws ConfigurationException {
        PscConfiguration consumerConfiguration = new PscConfiguration();
        consumerConfiguration.copy(pscConfiguration.subset(PscConfiguration.PSC_CONSUMER));
        Map<String, Exception> invalidConfigs = new HashMap<>();
        validateDeserializers(consumerConfiguration, invalidConfigs);
        validateInterceptors(consumerConfiguration, invalidConfigs);
        validateMessageListener(consumerConfiguration, invalidConfigs);
        validateOtherConsumerConfigs(consumerConfiguration, invalidConfigs);
        validateAdditionalRequiredConfigs(invalidConfigs);
        validateGenericConfiguration(invalidConfigs);

        if (isLogConfiguration)
            logConfiguration();

        if (invalidConfigs.isEmpty() || isLenient)
            return;

        StringBuilder stringBuilder = new StringBuilder();
        invalidConfigs.forEach((error, exception) ->
                stringBuilder.append(String.format("\t%s: %s\n", error, exception == null ? "" : exception.getMessage()))
        );
        throw new ConfigurationException("Invalid consumer configuration\n" + stringBuilder.toString());
    }

    private void validateDeserializers(
            PscConfiguration consumerConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        validateDeserializer(consumerConfiguration, invalidConfigs, true);
        validateDeserializer(consumerConfiguration, invalidConfigs, false);
    }

    private void validateDeserializer(
            PscConfiguration consumerConfiguration,
            Map<String, Exception> invalidConfigs,
            boolean isKeyDeserializer
    ) {
        String deserializerConfigName = isKeyDeserializer ? PscConfiguration.KEY_DESERIALIZER : PscConfiguration.VALUE_DESERIALIZER;
        if (!consumerConfiguration.containsKey(deserializerConfigName)) {
            invalidConfigs.put(deserializerConfigName + ": Config not found", null);
            return;
        }

        Deserializer deserializer = null;
        Object deserializerConfigValue = consumerConfiguration.getProperty(deserializerConfigName);
        if (deserializerConfigValue instanceof String) {
            String deserializerConfigValueFqdn = ((String) deserializerConfigValue).trim();
            if (deserializerConfigValueFqdn.isEmpty())
                invalidConfigs.put(deserializerConfigName + ": Must have a value", null);
            else {
                try {
                    deserializer = Class.forName(deserializerConfigValueFqdn).asSubclass(Deserializer.class)
                            .newInstance();
                } catch (ClassNotFoundException e) {
                    invalidConfigs.put(String.format("%s: %s deserializer class not found: '%s'\n",
                            deserializerConfigName, isKeyDeserializer ? "Key" : "Value", deserializerConfigValueFqdn), e
                    );
                } catch (IllegalAccessException | InstantiationException e) {
                    invalidConfigs.put(String.format("%s: Could not instantiate from %s deserializer class: '%s'\n",
                            deserializerConfigName, isKeyDeserializer ? "key" : "value", deserializerConfigValueFqdn), e
                    );
                }
            }
        } else if (deserializerConfigValue instanceof Deserializer)
            deserializer = (Deserializer) deserializerConfigValue;
        else
            invalidConfigs.put(deserializerConfigName + " is not a FQDN or a deserializer object", null);

        if (isKeyDeserializer)
            keyDeserializer = deserializer;
        else
            valueDeserializer = deserializer;
    }

    private List<TypePreservingInterceptor> getInterceptorObjects(
            Object interceptorConfigValue,
            boolean isTyped,
            Map<String, Exception> invalidConfigs
    ) {
        List<TypePreservingInterceptor> interceptorObjects = new ArrayList<>();
        if (interceptorConfigValue instanceof String) {
            String[] rawInterceptorFqdns = getMultiValueConfiguration((String) interceptorConfigValue);
            Arrays.stream(rawInterceptorFqdns).forEach(interceptorClassFqdn -> {
                if (interceptorClassFqdn.isEmpty()) {
                    invalidConfigs.put(
                            String.format("%s: invalid %s interceptor config value: '%s'\n",
                                    isTyped ? PscConfiguration.INTERCEPTORS_TYPED_CLASSES : PscConfiguration.INTERCEPTORS_RAW_CLASSES,
                                    isTyped ? "typed" : "raw",
                                    interceptorClassFqdn
                            ), null
                    );
                }

                try {
                    interceptorObjects.add(
                            Class.forName(interceptorClassFqdn).asSubclass(TypePreservingInterceptor.class).newInstance()
                    );
                } catch (ClassNotFoundException e) {
                    invalidConfigs.put(
                            String.format("%s: %s interceptor class not found: '%s'\n",
                                    isTyped ? PscConfiguration.INTERCEPTORS_TYPED_CLASSES : PscConfiguration.INTERCEPTORS_RAW_CLASSES,
                                    isTyped ? "typed" : "raw",
                                    interceptorClassFqdn
                            ), e
                    );
                } catch (IllegalAccessException | InstantiationException e) {
                    invalidConfigs.put(
                            String.format("%s: Could not instantiate from %s interceptor class: '%s'\n",
                                    isTyped ? PscConfiguration.INTERCEPTORS_TYPED_CLASSES : PscConfiguration.INTERCEPTORS_RAW_CLASSES,
                                    isTyped ? "typed" : "raw",
                                    interceptorClassFqdn
                            ), e
                    );
                }
            });
        } else if (interceptorConfigValue instanceof TypePreservingInterceptor)
            interceptorObjects.add((TypePreservingInterceptor) interceptorConfigValue);
        else if (interceptorConfigValue instanceof List) {
            ((List) interceptorConfigValue).forEach(subInterceptorConfigValue ->
                    interceptorObjects.addAll(getInterceptorObjects(
                            subInterceptorConfigValue,
                            isTyped,
                            invalidConfigs
                    ))
            );
        }
        else {
            invalidConfigs.put(
                    String.format("%s: Provided %s interceptor '%s' is not a FQDN or an interceptor object.\n",
                            isTyped ? PscConfiguration.INTERCEPTORS_TYPED_CLASSES : PscConfiguration.INTERCEPTORS_RAW_CLASSES,
                            isTyped ? "typed" : "raw",
                            interceptorConfigValue
                    ), null
            );
        }

        return interceptorObjects;
    }

    private void validateInterceptors(
            PscConfiguration clientConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        Object rawInterceptorsConfigValue = clientConfiguration.getProperty(PscConfiguration.INTERCEPTORS_RAW_CLASSES);
        if (rawInterceptorsConfigValue != null) {
            this.rawInterceptors = getInterceptorObjects(
                    rawInterceptorsConfigValue, false, invalidConfigs
            ).stream().map(object -> (TypePreservingInterceptor<byte[], byte[]>) object).collect(Collectors.toList());
        }

        Object typedInterceptorsConfigValue = clientConfiguration.getProperty(PscConfiguration.INTERCEPTORS_TYPED_CLASSES);
        if (typedInterceptorsConfigValue != null) {
            this.typedInterceptors = getInterceptorObjects(
                    typedInterceptorsConfigValue, true, invalidConfigs
            );
        }
    }

    private void validateMessageListener(
            PscConfiguration consumerConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        if (!consumerConfiguration.containsKey(PscConfiguration.MESSAGE_LISTENER))
            return;

        Object messageListenerConfigValue = consumerConfiguration.getProperty(PscConfiguration.MESSAGE_LISTENER);
        if (messageListenerConfigValue instanceof String) {
            String messageListenerConfigValueFqdn = ((String) messageListenerConfigValue).trim();
            if (messageListenerConfigValueFqdn.isEmpty())
                return;

            try {
                messageListener = Class.forName(messageListenerConfigValueFqdn).asSubclass(MessageListener.class)
                        .newInstance();
            } catch (ClassNotFoundException e) {
                invalidConfigs.put(String.format("%s: Message listener class not found: '%s'\n",
                        PscConfiguration.MESSAGE_LISTENER, messageListenerConfigValueFqdn), e
                );
            } catch (IllegalAccessException | InstantiationException e) {
                invalidConfigs.put(String.format("%s: Could not instantiate from message listener class: '%s'\n",
                        PscConfiguration.MESSAGE_LISTENER, messageListenerConfigValueFqdn), e
                );
            }
        } else if (messageListenerConfigValue instanceof MessageListener)
            messageListener = (MessageListener) messageListenerConfigValue;
        else
            invalidConfigs.put("Provided message listener is not a FQDN or a message listener object", null);
    }

    private void validateOtherConsumerConfigs(
            PscConfiguration consumerConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        verifyConfigHasValue(consumerConfiguration, PscConfiguration.CLIENT_ID, String.class, invalidConfigs);
        verifyConfigHasValue(consumerConfiguration, PscConfiguration.GROUP_ID, String.class, invalidConfigs);
        verifyConfigHasValue(consumerConfiguration, PscConfiguration.POLL_TIMEOUT_MS, Long.class, invalidConfigs);
        verifyConfigHasValue(consumerConfiguration, PscConfiguration.POLL_MESSAGES_MAX, Integer.class, invalidConfigs);
    }

    private <T> T verifyConfigHasValue(
            PscConfiguration configuration,
            String configKey,
            Class<T> expectedType,
            Map<String, Exception> invalidConfigs
    ) {
        if (!configuration.containsKey(configKey)) {
            invalidConfigs.put(configKey + ": Config not found", null);
            return null;
        } else if (configuration.getString(configKey).trim().isEmpty()) {
            invalidConfigs.put(configKey + ": Must have a value", null);
            return null;
        }
        return configuration.get(expectedType, configKey);
    }

    private void validateMetadataClientConfiguration(boolean isLenient, boolean isLogConfiguration) throws ConfigurationException {
        PscConfiguration metadataConfiguration = new PscConfiguration();
        metadataConfiguration.copy(pscConfiguration.subset(PscConfiguration.PSC_METADATA));
        Map<String, Exception> invalidConfigs = new HashMap<>();
        verifyConfigHasValue(metadataConfiguration, PscConfiguration.CLIENT_ID, String.class, invalidConfigs);
        if (isLogConfiguration)
            logConfiguration();

        if (invalidConfigs.isEmpty() || isLenient)
            return;

        StringBuilder stringBuilder = new StringBuilder();
        invalidConfigs.forEach((error, exception) ->
                stringBuilder.append(String.format("\t%s: %s\n", error, exception == null ? "" : exception.getMessage()))
        );
        throw new ConfigurationException("Invalid metadataClient configuration\n" + stringBuilder.toString());
    }

    private void validateProducerConfiguration(boolean isLenient, boolean isLogConfiguration) throws ConfigurationException {
        PscConfiguration producerConfiguration = new PscConfiguration();
        producerConfiguration.copy(pscConfiguration.subset(PscConfiguration.PSC_PRODUCER));
        Map<String, Exception> invalidConfigs = new HashMap<>();
        validateSerializers(producerConfiguration, invalidConfigs);
        validateInterceptors(producerConfiguration, invalidConfigs);
        validateOtherProducerConfigs(producerConfiguration, invalidConfigs);
        validateAdditionalRequiredConfigs(invalidConfigs);
        validateGenericConfiguration(invalidConfigs);
        if (isLogConfiguration)
            logConfiguration();

        if (invalidConfigs.isEmpty() || isLenient)
            return;

        StringBuilder stringBuilder = new StringBuilder();
        invalidConfigs.forEach((error, exception) ->
                stringBuilder.append(String.format("\t%s: %s\n", error, exception == null ? "" : exception.getMessage()))
        );
        throw new ConfigurationException("Invalid producer configuration\n" + stringBuilder.toString());
    }

    private void validateSerializers(
            PscConfiguration producerConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        validateSerializer(producerConfiguration, invalidConfigs, true);
        validateSerializer(producerConfiguration, invalidConfigs, false);
    }

    private void validateSerializer(
            PscConfiguration producerConfiguration,
            Map<String, Exception> invalidConfigs,
            boolean isKeySerializer
    ) {
        String serializerConfigName = isKeySerializer ? PscConfiguration.KEY_SERIALIZER : PscConfiguration.VALUE_SERIALIZER;
        if (!producerConfiguration.containsKey(serializerConfigName)) {
            invalidConfigs.put(serializerConfigName + ": Config not found", null);
            return;
        }

        Serializer serializer = null;
        Object serializerConfigValue = producerConfiguration.getProperty(serializerConfigName);
        if (serializerConfigValue instanceof String) {
            String serializerConfigValueFqdn = ((String) serializerConfigValue).trim();
            if (serializerConfigValueFqdn.isEmpty())
                invalidConfigs.put(serializerConfigName + ": Must have a value", null);
            else {
                try {
                    serializer = Class.forName(serializerConfigValueFqdn).asSubclass(Serializer.class).newInstance();
                } catch (ClassNotFoundException e) {
                    invalidConfigs.put(String.format("%s: %s serializer class not found: '%s'\n",
                            serializerConfigName, isKeySerializer ? "Key" : "Value", serializerConfigValueFqdn), e
                    );
                } catch (IllegalAccessException | InstantiationException e) {
                    invalidConfigs.put(String.format("%s: Could not instantiate from %s serializer class: '%s'\n",
                            serializerConfigName, isKeySerializer ? "key" : "value", serializerConfigValueFqdn), e
                    );
                }
            }
        } else if (serializerConfigValue instanceof Serializer)
            serializer = (Serializer) serializerConfigValue;
        else
            invalidConfigs.put(serializerConfigName + " is not a FQDN or a serializer object", null);

        if (isKeySerializer)
            keySerializer = serializer;
        else
            valueSerializer = serializer;
    }

    private void validateOtherProducerConfigs(
            PscConfiguration producerConfiguration,
            Map<String, Exception> invalidConfigs
    ) {
        verifyConfigHasValue(producerConfiguration, PscConfiguration.CLIENT_ID, String.class, invalidConfigs);
    }

    private void validateAdditionalRequiredConfigs(Map<String, Exception> invalidConfigs) {
        Set<String> addtionalRequiredConfigs = getAdditionalRequiredConfigs();
        for (String reqKey : addtionalRequiredConfigs) {
            verifyConfigHasValue(pscConfiguration, reqKey, Object.class, invalidConfigs);
        }
    }

    private PscConfiguration getSubsetConfiguration(String subsetPrefix) {
        PscConfiguration subsetConfiguration = new PscConfiguration();
        subsetConfiguration.copy(pscConfiguration.subset(subsetPrefix));
        return subsetConfiguration;
    }

    public static String[] getMultiValueConfiguration(PscConfiguration pscConfiguration, String configurationKey) {
        String value = pscConfiguration.getString(configurationKey);
        return getMultiValueConfiguration(value);
    }

    public static String[] getMultiValueConfiguration(String configurationValue) {
        if (configurationValue != null)
            return configurationValue.split("\\s*,\\s*");
        return new String[]{};
    }

    @VisibleForTesting
    protected static Set<String> getAdditionalRequiredConfigs() {
        Properties properties = new Properties();
        try {
            properties.load(PscConsumer.class.getClassLoader().getResourceAsStream("psc.properties"));
        } catch (IOException ioe) {
            logger.warn("Failed to get psc.additionalRequiredConfigs", ioe);
        }
        Set<String> additionalRequiredConfigs = new HashSet<>();
        if (properties.containsKey("psc.additionalRequiredConfigs")) {
            String additionalRequiredConfigsString = properties.getProperty(
                    "psc.additionalRequiredConfigs", "");
            if (!additionalRequiredConfigsString.isEmpty()) {
                Collections.addAll(additionalRequiredConfigs,
                        additionalRequiredConfigsString.replaceAll(" ", "").split(","));
            }
        }
        return additionalRequiredConfigs;
    }

    @VisibleForTesting
    protected boolean isConfigLoggingEnabled() {
        return configLoggingEnabled;
    }

    public String getConfigLoggingTopicUri() {
        return configLoggingTopicUri;
    }

    public PscConfiguration getConsumerConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_CONSUMER);
    }

    public PscConfiguration getProducerConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_PRODUCER);
    }

    public PscConfiguration getMetadataClientConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_METADATA);
    }

    public PscConfiguration getMetricsConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_METRICS);
    }

    public PscConfiguration getEnvironmentConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_ENVIRONMENT);
    }

    public PscConfiguration getDiscoveryConfiguration() {
        return getSubsetConfiguration(PscConfiguration.PSC_DISCOVERY);
    }

    public PscConfiguration getConfiguration() {
        return pscConfiguration;
    }

    public String getPscConsumerClientId() {
        return pscConfiguration.getString(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
    }

    public String getPscConsumerGroupId() {
        return pscConfiguration.getString(PscConfiguration.PSC_CONSUMER_GROUP_ID);
    }

    public Long getPscConsumerPollTimeoutMs() {
        return pscConfiguration.getLong(PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS);
    }

    public Integer getPscConsumerPollMessagesMax() {
        return pscConfiguration.getInt(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX);
    }

    public Deserializer getPscConsumerKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer getPscConsumerValueDeserializer() {
        return valueDeserializer;
    }

    public Serializer getPscProducerKeySerializer() {
        return keySerializer;
    }

    public Serializer getPscProducerValueSerializer() {
        return valueSerializer;
    }

    public List<TypePreservingInterceptor<byte[], byte[]>> getRawPscConsumerInterceptors() {
        return rawInterceptors;
    }

    public List<TypePreservingInterceptor> getTypedPscConsumerInterceptors() {
        return typedInterceptors;
    }

    public MessageListener getPscConsumerMessageListener() {
        return messageListener;
    }

    public String getPscConsumerAssignmentStrategyClass() {
        return pscConfiguration.getString(PscConfiguration.PSC_CONSUMER_ASSIGNMENT_STRATEGY_CLASS);
    }

    public String getPscProducerKeySerializerClass() {
        return pscConfiguration.getString(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER);
    }

    public String getPscProducerValueSerializerClass() {
        return pscConfiguration.getString(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER);
    }

    public List<TypePreservingInterceptor<byte[], byte[]>> getRawPscProducerInterceptors() {
        return rawInterceptors;
    }

    public List<TypePreservingInterceptor> getTypedPscProducerInterceptors() {
        return typedInterceptors;
    }

    public String getPscProducerClientId() {
        return pscConfiguration.getString(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
    }

    public boolean isPscMetricsReportingEnabled() {
        return metricsReportingEnabled;
    }

    public String getPscMetricsReporterClass() {
        return metricsReporterClass;
    }

    public String getPscEnvironmentProviderClass() {
        return pscConfiguration.getString(PscConfiguration.PSC_ENVIRONMENT_PROVIDER_CLASS);
    }

    public static String getPscDiscoveryServiceProviderFallbackConfigName() {
        return PscConfiguration.FALLBACK_FILE;
    }

    public String getClientType() {
        return pscConfiguration.getString(PSC_CLIENT_TYPE);
    }

    public String getProject() {
        return pscConfiguration.getString(PscConfiguration.PSC_PROJECT);
    }

    public Environment getEnvironment() {
        return environment;
    }

    public boolean isAutoResolutionEnabled() {
        return autoResolutionEnabled;
    }

    public boolean isProactiveSslResetEnabled() {
        return proactiveSslResetEnabled;
    }

    public int getAutoResolutionRetryCount() {
        return autoResolutionRetryCount;
    }

    public MetricsReporterConfiguration getMetricsReporterConfiguration() {
        return metricsReporterConfiguration;
    }

    public String getMetadataClientId() {
        return pscConfiguration.getString(PscConfiguration.PSC_METADATA_CLIENT_ID);
    }
}
