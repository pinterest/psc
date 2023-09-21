package com.pinterest.psc.example.kafka;

import java.util.Collections;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.StringDeserializer;

public class ExamplePscConsumer {

        private static final PscLogger logger = PscLogger.getLogger(ExamplePscConsumer.class);

        private static final long MILLIS_TO_RUN_CONSUMER = 10000;

    public static void main(String[] args) throws ConfigurationException, ConsumerException {
        if (args.length < 1) {
            logger.error("ExamplePscConsumer needs one argument: topicUri");
            return;
        }
        String topicUri = args[0];

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-psc-consumer-group");   // required
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-psc-consumer-client");  // required
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, false);   // set this to false in our example since we don't need to log the client configurations
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);

        PscConsumer<Integer, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUri));
        
        long timestamp = System.currentTimeMillis();
        int messagesConsumed = 0;
        while ((System.currentTimeMillis() - timestamp) < MILLIS_TO_RUN_CONSUMER) {
            PscConsumerPollMessageIterator<Integer, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                PscConsumerMessage<Integer, String> message = messageIterator.next();
                logger.info("Received message: " + message.toString());
                messagesConsumed++;
            }
        }
        pscConsumer.close();
        logger.info("Consumed " + messagesConsumed + " messages from " + topicUri);
    }
    
}
