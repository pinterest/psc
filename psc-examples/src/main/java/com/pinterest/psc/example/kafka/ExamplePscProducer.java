package com.pinterest.psc.example.kafka;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.StringSerializer;

public class ExamplePscProducer {

    private static final PscLogger logger = PscLogger.getLogger(ExamplePscProducer.class);
    private static final int NUM_MESSAGES = 10;

    public static void main(String[] args) throws ConfigurationException, ProducerException {
        if (args.length < 1) {
            logger.error("ExamplePscProducer needs one argument: topicUri");
            return;
        }
        String topicUri = args[0];

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-psc-producer-client");  // required
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, false);   // set this to false in our example since we don't need to log the client configurations
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());

        PscProducer<Integer, String> pscProducer = new PscProducer<>(pscConfiguration);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            PscProducerMessage<Integer, String> message = new PscProducerMessage<>(topicUri, i, "hello world " + i);
            logger.info("Sending message: " + message.toString() + " to topicUri " + topicUri);
            pscProducer.send(message);
        }

        pscProducer.close();
        
        logger.info("ExamplePscProducer sent " + NUM_MESSAGES + " messages to " + topicUri);
    }
}
