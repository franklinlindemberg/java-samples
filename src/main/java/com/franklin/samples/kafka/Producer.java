package com.franklin.samples.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void send(final String topic, final Models.User message) {
        logger.info("Producing message [{}]", message);

        kafkaTemplate.send(topic, message.toByteArray());
    }
}