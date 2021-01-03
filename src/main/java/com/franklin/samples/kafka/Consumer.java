package com.franklin.samples.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private Models.User payload = null;

    @KafkaListener(topics = "${app.topic}")
    public void consume(byte[] message) throws IOException {
        latch.countDown();
        payload = Models.User.parseFrom(message);

        logger.info("Consumed message [{}]", payload);
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public Models.User getPayload() {
        return payload;
    }
}
