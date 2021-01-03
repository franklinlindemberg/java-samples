package com.franklin.samples.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://${spring.kafka.consumer.bootstrap-servers}"})
class KafkaIntegrationTest
{
    @Autowired
    private Consumer consumer;

    @Autowired
    private Producer producer;

    @Value("${app.topic}")
    private String topic;

    @Test
    public void whenUserMessageIsProduced_thenUserMessageIsConsumed() throws Exception
    {
        final Models.User userMessage = Models.User.newBuilder()
                .setName("Jonh")
                .setAge(33)
                .build();

        producer.send(topic, userMessage);

        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(consumer.getLatch().getCount(), equalTo(0L));
        assertThat(consumer.getPayload(), equalTo(userMessage));
    }
}