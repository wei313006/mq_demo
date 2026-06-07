package com.consumer;

import com.consumer.producer.MessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class ConsumerApplicationTests {

    @Resource
    private MessageProducer messageProducer;

    @Test
    void contextLoads() {
        messageProducer.sendMessage("123445");
    }

}
