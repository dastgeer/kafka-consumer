package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.Commodity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
//@Service
public class CommodityNotificationConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "t_commodity",groupId = "cg_notification")
    public void consumeCommodityNotification(String message) throws JsonProcessingException, InterruptedException {
      Commodity commodity = objectMapper.readValue(message, Commodity.class);
      //want to delay
        Thread.sleep(ThreadLocalRandom.current().nextInt(500,1000));
      log.info("commodity notification message:{}",commodity);
    }
}
