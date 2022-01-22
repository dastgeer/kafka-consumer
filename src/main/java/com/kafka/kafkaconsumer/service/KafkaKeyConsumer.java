package com.kafka.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
//@Service
public class KafkaKeyConsumer {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @KafkaListener(topics = "multipartition",concurrency = "3")
    public void message(ConsumerRecord<String,String> record) throws InterruptedException {
        log.info("key : {} , value :{} , partitions : {}",record.key(),record.value(),record.partition());
        Thread.sleep(1000);

    }
}
