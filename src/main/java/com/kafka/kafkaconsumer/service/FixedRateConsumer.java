package com.kafka.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
//@Service
public class FixedRateConsumer {

    @KafkaListener(topics = "fixedrate" )
    public void recieveMessage(String message){
       log.info("got meessage from kafka..{}",message);
    }

}
