package com.kafka.kafkaconsumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

//@Service
public class CustomKafkaConsumer {

    //we can configure group id here whihc is different group id at method level which specify it is having different consumer from different group
    // or we can configure one consumer group id with across application using application.yml
    @KafkaListener(topics = "helloTopic")
    public void recieveMessage(String message){
        System.out.println("got meessage from kafka.."+message);
    }
}
