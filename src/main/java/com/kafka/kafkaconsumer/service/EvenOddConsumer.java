package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.EvenOdd;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EvenOddConsumer {

    private ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "t_evenodd")
    public void consumeEvenOdd(String message) throws JsonProcessingException {
        EvenOdd evenOdd = mapper.readValue(message, EvenOdd.class);
        if(evenOdd.getNumber()%2!=0){
            throw new IllegalArgumentException("not even number");
        }
        log.info("consumerEvenOdd :{}",evenOdd);
    }
}
