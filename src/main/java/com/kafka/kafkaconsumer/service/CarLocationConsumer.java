package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.CarLocation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CarLocationConsumer {

    private ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "t_carlocation", groupId = "cg_all_carlocation")
    public void consumeAllCarLocation(String message) throws JsonProcessingException {
        CarLocation carLocation = mapper.readValue(message, CarLocation.class);
        log.info("Consumed consumeAllCarLocation :{}", carLocation);

    }
// if we want to filter records that has to be read by seprate consumers that only read filter record then
// we have to approach eirther we have to use condition in below code  or we have to configure
// the condition in seprate containerfactory(ConcurrentkafkaListenerCntainerFactory) which will work only for that configure consumer
    //wher we have configured our custom filtered container factory otherwise for unfilter  wala will read message from default container
    //factory by kafkaconsumer side.
    // for configurting container factory we have to override bean in kafka config class.

    @KafkaListener(topics = "t_carlocation", groupId = "cg_far_carlocation",containerFactory ="farLocationContainerFactory")
    public void consume100KmMoreCarLocation(String message) throws JsonProcessingException {
        CarLocation carLocation = mapper.readValue(message, CarLocation.class);
        log.info("Consumed consume100KmMoreCarLocation :{}", carLocation);

    }
}
