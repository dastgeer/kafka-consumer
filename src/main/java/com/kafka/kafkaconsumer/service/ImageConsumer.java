package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.Image;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;

import javax.xml.ws.http.HTTPException;

@Service
@Slf4j
public class ImageConsumer {
    private ObjectMapper mapper = new ObjectMapper();

    //thisimplementation to simulate the retry mechanism
    @KafkaListener(topics = "t_image",containerFactory = "imageRetryContainerFactory")
    public void consumeImage(String message) throws JsonProcessingException {
        Image image = mapper.readValue(message, Image.class);
        //if there is any httpconnection or third party communication erro happened then we have to make how many time
        //it has to be retry before throwing exception either through custom error handler or global error hanler otherwise spring by default
        //log exception
        //to use retry mechanism we have to use spring ka SpringRetry template which is one of kind of library in spring for retyr
        //it can be use in kafka as well
        //if we want to use specific consumer consumercontainerfactory to implement then have to configure kafkalistener
        if(image.getType().equalsIgnoreCase("svg")){
            throw new HttpServerErrorException(HttpStatus.BAD_GATEWAY,"simulation for connection Timeout exception just to retry");
        }
        log.info("image processing consumer:{}",image);
    }
}
