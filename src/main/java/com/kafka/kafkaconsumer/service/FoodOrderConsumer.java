package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.FoodOrder;
import lombok.extern.slf4j.Slf4j;
import lombok.extern.slf4j.XSlf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FoodOrderConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();
    private static final int MAX_ORDER_QTY= 6;

    @KafkaListener(topics = "t_foodorder",errorHandler = "foodConsumerErrorHandler")
    public void consumeOrder(String order) throws JsonProcessingException {
        FoodOrder foodOrder = objectMapper.readValue(order, FoodOrder.class);
        if(foodOrder.getOrderQty()>MAX_ORDER_QTY){
            throw new IllegalArgumentException("food qty should not be more than 6");
            //in this it will throw exception but there is no proper handler by default spring printing error which is weird kind of log
            // to handle peorply we have use kafka error handler interface called ConsumerListenerErrorhandler have handlerMethod() with use of that
            // either we can log error in elastic serach or we can save into db to retry oeprtation. but this only for specific to this consumer
            //because we have metion kafkalister attribut errorhandler which class going to handle.
            // if we dont want to tobe hanlde error with specific error handler and want to be handle with global eror handler remove attribute
           // from kafkalistener
        }
        log.info("food order is consumed properly:{}",foodOrder);
    }
}
