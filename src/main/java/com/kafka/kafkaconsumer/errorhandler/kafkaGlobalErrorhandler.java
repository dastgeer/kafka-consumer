package com.kafka.kafkaconsumer.errorhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.stereotype.Service;

@Slf4j
@Service(value = "kafkaGlobalErrorhandler")
public class kafkaGlobalErrorhandler implements ConsumerAwareErrorHandler {
    //this interface is used to handle all kafka error handler as global for all consumers
    // but by default in kafkaListerConatinerFactory some other error handler is register to make use to handle
    //for all consumers to hanlder error as globally we must have to register this bean by seeting propety in setErrohandler(new KafkaGlobalErrorhannler())
    // but in kafka configuration override default kafkaListenercontainer factory/
//the flow for this global handler any exception which will be not hanled specifically in this consumer
    //will be come to global error hanlder if any error scenario handle to specicfic erorr handler then it will go that error hanler
    // if that specific aerror hanlder also throw exception literally then it will come to global error hanlder.
    @Override
    public void handle(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer) {
        log.warn("gloabl error hanlder data :{} , from which consumer :{}",data.value().toString());
    }
}
