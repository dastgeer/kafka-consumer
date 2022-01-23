package com.kafka.kafkaconsumer.errorhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Slf4j
@Service(value = "foodConsumerErrorHandler")
public class FoodOrderErrorHnadler implements ConsumerAwareListenerErrorHandler {
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
//from here we can log error object json & messages in elastic search & we can also save into db if we want as our choice
        //this is specific to one consumer to handle error.
        log.warn("error in food order consumer, pretending to log the error into elastic search message: {},because of issue: {}"
        ,message.getPayload(),exception.getMessage());
        //and wont throw exception because we didnt return any thing  and level of exception is first custom excpetion--> if throw exception
        // from custom herrorhandler then it will go to Global error handler if globalerrohandler written then it will handle propely
        // otherwise spring global error handler will manage.

        if(exception.getCause() instanceof RuntimeException){
            throw exception;
        }
        return null;
    }
}
