package com.kafka.kafkaconsumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

@Configuration
public class KafkaConfig {
    // this is property which is used for overriding kafka properties in consumer & producer side
    //it will by default use and override proprties from application.proprties/ application .yml but it is used for manually configuration also
    @Autowired
    private KafkaProperties kafkaProperties;
    //rememebr here in consumer side producer factory of type Object, Object
    //one more point there will be no override of kafkatemple in consumer side because it will taken care by kafkalistener, and their
    // kafkamessageListenerConstainer, KafkaListenerContainerFactory and all.cosumer coordinater will be take care for reaancing the partition
    //partitionassignor take care for this.
    @Bean
    public ConsumerFactory<Object,Object> consumerFactory(){
        Map<String, Object> consumerPropertiesMap = kafkaProperties.buildConsumerProperties();
        consumerPropertiesMap.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG,"120000");
        return new DefaultKafkaConsumerFactory<>(consumerPropertiesMap);
    }

}
