package com.kafka.kafkaconsumer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.CarLocation;
import com.kafka.kafkaconsumer.errorhandler.kafkaGlobalErrorhandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

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


    //this is container factory for specific consumer not globally means changes will not apply to all consumers
    //bean name should be same as what we have configured in consumer conatianerfactory attribute, method name also that will be very nice
    //ConcurrentKafkaListenerContainerFactory is class taken care for creating multiple kafkaLMessageListenerConatiner which will
    // listen messgae for each consumer instance in same consumer group,
    //ConcurrentKafkaListenerContainerFactoryConfigurer is used to configure concurrentkafkalistenerContainerfatory with consumerfactory
    @Bean(name = "farLocationContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object,Object> farLocationContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer){
        ConcurrentKafkaListenerContainerFactory<Object,Object> customContainerFactory = new ConcurrentKafkaListenerContainerFactory<Object,Object>();
        configurer.configure(customContainerFactory,consumerFactory());
        //this method is actully to filter the record before sending to actal kafka listener because messages is passes to
        // kafka consumer is through kafkamessagelistenercontainer and these container is created from  concurrentkafkaListerContainerFactory
        customContainerFactory.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>() {
        //this is actual filter code
            ObjectMapper objectmapper = new ObjectMapper();
            /**
             * Return true if the record should be discarded.
             *
             * @param consumerRecord the record.
             * @return true to discard.
             */
            @Override
            public boolean filter(ConsumerRecord<Object, Object> consumerRecord) {
                CarLocation carLocationObj = null;
                try {
                    carLocationObj = objectmapper.readValue(consumerRecord.value().toString(), CarLocation.class);
                    if(carLocationObj!=null && carLocationObj.getDistance()<=100){
                        return true;
                    }
                    return false;
                } catch (JsonProcessingException e) {
                    return false;
                }
            }
        });
    return customContainerFactory;
    }


    //this is kafkaListenerCOntainerFactory is by default implemantion bean for all consumer in kafka
    //whatever property we will set in this bean will be reflected as globally to all consumer
    //so we have setted the global error handler which wil be refcted the globally consumer to hanlde error
    // it will override the default implementation.
    @Bean(value = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object,Object> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer){
        ConcurrentKafkaListenerContainerFactory<Object,Object> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(containerFactory,consumerFactory());
        containerFactory.setErrorHandler(new kafkaGlobalErrorhandler());
        return containerFactory;
    }
}
