package com.kafka.kafkaconsumer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.CarLocation;
import com.kafka.kafkaconsumer.errorhandler.kafkaGlobalErrorhandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

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

    //this is container factory only for specific to implement the retry logic, and it will retry specific configured no of times
    //before throw the exception to handler or logger
    @Bean(value = "imageRetryContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object,Object> imageRetryContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer){
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(containerFactory,consumerFactory());
        //this setter is used for error handler
        containerFactory.setErrorHandler(new kafkaGlobalErrorhandler());
        //this setter is used for retry operation
        containerFactory.setRetryTemplate(createRetryTemplate());
        return  containerFactory;
    }

    //this is the actual implementation for retry mechanism in spring & same also will be used in kafka conatiner to implement retry
    //mecahnism.
    private RetryTemplate createRetryTemplate(){
        //it is the template which is hold the retry ploic and retry backoff policy as well and mapping into it.
        RetryTemplate retryTemplate = new RetryTemplate();
        // this is policy which will configure how many try has to take before throw exception
        RetryPolicy retryPolicy = new SimpleRetryPolicy(3);
        retryTemplate.setRetryPolicy(retryPolicy);
        //how much interval means time it has to wait before again retry for next attempt ,it is kind of gap beetween calls.
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

//this is for when  there is no issue resolved after specific amount of retry to process record might be api down or some permanent
    //technical issue,or non-functional issue, and it has to be processed in different manner by saving this record to some other topic
    // that process is called deadLetterPublishingRecoverer, and that record is called dead letter record .
    // excample some  invoice processed got failed and keep on failing after specific retry then it will be process after saving this
    //record to some other topic and some other consumer consume this dead letter new topic and might send highlight notifications etc.
//we must sure this new topic where dead message will go should have same no of partitions or more than that as from where this record has consumer
    @Bean(value = "invoiceDltcontainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object,Object> invoiceDltContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, KafkaTemplate<Object,Object> kafkaTemplate){

        ConcurrentKafkaListenerContainerFactory<Object,Object> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(containerFactory,consumerFactory());
        //here in this conatinerfactory we have another method paramanetre kafkatemplate whihc will be used for send dead record to
        //another topic . this dead record must have to go to same partition of topic .

        DeadLetterPublishingRecoverer invoice_recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                                        (record, execption) -> new TopicPartition("t_invoice_dlt", record.partition()));

        //if want to set acks acknowledge on conatiner properties if any record got failed then just it is not processed yet so setacks(false)
        //Set to false to tell the container to NOT commit the offset for a recovered record.
          //    ackAfterHandle false to suppress committing the offset.
        //containerFactory.getContainerProperties().setackAfterHandle(false)
        FixedBackOff backOff = new FixedBackOff();
        backOff.setMaxAttempts(4);
        SeekToCurrentErrorHandler erroHandler = new SeekToCurrentErrorHandler(invoice_recoverer,backOff);/// this class is used to first retry this much of condiufured time
       // if not then succeeded then dead record send to configured dead topic to same partition

        containerFactory.setErrorHandler(erroHandler);
        return containerFactory;
    }
}
