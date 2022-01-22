package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.Employee;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
//@Service
public class EmployeeConsumer {

    private ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "t_employee")
    public void consumeMsg(String msg) throws JsonProcessingException {
        Employee employeeObj = mapper.readValue(msg, Employee.class);
        log.info("employee ->{}",employeeObj);
    }
}
