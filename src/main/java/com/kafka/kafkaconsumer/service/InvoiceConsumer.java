package com.kafka.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaconsumer.entity.Invoice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InvoiceConsumer {

    private ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "t_invoice",containerFactory = "invoiceDltcontainerFactory")
    public void consumeInvoice(String message) throws JsonProcessingException {
        Invoice invoice = mapper.readValue(message, Invoice.class);
        if(invoice.getAmount()<1){
            throw new IllegalArgumentException("invoice amount is : "+invoice.getAmount()+" for invoice id:{}"+invoice.getInvoiceId());
        }
        log.info("invoice consumer details :{}",invoice);
    }
}
