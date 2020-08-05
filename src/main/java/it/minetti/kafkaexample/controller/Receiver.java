package it.minetti.kafkaexample.controller;

import it.minetti.kafkaexample.service.VeryImportantService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

@Component
@Slf4j
public class Receiver {

    @Autowired
    public VeryImportantService veryImportantService;

    @KafkaListener(topics = "${kafkatest.topic}")
    public void receive(@Payload String payload, @Header("event_type") String eventType) {
        if (!equalsIgnoreCase(eventType, "VALIDATED")) {
            return;
        }
        log.info("received payload='{}'", payload);
        veryImportantService.doSomething(payload);
    }
}