package it.minetti.kafkaexample.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class VeryImportantService {

    public void doSomething(String message) {
        log.info("Transforming ... {}", StringUtils.upperCase(message));
    }
}
