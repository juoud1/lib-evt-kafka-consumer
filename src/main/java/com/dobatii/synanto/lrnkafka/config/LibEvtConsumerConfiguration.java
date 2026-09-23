package com.dobatii.synanto.lrnkafka.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Lib Event component responsible for configuring evry kafka bean necessary 
 *     for consuming the events from kafka topic
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

@Configuration
@EnableKafka
public class LibEvtConsumerConfiguration {

}
