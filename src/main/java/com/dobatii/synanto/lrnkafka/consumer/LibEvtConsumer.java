package com.dobatii.synanto.lrnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Lib Event component for consuming the events from kafka topic
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

@Component
@Slf4j
public class LibEvtConsumer {
	
	/*
	 * Poll message from topic(s) and provide ConsumerRecord object
	 */
	@KafkaListener(topics = {"lib-evts-topic"})
	public void listenMessage(ConsumerRecord<Integer, String> consumerRecord) {
		
		IO.println("ConsumerRecord polled = " + consumerRecord);
	}
}
