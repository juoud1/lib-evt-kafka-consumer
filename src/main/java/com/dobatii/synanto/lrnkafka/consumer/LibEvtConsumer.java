package com.dobatii.synanto.lrnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dobatii.synanto.lrnkafka.service.LibEvtConsumerService;
import com.dobatii.synanto.lrnkafka.util.dto.LibEvt;

import tools.jackson.databind.ObjectMapper;

/**
 * Lib Event component for consuming the events from kafka topic
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

@Component
public class LibEvtConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(LibEvtConsumer.class);
	
	private final ObjectMapper objectMapper;
	
	private final LibEvtConsumerService libEvtConsumerService;
	
	/**
	 * Creates the Kafka event consumer with its JSON mapper and event-processing service.
	 *
	 * @param objectMapper mapper used to deserialize Kafka message values
	 * @param libEvtConsumerService service that processes deserialized library events
	 */
	public LibEvtConsumer(ObjectMapper objectMapper, LibEvtConsumerService libEvtConsumerService) {
		this.libEvtConsumerService = libEvtConsumerService;
		this.objectMapper = objectMapper;
	}
	
	/**
	 * Receives a Kafka record from the configured library events topic and delegates
	 * its processing.
	 *
	 * @param consumerRecord record polled from Kafka
	 */
	@KafkaListener(topics = {"lib-evts-topic"})
	public void listenMessage(ConsumerRecord<Integer, String> consumerRecord) {
		
		LOGGER.info("ConsumerRecord polled: {}", consumerRecord);
		processLibEvtRecord(consumerRecord);
		
	}
	
	/**
	 * Deserializes the record value into a library event and passes it to the service.
	 *
	 * @param consumerRecord Kafka record containing the serialized library event
	 */
	private void processLibEvtRecord(ConsumerRecord<Integer, String> consumerRecord) {
		LOGGER.info("Traitement du ConsumerRecord {} en cours ...", consumerRecord);
		
		// validation et traitement ....
		LibEvt polledConsumerRecordValue = objectMapper.readValue(consumerRecord.value(), LibEvt.class);
		LOGGER.info("Valeur du ConsumerRecord désérialisée : {}", polledConsumerRecordValue);
		
		libEvtConsumerService.processConsumerLibEvt(polledConsumerRecordValue);
		
		LOGGER.info("ConsumerRecord traité avec succès : {}", consumerRecord);
	}
	
}
