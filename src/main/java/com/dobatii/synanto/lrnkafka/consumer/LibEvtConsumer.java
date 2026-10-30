package com.dobatii.synanto.lrnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.dobatii.synanto.lrnkafka.service.LibEvtConsumerService;
import com.dobatii.synanto.lrnkafka.util.dto.LibEvt;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

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
		
		IO.println("ConsumerRecord polled = " + consumerRecord);
		processLibEvtRecord(consumerRecord);
		
	}
	
	/**
	 * Deserializes the record value into a library event and passes it to the service.
	 *
	 * @param consumerRecord Kafka record containing the serialized library event
	 */
	private void processLibEvtRecord(ConsumerRecord<Integer, String> consumerRecord) {
		IO.println("Le traitement de polled Consumer record " + consumerRecord + " \n est encours ...");
		
		// validation et traitement ....
		LibEvt polledConsumerRecordValue = objectMapper.readValue(consumerRecord.value(), LibEvt.class);
		IO.println("La valeur de polled Consumer record = " + polledConsumerRecordValue);
		
		libEvtConsumerService.processConsumerLibEvt(polledConsumerRecordValue);
		
		IO.println("Le polled Consumer record " + consumerRecord + " \n traité avec succès");
	}
	
}
