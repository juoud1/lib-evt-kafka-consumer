package com.dobatii.synanto.lrnkafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.dobatii.synanto.lrnkafka.service.LibEvtConsumerService;
import com.dobatii.synanto.lrnkafka.util.dto.LibEvt;
import com.dobatii.synanto.lrnkafka.util.enums.LibEvtType;

import tools.jackson.databind.ObjectMapper;

class LibEvtConsumerTest {
	private ObjectMapper objectMapper;
	private LibEvtConsumerService service;

	@BeforeEach
	void setUp() {
		objectMapper = mock(ObjectMapper.class);
		service = mock(LibEvtConsumerService.class);
	}

	@Test
	void listenMessageDeserializesAndProcessesTheEvent() {
		LibEvtConsumer consumer = new LibEvtConsumer(objectMapper, service);
		LibEvt expectedEvent = new LibEvt(BigInteger.ONE, LibEvtType.NEW, null);
		ConsumerRecord<Integer, String> record = new ConsumerRecord<>("lib-evts-topic", 0, 0L, 1, "{}");

		when(objectMapper.readValue(anyString(), eq(LibEvt.class))).thenReturn(expectedEvent);

		consumer.listenMessage(record);

		verify(objectMapper).readValue("{}", LibEvt.class);
		ArgumentCaptor<LibEvt> eventCaptor = ArgumentCaptor.forClass(LibEvt.class);
		verify(service).processConsumerLibEvt(eventCaptor.capture());
		assertEquals(expectedEvent, eventCaptor.getValue());
	}

	@Test
	void listenMessageDoesNotProcessEventWhenDeserializationFails() {
		LibEvtConsumer consumer = new LibEvtConsumer(objectMapper, service);
		ConsumerRecord<Integer, String> record = new ConsumerRecord<>("lib-evts-topic", 0, 0L, 1, "invalid");
		IllegalArgumentException deserializationError = new IllegalArgumentException("Invalid event");

		when(objectMapper.readValue(anyString(), eq(LibEvt.class))).thenThrow(deserializationError);

		IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
				() -> consumer.listenMessage(record));

		assertEquals(deserializationError, thrown);
		verifyNoInteractions(service);
	}
}