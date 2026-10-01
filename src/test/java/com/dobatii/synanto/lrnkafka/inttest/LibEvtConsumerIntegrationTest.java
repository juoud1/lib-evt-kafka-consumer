package com.dobatii.synanto.lrnkafka.inttest;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doCallRealMethod;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.dobatii.synanto.lrnkafka.consumer.LibEvtConsumer;
import com.dobatii.synanto.lrnkafka.repository.LibEvtRepository;
import com.dobatii.synanto.lrnkafka.service.LibEvtConsumerService;
import com.dobatii.synanto.lrnkafka.util.dto.Author;
import com.dobatii.synanto.lrnkafka.util.dto.Book;
import com.dobatii.synanto.lrnkafka.util.dto.LibEvt;
import com.dobatii.synanto.lrnkafka.util.enums.LibEvtType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@AutoConfigureTestRestTemplate
@EmbeddedKafka(topics= {"lib-evt-topic"},partitions = 1, controlledShutdown = true)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
									"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext
public class LibEvtConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;
	
	@Autowired
	LibEvtRepository libEvtRepository;
	
	@MockitoSpyBean
	LibEvtConsumerService consumerService;
	
	@MockitoSpyBean
	LibEvtConsumer libEvtConsumer;

	private ConsumerRecord<Integer, String> libEvtConsumerRecord;
	
	@BeforeEach
	void setUp() {
		
		for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
		
		IO.println("BrokersAsString : " + embeddedKafkaBroker.getBrokersAsString());
		IO.println("getPartitionsPerTopic : " + embeddedKafkaBroker.getPartitionsPerTopic());
		IO.println("Topics : " + embeddedKafkaBroker.getTopics());
	}
	
	@AfterEach
	void tearDown() {
		libEvtRepository.deleteAll();
	}
	
	@Test
	void publishNewLibEvt() throws InterruptedException, ExecutionException {
		
		IO.println("BrokersAsString : " + embeddedKafkaBroker.getBrokersAsString());
		IO.println("getPartitionsPerTopic : " + embeddedKafkaBroker.getPartitionsPerTopic());
		IO.println("Topics : " + embeddedKafkaBroker.getTopics());
		
		//GIVEN
		String jsonInput = "{\"libEvtId\":null,\"libEvtType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Abby ti Dongongo\",\"bookAuthor\":{\"authorId\":3, \"authorName\":\"Dobtiia\"}, \"pubDate\":\"2026-01-07\"}}";
		kafkaTemplate.sendDefault(jsonInput).get();
		libEvtConsumerRecord = new ConsumerRecord<>("lib-evt-topic", 0, 150, 2020, jsonInput); //mock(ConsumerRecord.class);
		
		var author = new Author(BigInteger.ONE, "Dobatia");
		var book = new Book(BigInteger.TWO, "Abby ti Dongongo", author, LocalDate.of(2020, 1, 10));	
		var libevt = new LibEvt(BigInteger.TEN, LibEvtType.NEW, book);
		
		
		//WHEN
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(4, TimeUnit.SECONDS);
		
		doCallRealMethod().when(libEvtConsumer).listenMessage(isA(ConsumerRecord.class));
		libEvtConsumer.listenMessage(libEvtConsumerRecord);
		
		doCallRealMethod().when(consumerService).processConsumerLibEvt(isA(LibEvt.class));
		consumerService.processConsumerLibEvt(libevt);
		
		//THEN 
		// verify ne fonctionne pas ?
		// voir alternative https://www.baeldung.com/mockito-void-methods
//		verify(libEvtConsumer, timeout(1)).listenMessage(libEvtConsumerRecord);
//		verify(consumerService, timeout(1)).processConsumerLibEvt(libevt);
																				
		var libEvts = libEvtRepository.findAll();
		assertEquals(1, libEvts.size());
		libEvts.forEach(levt -> {
			IO.println("levt dans testIT " + levt);
			assertNotNull(levt.getId());
			assertEquals(LibEvtType.NEW, levt.getLibEvtType());
		});
	}
	
	@Disabled
	@Test
	void testProcessConsumerLibEvt() {
		fail("Not implement.");
	}
	
	
}
