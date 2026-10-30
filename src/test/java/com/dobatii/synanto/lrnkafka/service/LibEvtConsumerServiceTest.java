package com.dobatii.synanto.lrnkafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.dobatii.synanto.lrnkafka.data.entity.AuthorEntity;
import com.dobatii.synanto.lrnkafka.data.entity.BookEntity;
import com.dobatii.synanto.lrnkafka.data.entity.LibEvtEntity;
import com.dobatii.synanto.lrnkafka.repository.AuthorRepository;
import com.dobatii.synanto.lrnkafka.repository.BookRepository;
import com.dobatii.synanto.lrnkafka.repository.LibEvtRepository;
import com.dobatii.synanto.lrnkafka.util.dto.Author;
import com.dobatii.synanto.lrnkafka.util.dto.Book;
import com.dobatii.synanto.lrnkafka.util.dto.LibEvt;
import com.dobatii.synanto.lrnkafka.util.enums.BookAvailabilityStatus;
import com.dobatii.synanto.lrnkafka.util.enums.BookStatus;
import com.dobatii.synanto.lrnkafka.util.enums.LibEvtType;

class LibEvtConsumerServiceTest {
	private LibEvtRepository libEvtRepository;
	private BookRepository bookRepository;
	private AuthorRepository authorRepository;
	private LibEvtConsumerService service;

	@BeforeEach
	void setUp() {
		libEvtRepository = mock(LibEvtRepository.class);
		bookRepository = mock(BookRepository.class);
		authorRepository = mock(AuthorRepository.class);
		service = new LibEvtConsumerService(libEvtRepository, bookRepository, authorRepository);
	}

	@Test
	void processConsumerLibEvtSavesNewEventWithLinkedEntities() {
		Author author = new Author(BigInteger.TEN, "Ada Lovelace");
		Book book = new Book(BigInteger.ONE, "Analytical Engine", author, LocalDate.of(1843, 1, 1));
		LibEvt event = new LibEvt(BigInteger.valueOf(2), LibEvtType.NEW, book);
		when(authorRepository.findByAuthorName(author.authorName())).thenReturn(List.of());
		when(bookRepository.findAll()).thenReturn(List.of());
		when(authorRepository.saveAndFlush(any(AuthorEntity.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));
		when(bookRepository.saveAndFlush(any(BookEntity.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));
		when(libEvtRepository.saveAndFlush(any(LibEvtEntity.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));

		service.processConsumerLibEvt(event);

		ArgumentCaptor<AuthorEntity> authorCaptor = ArgumentCaptor.forClass(AuthorEntity.class);
		verify(authorRepository).saveAndFlush(authorCaptor.capture());
		assertEquals(author.authorName(), authorCaptor.getValue().getAuthorName());

		ArgumentCaptor<BookEntity> bookCaptor = ArgumentCaptor.forClass(BookEntity.class);
		verify(bookRepository, times(2)).saveAndFlush(bookCaptor.capture());
		BookEntity savedBook = bookCaptor.getAllValues().get(1);
		assertEquals(book.bookName(), savedBook.getBookName());
		assertEquals(BookStatus.NEW, savedBook.getBookStatus());
		assertEquals(BookAvailabilityStatus.AVAILABLE, savedBook.getBookavailabilityStatus());
		assertSame(authorCaptor.getValue(), savedBook.getBookAuthor());

		ArgumentCaptor<LibEvtEntity> eventCaptor = ArgumentCaptor.forClass(LibEvtEntity.class);
		verify(libEvtRepository).saveAndFlush(eventCaptor.capture());
		assertEquals(LibEvtType.NEW, eventCaptor.getValue().getLibEvtType());
		assertSame(savedBook, eventCaptor.getValue().getBook());
		assertNotNull(savedBook.getLibEvt());
		assertSame(eventCaptor.getValue(), savedBook.getLibEvt());
	}

	@Test
	void processConsumerLibEvtAcceptsUpdateWithoutPersisting() {
		service.processConsumerLibEvt(new LibEvt(BigInteger.ONE, LibEvtType.UPDATE, null));

		verifyNoInteractions(libEvtRepository, bookRepository, authorRepository);
	}

	@Test
	void processConsumerLibEvtRejectsNullEvent() {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> service.processConsumerLibEvt(null));

		assertEquals("Aucune donnée à traiter ni à persister.", exception.getMessage());
		verifyNoInteractions(libEvtRepository, bookRepository, authorRepository);
	}

	@Test
	void processConsumerLibEvtRejectsUnsupportedEventType() {
		LibEvt event = new LibEvt(BigInteger.ONE, LibEvtType.OTHER, null);

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> service.processConsumerLibEvt(event));

		assertEquals("Unexpected value: OTHER", exception.getMessage());
		verifyNoInteractions(libEvtRepository, bookRepository, authorRepository);
	}
}