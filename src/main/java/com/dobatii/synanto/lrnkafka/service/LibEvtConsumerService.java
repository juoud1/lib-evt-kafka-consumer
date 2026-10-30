package com.dobatii.synanto.lrnkafka.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

/**
 * LibEvt consumer for validating and traiting consumer record
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

@Service
public class LibEvtConsumerService {
	private static final Logger LOGGER = LoggerFactory.getLogger(LibEvtConsumerService.class);
	
	private final LibEvtRepository libEvtRepository;
	private final BookRepository bookRepository;
	private final AuthorRepository authorRepository;
	
	/**
	 * Creates the service with the repositories used to persist library events,
	 * books, and authors.
	 *
	 * @param libEvtRepository repository for library events
	 * @param bookRepository repository for books
	 * @param authorRepository repository for authors
	 */
	public LibEvtConsumerService(LibEvtRepository libEvtRepository,
			BookRepository bookRepository,
			AuthorRepository authorRepository) {
		this.authorRepository = authorRepository;
		this.bookRepository = bookRepository;
		this.libEvtRepository = libEvtRepository;
		
	}
	
	/**
	 * Validates an event and routes it according to its type.
	 *
	 * @param libEvtConsumer event to process
	 * @throws IllegalArgumentException if the event is null or has an unsupported type
	 */
	public void processConsumerLibEvt(LibEvt libEvtConsumer) {
		LOGGER.info("Le traitement de Consumer Library Event {} est en cours ...", libEvtConsumer);
		
		// validation et traitement ....
		if (Objects.isNull(libEvtConsumer)) {
			LOGGER.error("ERREUR : les données de library event sont vides.");
			throw new IllegalArgumentException("Aucune donnée à traiter ni à persister.");
		}
		
		switch (libEvtConsumer.libEvtType()) {
			case LibEvtType.NEW -> saveLibEvent(libEvtConsumer);
			case LibEvtType.UPDATE -> updateLibEvent(libEvtConsumer);
//		case LibEvtTyp
			default -> throw new IllegalArgumentException("Unexpected value: " + libEvtConsumer.libEvtType());
		
		}
		
		LOGGER.info("L'événement {} traité avec succès", libEvtConsumer);
	}
	
	/**
	 * Creates and persists a new author, book, and library event, then links the
	 * persisted entities.
	 *
	 * @param libEvt event containing the book and author to save
	 */
	private void saveLibEvent (LibEvt libEvt) {
		
		LOGGER.info("L'enregistrement du nouvel Consumer Library Event {} est en cours ...", libEvt);
		
		// validation et traitement ....
		var bookEntity = dtoToBookEntity(libEvt);
		var authorEntity = dtoToAuthorEntity(libEvt);
		var libEvtEntity = dtoToLibEvtEntity(libEvt);
		
		var authorSaved = authorRepository.saveAndFlush(authorEntity);
		
		bookEntity.setBookAuthor(authorSaved);
		var bookSaved = bookRepository.saveAndFlush(bookEntity);
		
//		bookEntity.setLibEvt(libEvtSaved);
		
		libEvtEntity.setBook(bookSaved);
		var libEvtSaved = libEvtRepository.saveAndFlush(libEvtEntity);
		bookSaved.setLibEvt(libEvtSaved);
		
		bookSaved = bookRepository.saveAndFlush(bookSaved);
		
		LOGGER.info("Auteur du livre enregistré : {}", authorSaved);
		LOGGER.info("Livre enregistré pour l'événement : {}", bookSaved);
		LOGGER.info("Nouvel événement enregistré avec succès : {}", libEvtSaved);
	}
	
	/**
	 * Handles an update event. The current implementation logs the event without
	 * modifying persisted entities.
	 *
	 * @param libEvt event to handle
	 */
	private void updateLibEvent (LibEvt libEvt) {
		
		LOGGER.info("Traitement de la mise à jour de Consumer Library Event {} en cours ...", libEvt);
		
		// validation et traitement ....
		
		LOGGER.info("Mise à jour de l'événement traitée : {}", libEvt);
	}
	
	/**
	 * Builds an author entity from the author included in a library event.
	 *
	 * @param libEvt event containing the author
	 * @return the corresponding author entity
	 */
	private AuthorEntity dtoToAuthorEntity(LibEvt libEvt) {
		var books = bookRepository.findAll().stream()
						.filter(b -> Objects.nonNull(b.getBookAuthor()) && b.getBookAuthor().getAuthorName()
								.equalsIgnoreCase(libEvt.book().bookAuthor().authorName()))
						.toList();
		
		Function<Author, AuthorEntity> f = a -> new AuthorEntity(a.authorName(), "admin_synanto", LocalDateTime.now(), 
																	"admin_synanto", LocalDateTime.now(), null);
		return f.apply(libEvt.book().bookAuthor());
	}
	
	/**
	 * Builds a book entity from the book included in a library event.
	 *
	 * @param libEvt event containing the book
	 * @return the corresponding book entity
	 */
	private BookEntity dtoToBookEntity(LibEvt libEvt) {
		var bookAuth = authorRepository.findByAuthorName(libEvt.book()
								.bookAuthor()
								.authorName());
		LOGGER.debug("Auteurs trouvés pour le livre : {}", bookAuth);
		
		Function<Book, BookEntity> f = b -> new BookEntity(b.bookName(), b.pubDate(), 
				BookStatus.NEW, BookAvailabilityStatus.AVAILABLE, null,
				null,
				"admin_synanto", LocalDateTime.now(), 
				"admin_synanto", LocalDateTime.now());

		return f.apply(libEvt.book()); 
	}
	
	/**
	 * Builds a library event entity from the supplied event.
	 *
	 * @param libEvt event to map
	 * @return the corresponding library event entity
	 */
	private LibEvtEntity dtoToLibEvtEntity(LibEvt libEvt) {
		//var book = bookRepository.findByBookName(libEvt.book().bookName()).orElseGet(BookEntity::new);
		
		Function<LibEvt, LibEvtEntity> f = e -> new LibEvtEntity(libEvt.libEvtType(), null, "admin_synanto", LocalDateTime.now(), "admin_synanto", LocalDateTime.now());

		return f.apply(libEvt);
	}
}
