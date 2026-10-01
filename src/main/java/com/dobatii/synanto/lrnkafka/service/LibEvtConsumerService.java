package com.dobatii.synanto.lrnkafka.service;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

import lombok.extern.slf4j.Slf4j;

/**
 * LibEvt consumer for validating and traiting consumer record
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

@Service
@Transactional
@Slf4j
public class LibEvtConsumerService {
	
	private final LibEvtRepository libEvtRepository;
	private final BookRepository bookRepository;
	private final AuthorRepository authorRepository;
	
	public LibEvtConsumerService(LibEvtRepository libEvtRepository,
			BookRepository bookRepository,
			AuthorRepository authorRepository) {
		this.authorRepository = authorRepository;
		this.bookRepository = bookRepository;
		this.libEvtRepository = libEvtRepository;
		
	}
	
	public void processConsumerLibEvt(LibEvt libEvtConsumer) {
		IO.println("Le traitement de Consumer Library Event " + libEvtConsumer + " \n est encours ...");
		
		// validation et traitement ....
		if (Objects.isNull(libEvtConsumer)) {
			IO.println("ERREUR : les données de library event sont vides.");
			throw new IllegalArgumentException("Aucune donnée à traiter ni à persister.");
		}
		
		switch (libEvtConsumer.libEvtType()) {
			case LibEvtType.NEW -> saveLibEvent(libEvtConsumer);
			case LibEvtType.UPDATE -> updateLibEvent(libEvtConsumer);
//		case LibEvtTyp
			default -> throw new IllegalArgumentException("Unexpected value: " + libEvtConsumer.libEvtType());
		
		}
		
		IO.println("L'événement " + libEvtConsumer + " \n traité avec succès");
	}
	
	private void saveLibEvent_old (LibEvt libEvt) {
		
		IO.println("L'enregistrement du nouvel Consumer Library Event " + libEvt + " \n est encours ...");
		
		// validation et traitement ....
		var libEvtEntityToSave = dtoToLibEvtEntity(libEvt);
		var libEvtSaved = libEvtRepository.saveAndFlush(libEvtEntityToSave);
		
		var bookEntityToSave = dtoToBookEntity(libEvt);
		var authorEntityToSave = dtoToAuthorEntity(libEvt);
		
		AuthorEntity authorSaved = saveAuthor(authorEntityToSave);
		
		// Manually processing one to one relationship using joinTable
		BookEntity bookSaved = null;
		var optBookSaved = saveBook(bookEntityToSave, authorSaved, libEvtSaved);;
		if (optBookSaved.isPresent()) {
			bookSaved = optBookSaved.get();
			libEvtSaved.setBook(bookSaved);
			libEvtRepository.saveAndFlush(libEvtSaved);
			
		} else { // Si le livre existe déjà dans la bdd, inutile de sauvegarder libEventEntity
			libEvtRepository.delete(libEvtEntityToSave);
		}
	
		IO.println("L'auteur du livre enregistré = " + authorSaved + " \n enregistré avec succès");
		IO.println("Le livre de l'événement enregistré = " + bookSaved + " \n enregistré avec succès");
		IO.println("Le nouvel événement " + libEvtSaved + " \n enregistré avec succès");
	}
	
	private void saveLibEvent (LibEvt libEvt) {
		
		IO.println("L'enregistrement du nouvel Consumer Library Event " + libEvt + " \n est encours ...");
		
		// validation et traitement ....
		var authorEntityToSave = dtoToAuthorEntity(libEvt);
		AuthorEntity authorSaved = saveAuthor(authorEntityToSave);
		
		var bookEntityToSave = dtoToBookEntity(libEvt);
		var libEvtEntityToSave = dtoToLibEvtEntity(libEvt);
		
//		var optAuthorSaved = checkExistanceAuthor(authorEntityToSave);
//		AuthorEntity authorSaved = null;
//		if (optAuthorSaved.isEmpty()) {
//			IO.println("Enregistrement du nouvel auteur.");
//			authorSaved = authorRepository.saveAndFlush(authorEntityToSave);
//		} else {
//			IO.println("L'auteur existe déjà");
//			IO.println("Données de l'auteur, récupérées.");
//			authorSaved = optAuthorSaved.get();
//		}
		//var authorSaved = authorRepository.saveAndFlush(authorEntity);
		
		LibEvtEntity libEvtSaved = null;
		
		bookEntityToSave.setBookAuthor(authorSaved);
		var optBookSaved = checkExistanceBook(bookEntityToSave);
		BookEntity bookSaved = null;
		if (optBookSaved.isEmpty()) {
			IO.println("Enregistrement du nouveau livre.");
			bookSaved = bookRepository.saveAndFlush(bookEntityToSave);
			
			libEvtEntityToSave.setBook(bookSaved);
//			var libEvtSaved = libEvtRepository.saveAndFlush(libEvtEntityToSave);
			libEvtSaved = libEvtRepository.saveAndFlush(libEvtEntityToSave);
			bookSaved.setLibEvt(libEvtSaved);
			
			bookSaved = bookRepository.saveAndFlush(bookSaved);
			
			IO.println("L'auteur du livre enregistré = " + authorSaved + " \n enregistré avec succès");
			IO.println("Le livre de l'événement enregistré = " + bookSaved + " \n enregistré avec succès");
			IO.println("Le nouvel événement " + libEvtSaved + " \n enregistré avec succès");
		} 
		/*else {
			IO.println("Le livre existe déjà");
			IO.println("Données du livre, récupérées.");
			bookSaved = optBookSaved.get();
		}*/
		//var bookSaved = bookRepository.saveAndFlush(bookEntityToSave);
		
//		bookEntity.setLibEvt(libEvtSaved);
		
		/*
		libEvtEntityToSave.setBook(bookSaved);
		var libEvtSaved = libEvtRepository.saveAndFlush(libEvtEntityToSave);
		bookSaved.setLibEvt(libEvtSaved);
		
		bookSaved = bookRepository.saveAndFlush(bookSaved);
		*/
		
		IO.println("L'auteur du livre enregistré = " + authorSaved + " \n enregistré avec succès");
		IO.println("Le livre de l'événement existe déjà = " + bookSaved + " \n enregistré avec succès");
		IO.println("Le nouvel événement " + libEvtSaved + " \n enregistré avec succès");
	}
	
	private Optional<BookEntity> saveBook(BookEntity bookEntityToSave, AuthorEntity savedAuthorEntity,
								LibEvtEntity savedLibEvtEntity) {
		
		if (Objects.isNull(bookEntityToSave)){
			IO.println("ERREUR : livre à enregistrer est null ou vide!");
			throw new IllegalArgumentException("Le livre à enregistrer ne doit pas être null ou vide!");
		}
		
		bookEntityToSave.setBookAuthor(savedAuthorEntity);	
		bookEntityToSave.setLibEvt(savedLibEvtEntity);
		var optBookSaved = checkExistanceBook(bookEntityToSave);
		
		BookEntity bookSaved = null;
		if (optBookSaved.isEmpty()) {
			IO.println("Enregistrement du nouveau livre.");
			bookSaved = bookRepository.saveAndFlush(bookEntityToSave);
		} else {
			IO.println("Le livre existe déjà");
			IO.println("Données du livre ignorées.");
			//bookSaved = optBookSaved.get();
		}
		
		return Optional.ofNullable(bookSaved);
	}
	
	private AuthorEntity saveAuthor(AuthorEntity authorEntityToSave) {
		var optAuthorSaved = checkExistanceAuthor(authorEntityToSave);
		
		AuthorEntity authorSaved = null;
		if (optAuthorSaved.isEmpty()) {
			IO.println("Enregistrement du nouvel auteur.");
			authorSaved = authorRepository.saveAndFlush(authorEntityToSave);
		} else {
			IO.println("L'auteur existe déjà");
			IO.println("Données de l'auteur, récupérées.");
			authorSaved = optAuthorSaved.get();
		}
		
		return authorSaved;
	}
	
	

	private Optional<BookEntity> checkExistanceBook(BookEntity bookEntityToSave) {
		IO.println("Vérification de l'existence du nouveau livre à créer est en cours...");
		if (Objects.isNull(bookEntityToSave) || bookEntityToSave.getBookName().isBlank()) {
			IO.println("ERREUR lors de la création du nouveau livre!");
			throw new IllegalArgumentException("L'entité livre ou bien le nom du livre ne doit pas être null!");
		}
		
		IO.println("Vérification de l'existence du nouveau livre avec succès.");
		
		return bookRepository.findByBookName(bookEntityToSave.getBookName());
	}

	
	private Optional<AuthorEntity> checkExistanceAuthor(AuthorEntity authorEntityToSave) {
		IO.println("Vérification de l'existence du nouvel auteur à créer est en cours...");
		if (Objects.isNull(authorEntityToSave) || authorEntityToSave.getAuthorName().isBlank()) {
			IO.println("ERREUR lors de la création du nouvel auteur!");
			throw new IllegalArgumentException("L'entité auteur ou bien le nom de l'auteur ne doit pas être null!");
		}
		
		IO.println("Vérification de l'existence du nouvel auteur avec succès.");
		
		return authorRepository.findByAuthorName(authorEntityToSave.getAuthorName());
	}
	
	private void updateLibEvent (LibEvt libEvt) {
		
		IO.println("L'enregistrement de la mise à jour de Consumer Library Event " + libEvt + " \n est encours ...");
		
		// validation et traitement ....
		
		IO.println("La mise à jour événement " + libEvt + " \n enregistré avec succès");
	}
	
	private AuthorEntity dtoToAuthorEntity(LibEvt libEvt) {
		var books = bookRepository.findAll().stream()
						.filter(b -> Objects.nonNull(b.getBookAuthor()) && b.getBookAuthor().getAuthorName()
								.equalsIgnoreCase(libEvt.book().bookAuthor().authorName()))
						.toList();
		
		Function<Author, AuthorEntity> f = a -> new AuthorEntity(a.authorName(), "admin_synanto", LocalDateTime.now(), 
																	"admin_synanto", LocalDateTime.now(), null);
		return f.apply(libEvt.book().bookAuthor());
	}
	
	private BookEntity dtoToBookEntity(LibEvt libEvt) {
		var bookAuth = authorRepository.findByAuthorName(libEvt.book()
								.bookAuthor()
								.authorName());
		IO.println("Liste de bookAuth = " + bookAuth);
		
		Function<Book, BookEntity> f = b -> new BookEntity(b.bookName(), b.pubDate(), 
				BookStatus.NEW, BookAvailabilityStatus.AVAILABLE, null,
				null,
				"admin_synanto", LocalDateTime.now(), 
				"admin_synanto", LocalDateTime.now());

		return f.apply(libEvt.book()); 
	}
	
	private LibEvtEntity dtoToLibEvtEntity(LibEvt libEvt) {
		//var book = bookRepository.findByBookName(libEvt.book().bookName()).orElseGet(BookEntity::new);
		
		Function<LibEvt, LibEvtEntity> f = e -> new LibEvtEntity(libEvt.libEvtType(), null, "admin_synanto", LocalDateTime.now(), "admin_synanto", LocalDateTime.now());

		return f.apply(libEvt);
	}
}
