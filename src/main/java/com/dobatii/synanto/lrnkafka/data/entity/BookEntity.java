package com.dobatii.synanto.lrnkafka.data.entity;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

import org.hibernate.annotations.ManyToAny;
import org.springframework.data.jpa.domain.AbstractPersistable;

import com.dobatii.synanto.lrnkafka.util.enums.BookAvailabilityStatus;
import com.dobatii.synanto.lrnkafka.util.enums.BookStatus;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Book entity
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

@Entity
@Table(name = "book")
public class BookEntity extends AbstractPersistable<BigInteger> {
	
	//	@Id @GeneratedValue 
//	private BigInteger id;
	private String bookName; 
	private LocalDate pubDate;
	
	@Enumerated(EnumType.STRING)
	private BookStatus bookStatus;
	
	@Enumerated(EnumType.STRING)
	private BookAvailabilityStatus bookavailabilityStatus;
	
	@ManyToOne
	private AuthorEntity bookAuthor;
	
	@OneToOne(cascade = {CascadeType.ALL})
	// AU LIEU DE @JoinColumn(name = "lib_evt_id", referencedColumnName = "id")
	// ON UTILISE @JoinTable voir https://www.baeldung.com/jpa-one-to-one
	@JoinTable(name = "libevt_book", 
		joinColumns = {@JoinColumn(name = "book_id", referencedColumnName = "id")},
		inverseJoinColumns = {@JoinColumn(name="lib_evt_id", referencedColumnName = "id")})
	LibEvtEntity libEvt;
	
	private String bookCreatedBy;
	private LocalDateTime bookCreatedDatetime;
	private String bookLastModifiedBy;
	private LocalDateTime bookLastModifiedDatetime;
	
	public BookEntity() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public BookEntity(String bookName, LocalDate pubDate, BookStatus bookStatus,
			BookAvailabilityStatus bookavailabilityStatus, AuthorEntity bookAuthor, LibEvtEntity libEvt,
			String bookCreatedBy, LocalDateTime bookCreatedDatetime, String bookLastModifiedBy,
			LocalDateTime bookLastModifiedDatetime) {
		super();
		this.bookName = bookName;
		this.pubDate = pubDate;
		this.bookStatus = bookStatus;
		this.bookavailabilityStatus = bookavailabilityStatus;
		this.bookAuthor = bookAuthor;
		this.libEvt = libEvt;
		this.bookCreatedBy = bookCreatedBy;
		this.bookCreatedDatetime = bookCreatedDatetime;
		this.bookLastModifiedBy = bookLastModifiedBy;
		this.bookLastModifiedDatetime = bookLastModifiedDatetime;
	}

	public String getBookName() {
		return bookName;
	}

	public void setBookName(String bookName) {
		this.bookName = bookName;
	}

	public LocalDate getPubDate() {
		return pubDate;
	}

	public void setPubDate(LocalDate pubDate) {
		this.pubDate = pubDate;
	}

	public BookStatus getBookStatus() {
		return bookStatus;
	}

	public void setBookStatus(BookStatus bookStatus) {
		this.bookStatus = bookStatus;
	}

	public BookAvailabilityStatus getBookavailabilityStatus() {
		return bookavailabilityStatus;
	}

	public void setBookavailabilityStatus(BookAvailabilityStatus bookavailabilityStatus) {
		this.bookavailabilityStatus = bookavailabilityStatus;
	}

	public AuthorEntity getBookAuthor() {
		return bookAuthor;
	}

	public void setBookAuthor(AuthorEntity bookAuthor) {
		this.bookAuthor = bookAuthor;
	}

	public LibEvtEntity getLibEvt() {
		return libEvt;
	}

	public void setLibEvt(LibEvtEntity libEvt) {
		this.libEvt = libEvt;
	}

	public String getBookCreatedBy() {
		return bookCreatedBy;
	}

	public void setBookCreatedBy(String bookCreatedBy) {
		this.bookCreatedBy = bookCreatedBy;
	}

	public LocalDateTime getBookCreatedDatetime() {
		return bookCreatedDatetime;
	}

	public void setBookCreatedDatetime(LocalDateTime bookCreatedDatetime) {
		this.bookCreatedDatetime = bookCreatedDatetime;
	}

	public String getBookLastModifiedBy() {
		return bookLastModifiedBy;
	}

	public void setBookLastModifiedBy(String bookLastModifiedBy) {
		this.bookLastModifiedBy = bookLastModifiedBy;
	}

	public LocalDateTime getBookLastModifiedDatetime() {
		return bookLastModifiedDatetime;
	}

	public void setBookLastModifiedDatetime(LocalDateTime bookLastModifiedDatetime) {
		this.bookLastModifiedDatetime = bookLastModifiedDatetime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(bookAuthor, bookCreatedBy, bookCreatedDatetime, bookLastModifiedBy,
				bookLastModifiedDatetime, bookName, bookStatus, bookavailabilityStatus, libEvt, pubDate);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		BookEntity other = (BookEntity) obj;
		return Objects.equals(bookAuthor, other.bookAuthor) && Objects.equals(bookCreatedBy, other.bookCreatedBy)
				&& Objects.equals(bookCreatedDatetime, other.bookCreatedDatetime)
				&& Objects.equals(bookLastModifiedBy, other.bookLastModifiedBy)
				&& Objects.equals(bookLastModifiedDatetime, other.bookLastModifiedDatetime)
				&& Objects.equals(bookName, other.bookName) && bookStatus == other.bookStatus
				&& bookavailabilityStatus == other.bookavailabilityStatus && Objects.equals(libEvt, other.libEvt)
				&& Objects.equals(pubDate, other.pubDate);
	}

	@Override
	public String toString() {
		return "BookEntity [bookName=" + bookName + ", pubDate=" + pubDate + ", bookStatus=" + bookStatus
				+ ", bookavailabilityStatus=" + bookavailabilityStatus + ", bookAuthor=" + bookAuthor + ", libEvt="
				+ libEvt + ", bookCreatedBy=" + bookCreatedBy + ", bookCreatedDatetime=" + bookCreatedDatetime
				+ ", bookLastModifiedBy=" + bookLastModifiedBy + ", bookLastModifiedDatetime="
				+ bookLastModifiedDatetime + "]";
	}
	
}
