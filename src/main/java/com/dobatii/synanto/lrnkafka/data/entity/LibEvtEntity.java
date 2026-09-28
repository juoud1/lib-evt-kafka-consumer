package com.dobatii.synanto.lrnkafka.data.entity;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Objects;

import org.springframework.data.jpa.domain.AbstractPersistable;

import com.dobatii.synanto.lrnkafka.util.enums.LibEvtType;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Library event entity
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

@Entity
@Table(name = "lib_evt")
public class LibEvtEntity extends AbstractPersistable<BigInteger> {	
	
//	@Id @GeneratedValue 
//	private BigInteger id;
	
	@Enumerated(EnumType.STRING)
	private LibEvtType libEvtType;
	
//	@ToString.Exclude
	@OneToOne(cascade = {CascadeType.ALL})
	// AU LIEU DE @JoinColumn(name = "lib_evt_id", referencedColumnName = "id")
	// ON UTILISE @JoinTable voir https://www.baeldung.com/jpa-one-to-one
	@JoinTable(name = "libevt_book", 
		joinColumns = {@JoinColumn(name = "lib_evt_id", referencedColumnName = "id")},
		inverseJoinColumns = {@JoinColumn(name="book_id", referencedColumnName = "id")})
	private BookEntity book;
	
	private String libEvtCreatedBy;
	private LocalDateTime libEvtCreatedDatetime;
	private String libEvtLastModifiedBy;
	private LocalDateTime libEvtLastModifiedDatetime;
	
	public LibEvtEntity() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public LibEvtEntity(LibEvtType libEvtType, BookEntity book, String libEvtCreatedBy,
			LocalDateTime libEvtCreatedDatetime, String libEvtLastModifiedBy,
			LocalDateTime libEvtLastModifiedDatetime) {
		super();
		this.libEvtType = libEvtType;
		this.book = book;
		this.libEvtCreatedBy = libEvtCreatedBy;
		this.libEvtCreatedDatetime = libEvtCreatedDatetime;
		this.libEvtLastModifiedBy = libEvtLastModifiedBy;
		this.libEvtLastModifiedDatetime = libEvtLastModifiedDatetime;
	}

	public LibEvtType getLibEvtType() {
		return libEvtType;
	}

	public void setLibEvtType(LibEvtType libEvtType) {
		this.libEvtType = libEvtType;
	}

	public BookEntity getBook() {
		return book;
	}

	public void setBook(BookEntity book) {
		this.book = book;
	}

	public String getLibEvtCreatedBy() {
		return libEvtCreatedBy;
	}

	public void setLibEvtCreatedBy(String libEvtCreatedBy) {
		this.libEvtCreatedBy = libEvtCreatedBy;
	}

	public LocalDateTime getLibEvtCreatedDatetime() {
		return libEvtCreatedDatetime;
	}

	public void setLibEvtCreatedDatetime(LocalDateTime libEvtCreatedDatetime) {
		this.libEvtCreatedDatetime = libEvtCreatedDatetime;
	}

	public String getLibEvtLastModifiedBy() {
		return libEvtLastModifiedBy;
	}

	public void setLibEvtLastModifiedBy(String libEvtLastModifiedBy) {
		this.libEvtLastModifiedBy = libEvtLastModifiedBy;
	}

	public LocalDateTime getLibEvtLastModifiedDatetime() {
		return libEvtLastModifiedDatetime;
	}

	public void setLibEvtLastModifiedDatetime(LocalDateTime libEvtLastModifiedDatetime) {
		this.libEvtLastModifiedDatetime = libEvtLastModifiedDatetime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(book, libEvtCreatedBy, libEvtCreatedDatetime, libEvtLastModifiedBy,
				libEvtLastModifiedDatetime, libEvtType);
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
		LibEvtEntity other = (LibEvtEntity) obj;
		return Objects.equals(book, other.book) && Objects.equals(libEvtCreatedBy, other.libEvtCreatedBy)
				&& Objects.equals(libEvtCreatedDatetime, other.libEvtCreatedDatetime)
				&& Objects.equals(libEvtLastModifiedBy, other.libEvtLastModifiedBy)
				&& Objects.equals(libEvtLastModifiedDatetime, other.libEvtLastModifiedDatetime)
				&& libEvtType == other.libEvtType;
	}

	@Override
	public String toString() {
		return "LibEvtEntity [libEvtType=" + libEvtType + ", libEvtCreatedBy=" + libEvtCreatedBy
				+ ", libEvtCreatedDatetime=" + libEvtCreatedDatetime + ", libEvtLastModifiedBy=" + libEvtLastModifiedBy
				+ ", libEvtLastModifiedDatetime=" + libEvtLastModifiedDatetime + "]";
	}
	
}
