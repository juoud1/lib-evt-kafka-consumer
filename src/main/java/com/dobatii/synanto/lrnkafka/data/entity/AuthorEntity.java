package com.dobatii.synanto.lrnkafka.data.entity;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.data.jpa.domain.AbstractPersistable;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author entity
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

@Entity
@Table(name = "author")
public class AuthorEntity extends AbstractPersistable<BigInteger> {

//	@Id 
//	@GeneratedValue 
//	private BigInteger id; 

	private String authorName;
	private String authorCreatedBy;
	
	private LocalDateTime authorCreatedDatetime;
	private String authorLastModifiedBy;
	private LocalDateTime authorLastModifiedDatetime;
	
	
	@Transient
	@OneToMany(mappedBy = "bookAuthor")
	private List<BookEntity> books = new ArrayList<>();
	
	public AuthorEntity() {
		super();
		
	}
	
	public AuthorEntity(String authorName, String authorCreatedBy, LocalDateTime authorCreatedDatetime,
			String authorLastModifiedBy, LocalDateTime authorLastModifiedDatetime, List<BookEntity> books) {
		super();
		this.authorName = authorName;
		this.authorCreatedBy = authorCreatedBy;
		this.authorCreatedDatetime = authorCreatedDatetime;
		this.authorLastModifiedBy = authorLastModifiedBy;
		this.authorLastModifiedDatetime = authorLastModifiedDatetime;
		this.books = books;
	}

	public String getAuthorName() {
		return authorName;
	}

	public void setAuthorName(String authorName) {
		this.authorName = authorName;
	}

	public String getAuthorCreatedBy() {
		return authorCreatedBy;
	}

	public void setAuthorCreatedBy(String authorCreatedBy) {
		this.authorCreatedBy = authorCreatedBy;
	}

	public LocalDateTime getAuthorCreatedDatetime() {
		return authorCreatedDatetime;
	}

	public void setAuthorCreatedDatetime(LocalDateTime authorCreatedDatetime) {
		this.authorCreatedDatetime = authorCreatedDatetime;
	}

	public String getAuthorLastModifiedBy() {
		return authorLastModifiedBy;
	}

	public void setAuthorLastModifiedBy(String authorLastModifiedBy) {
		this.authorLastModifiedBy = authorLastModifiedBy;
	}

	public LocalDateTime getAuthorLastModifiedDatetime() {
		return authorLastModifiedDatetime;
	}

	public void setAuthorLastModifiedDatetime(LocalDateTime authorLastModifiedDatetime) {
		this.authorLastModifiedDatetime = authorLastModifiedDatetime;
	}

	public List<BookEntity> getBooks() {
		return books;
	}

	public void setBooks(List<BookEntity> books) {
		this.books = books;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(authorCreatedBy, authorCreatedDatetime, authorLastModifiedBy,
				authorLastModifiedDatetime, authorName);
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
		AuthorEntity other = (AuthorEntity) obj;
		return Objects.equals(authorCreatedBy, other.authorCreatedBy)
				&& Objects.equals(authorCreatedDatetime, other.authorCreatedDatetime)
				&& Objects.equals(authorLastModifiedBy, other.authorLastModifiedBy)
				&& Objects.equals(authorLastModifiedDatetime, other.authorLastModifiedDatetime)
				&& Objects.equals(authorName, other.authorName);
	}

	@Override
	public String toString() {
		return "AuthorEntity [authorName=" + authorName + ", authorCreatedBy=" + authorCreatedBy
				+ ", authorCreatedDatetime=" + authorCreatedDatetime + ", authorLastModifiedBy=" + authorLastModifiedBy
				+ ", authorLastModifiedDatetime=" + authorLastModifiedDatetime + "]";
	}
	
}
