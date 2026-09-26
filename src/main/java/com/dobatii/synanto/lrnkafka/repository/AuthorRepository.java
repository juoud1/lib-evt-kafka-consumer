package com.dobatii.synanto.lrnkafka.repository;

import java.math.BigInteger;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.dobatii.synanto.lrnkafka.data.entity.AuthorEntity;

/**
 * Author DAO mechanism for connecting to DB, mapping table/entity,
 *  saving, finding, listing, updating or deleting AuthorEntity
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

public interface AuthorRepository extends JpaRepository<AuthorEntity, BigInteger> {
	public List<AuthorEntity> findByAuthorName (String authorName);
}
