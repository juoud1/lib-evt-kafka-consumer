package com.dobatii.synanto.lrnkafka.repository;

import java.math.BigInteger;

import org.springframework.data.jpa.repository.JpaRepository;

import com.dobatii.synanto.lrnkafka.data.entity.LibEvtEntity;

/**
 * LibEvt DAO mechanism for connecting to DB, mapping table/entity,
 *  saving, finding, listing, updating or deleting AuthorEntity
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

public interface LibEvtRepository extends JpaRepository<LibEvtEntity, BigInteger> {
	
}
