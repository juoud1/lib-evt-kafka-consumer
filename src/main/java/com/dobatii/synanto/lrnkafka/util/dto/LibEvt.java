package com.dobatii.synanto.lrnkafka.util.dto;

import java.math.BigInteger;

import com.dobatii.synanto.lrnkafka.util.enums.LibEvtType;

/**
 * Library event polled DTO
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */
public record LibEvt(BigInteger libEvtId, LibEvtType libEvtType, Book book) {

}
