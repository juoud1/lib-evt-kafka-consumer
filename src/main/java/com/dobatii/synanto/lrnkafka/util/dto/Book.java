package com.dobatii.synanto.lrnkafka.util.dto;

import java.math.BigInteger;
import java.time.LocalDate;

/**
 * Book polled DTO
 * 
 * @author juoud1
 * @version 1.0
 * @since 2026
 */

public record Book(BigInteger bookId, String bookName, Author bookAuthor, LocalDate pubDate) {

}
