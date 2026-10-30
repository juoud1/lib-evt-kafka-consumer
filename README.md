# Library Event Consumer

A Spring Boot application that consumes library events from Kafka and persists new authors, books, and events using Spring Data JPA and an in-memory H2 database.

## Requirements

- Java 25
- A Kafka broker available at `localhost:9092`
- The Kafka topic `lib-evts-topic`

Create the topic locally if it does not already exist:

```bash
kafka-topics.sh --create \
	--topic lib-evts-topic \
	--bootstrap-server localhost:9092 \
	--partitions 3 \
	--replication-factor 1
```

## Run

Start the application with the local profile:

```bash
./mvnw spring-boot:run
```

The application listens on port `8282` with the context path `/consumer`. The active profile is `local` by default.

## Kafka event format

The consumer reads JSON messages from `lib-evts-topic`. A `NEW` event has this shape:

```json
{
	"libEvtId": 1,
	"libEvtType": "NEW",
	"book": {
		"bookId": 12,
		"bookName": "Analytical Engine",
		"bookAuthor": {
			"authorId": 3,
			"authorName": "Ada Lovelace"
		},
		"pubDate": "1843-01-01"
	}
}
```

Supported event types are `NEW`, `UPDATE`, and `OTHER`. `NEW` persists the author, book, and event. `UPDATE` currently logs the event without changing persisted data. `OTHER` is rejected as an unsupported event type.

To publish a message locally, run:

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic lib-evts-topic
```

Then paste the JSON event on the producer prompt.

## H2 console

The local profile uses an in-memory H2 database. The console is available at [http://localhost:8282/consumer/h2-db](http://localhost:8282/consumer/h2-db). Use the JDBC URL configured in `application-local.properties`:

```text
jdbc:h2:mem:././librarydb
```

The in-memory database is reset when the application stops.

## Tests

Run the test suite with the Maven wrapper:

```bash
./mvnw test
```
