# Copilot instructions for lib-evt-kafka-consumer

## Build, test, and validation

This project is a Spring Boot 4 application backed by Maven. Use the Maven wrapper from the repository root.

- Run the app locally:
  `./mvnw spring-boot:run`
- Run the full test suite:
  `./mvnw test`
- Run a single test class:
  `./mvnw -Dtest=LibEvtConsumerServiceTest test`
- Run a single test method:
  `./mvnw -Dtest=LibEvtConsumerServiceTest#processConsumerLibEvtSavesNewEventWithLinkedEntities test`
- There is no dedicated lint plugin configured in `pom.xml`; Maven test/build execution is the project’s validation gate.
- Local development expects Java 25 and a Kafka broker at `localhost:9092` with topic `lib-evts-topic`.

## Architecture at a glance

The application is a Kafka consumer for library events, not a web app with user-facing CRUD endpoints.

- `LibEvtConsumerApplication` bootstraps the Spring Boot app.
- `LibEvtConsumerConfiguration` enables Kafka and configures the listener container factory (`setConcurrency(3)`).
- `LibEvtConsumer` is the Kafka entry point: it consumes `ConsumerRecord<Integer, String>` from `lib-evts-topic`, deserializes the payload into a `LibEvt` DTO, and calls `LibEvtConsumerService.processConsumerLibEvt(...)`.
- `LibEvtConsumerService` is the domain logic layer. It dispatches on `LibEvtType`:
  - `NEW` => save author + book + library event
  - `UPDATE` => log and return without persisting
  - `OTHER` => reject with `IllegalArgumentException`
- Persistence is handled through Spring Data repositories (`AuthorRepository`, `BookRepository`, `LibEvtRepository`) and JPA entities (`AuthorEntity`, `BookEntity`, `LibEvtEntity`).
- The local profile uses an in-memory H2 database (`application-local.properties`), exposed via the app at `/consumer/h2-db` on port `8282`.
- Event payloads and enums live under `src/main/java/.../util/dto` and `util/enums`; keep DTO/entity shapes aligned when changing message contracts.

## Repository-specific conventions

- Treat `LibEvt` as the canonical incoming Kafka payload shape; use the record-based DTOs and enum values instead of arbitrary maps or ad-hoc JSON parsing.
- `NEW` is the only event type that writes to the database. `UPDATE` is intentionally non-persistent and only logs the event.
- Use the project’s existing profile-based local setup (`local` is the active default in the README). Don’t assume a remote Kafka cluster or a different DB while working locally.
- In the service layer, audit fields such as `createdBy`, `lastModifiedBy`, and timestamps are set using the repository’s current pattern (`"admin_synanto"` and `LocalDateTime.now()`). Follow that existing convention for consistency.
- Tests use JUnit 5 + Mockito and mock repository interactions directly; keep this style when adding coverage for processing logic.
- When changing the event contract, update the matching DTOs, enum handling, and any Kafka serialization assumptions together.
