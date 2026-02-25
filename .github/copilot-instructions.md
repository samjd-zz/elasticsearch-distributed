# GitHub Copilot — Workspace Instructions

> These instructions apply to all Copilot suggestions in this workspace.
> They are derived from `.clinerules` and the project's architectural intent.

---

## Project Identity

- **Language:** Java 21 (LTS) — embrace every modern language feature.
- **Build:** Maven 3.9+; standard `src/main/java` / `src/test/java` layout.
- **Root package:** `com.elasticsearch.distributed`
- **Purpose:** A study project for a Senior Software Engineer role on the Elasticsearch
  Distributed Systems team. Every class is also a learning artefact — include study notes.

---

## 1. Java 21 — Prefer Modern Constructs

```java
// PREFER records for immutable value types
record ShardRouting(String index, int shardId, String nodeId, ShardState state) {}

// PREFER sealed classes for closed hierarchies
sealed interface ClusterEvent permits NodeJoined, NodeLeft, ShardStarted {}

// PREFER pattern-matching switch
String describe(ClusterEvent e) {
    return switch (e) {
        case NodeJoined j  -> "Node joined: " + j.nodeId();
        case NodeLeft  l   -> "Node left: "   + l.nodeId();
        case ShardStarted s -> "Shard started: " + s.shardId();
    };
}

// PREFER virtual threads for I/O-bound work
var executor = Executors.newVirtualThreadPerTaskExecutor();

// PREFER StructuredTaskScope for fan-out to replicas
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    replicas.forEach(r -> scope.fork(() -> replicaWrite(r, op)));
    scope.join().throwIfFailed();
}

// PREFER var for local variables where type is obvious
var client = factory.getClient();

// PREFER Optional — never return null from public APIs
Optional<String> leaderId() { return Optional.ofNullable(currentLeaderId); }
```

**Never use:** raw types · `instanceof` without pattern capture · `null` returns from public methods · `System.out.println`.

---

## 2. Elasticsearch Client Rules

- Always retrieve the client from `ElasticsearchClientFactory`; never construct `ElasticsearchClient` ad-hoc in a service.
- Manage the factory as a `Closeable`; close it in a `try-with-resources` or shutdown hook.
- **Bulk indexing** — use the Bulk API, never individual index calls in a loop:

```java
BulkRequest.Builder br = new BulkRequest.Builder();
docs.forEach(d -> br.operations(op -> op.index(i -> i.index(indexName).id(d.id()).document(d))));
client.bulk(br.build());
```

- **Deep pagination** — always use `search_after` + a Point-in-Time; never `from/size` beyond 10 000.
- **Resilience** — wrap every ES call with a Resilience4j `CircuitBreaker` + `Retry`:

```java
CircuitBreaker cb = registry.circuitBreaker("es-bulk");
Retry retry = Retry.of("es-bulk", retryConfig);
CheckedSupplier<BulkResponse> supplier = CircuitBreaker.decorateCheckedSupplier(cb,
    () -> client.bulk(request));
BulkResponse response = Retry.decorateCheckedSupplier(retry, supplier).get();
```

- Mappings must use **strict dynamic mapping** (`"dynamic": "strict"`); no field auto-creation.
- All connection parameters come from `ElasticsearchConfig.fromEnv()` — no hardcoded hosts, ports, or credentials.

---

## 3. Distributed Systems Patterns

### Immutability & State

- Model cluster state with **immutable records**; use a `Builder` inner class for incremental construction.
- Only the master/leader produces new `ClusterState` instances; all readers observe via `volatile` references.
- Publish state changes via a two-phase commit protocol (propose → acknowledge → commit).

### Concurrency Primitives

| Need                  | Use                                                        |
| --------------------- | ---------------------------------------------------------- |
| Sequence numbers      | `AtomicLong seqNoGenerator`                                |
| Metrics counters      | `LongAdder` (less contention than `AtomicLong` for writes) |
| Shard-level r/w lock  | `StampedLock` with optimistic read path                    |
| Role/term transitions | `ReentrantLock` for atomic compound updates                |
| Leader election state | `volatile` + `AtomicReference`                             |

### Raft / Leader Election

- Random election timeout: **150–300 ms** with `ThreadLocalRandom`.
- Heartbeat interval: **50 ms**.
- Quorum size: ⌊N/2⌋ + 1 where N = number of voting nodes.
- Always compare terms before granting a vote; reset to FOLLOWER on seeing a higher term.

### Shard Model

- Every shard has a `primaryTerm` (epoch) and a monotonically increasing `seqNo`.
- Promote a replica to primary by incrementing `primaryTerm`; this invalidates any stale primary (zombie detection).
- Route documents with a Murmur3-inspired hash: `Math.abs(routingValue.hashCode()) % numPrimaries`.

### Idempotency & Write Path

- All write operations must be idempotent; derive `_id` from a deterministic business-key hash.
- Write to primary → fan out to all ISR replicas → ack client only after all ISR replicas confirm.
- Use `StampedLock` optimistic read for the hot index path; upgrade to write lock only on conflict.
- Expose OCC (`indexWithCAS`) for compare-and-set semantics using `seqNo` + `primaryTerm`.

### Translog / Durability

- Append every operation to the translog **before** acknowledging the client.
- Frame layout: `seqNo(8) | primaryTerm(8) | bodyLen(4) | body | CRC32(4)`.
- Magic header: `0x454C5354`.
- Trim the translog to the global checkpoint with `trimToGlobalCheckpoint(long)`.
- Roll over at **512 MB**; provide `readOpsFrom(Path, long)` for crash-recovery replay.

### Observability

- Every public service method emits a Micrometer `Timer` **and** `Counter`.
- All log statements inside ES calls include MDC fields: `index`, `operation`, `node`.
- Use SLF4J (`LoggerFactory.getLogger(getClass())`); never `System.out.println`.
- Use structured logging (logback JSON encoder) in production profiles.

### Backpressure

- Place a Resilience4j `RateLimiter` in front of bulk indexing paths.
- Default bulk batch: **500 documents** or **5 MB**, whichever comes first; tune via `es.bulk.batch-size`.

---

## 4. Javadoc & Study Notes Style

All `public` classes and methods in `service` and `client` packages **must** have Javadoc.
Use the following structure to double as study material:

```java
/**
 * Brief one-line summary.
 *
 * <h2>Study Note</h2>
 * <p>Explain the distributed systems concept this implements.</p>
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 *   <li>Key insight 1</li>
 *   <li>Key insight 2</li>
 * </ul>
 *
 * @param param description
 * @return description
 */
```

---

## 5. Testing

### Unit Tests

- Class name: `<Subject>Test`, same package under `src/test/java`.
- Framework: JUnit 5 (`@Test`, `@ParameterizedTest`, `@ExtendWith(MockitoExtension.class)`).
- Assertions: **AssertJ** fluent style; no bare JUnit `assertEquals`.
- Mocking: **Mockito** to mock `ElasticsearchClient` at the service boundary — never mock the real cluster.
- Coverage target: ≥ 70 % lines (JaCoCo gate enforced at `mvn verify`).

### Integration Tests

- Class name: `<Subject>IT`; activated via `mvn verify -Pintegration-tests`.
- Use **Testcontainers** `ElasticsearchContainer` — never mock the ES client in ITs.
- Annotate with `@Testcontainers` + `@Container` (static) for container reuse.
- Each IT creates **and deletes** its own index; no shared state between tests.

---

## 6. Code Style

| Rule                 | Value                                                                   |
| -------------------- | ----------------------------------------------------------------------- |
| Indentation          | 4 spaces — no tabs                                                      |
| Max line length      | 120 characters                                                          |
| Imports              | No wildcards (except static); auto-organised; threshold 5 / 3           |
| Constants            | `UPPER_SNAKE_CASE`                                                      |
| Records              | `UpperCamelCase`; accessors have no `get` prefix                        |
| Collections returned | `List.copyOf()` or `Collections.unmodifiableList()` — never raw mutable |
| Class size           | ≤ 300 lines; split responsibility beyond that                           |
| TODO/FIXME           | Must include linked issue or explanation — never bare                   |

---

## 7. Build & Dependencies

- All dependency versions declared in `<properties>` in `pom.xml` — never inline versions in `<dependency>` blocks.
- Check `mvn dependency:tree` before adding any new dependency.
- Elasticsearch client **and** server versions must match the same minor version.
- Run `mvn verify -Pstatic-analysis` before opening a PR.

---

## 8. Security

- Never log credentials, API keys, or PII.
- Use TLS for all ES connections outside localhost; validate server cert.
- Store API keys in environment variables; document required env vars in `README.md`.
- Apply `_source_excludes` to strip sensitive fields before returning search responses.

---

## 9. Performance

- JVM: `-XX:+UseZGC -XX:+ZGenerational -Xms<heap> -Xmx<heap>` (set `-Xms` == `-Xmx`).
- Connection pool: `maxConnTotal=200`, `maxConnPerRoute=20`.
- Set all timeouts explicitly (`socketTimeoutMs`, `connectTimeoutMs`, `responseTimeoutMs`).

---

## 10. Hard Constraints — Never Do These

- No Spring Boot, Quarkus, or any DI framework — plain Java only.
- No Lombok — use records and IDE generation.
- No `System.out.println` — use SLF4J.
- No broad `catch (Exception e)` — catch specific types.
- No mutable collection returns from public APIs.
- No God classes > 300 lines.
- No ad-hoc `ElasticsearchClient` construction outside `ElasticsearchClientFactory`.
- No hardcoded hosts, ports, credentials, or batch sizes.
- No `from/size` pagination beyond 10 000 — use `search_after` + PIT.
- Do not upgrade ES client and server versions independently.
