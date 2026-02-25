# agents.md — AI Agent Guidelines

This file provides authoritative guidance for AI coding agents (Copilot Agent, Claude,
Cursor, etc.) working autonomously in this repository. Follow every rule here without
exception.

---

## Project Snapshot

| Property     | Value                                                             |
| ------------ | ----------------------------------------------------------------- |
| Language     | Java 21 (LTS)                                                     |
| Build tool   | Maven 3.9+                                                        |
| Root package | `com.elasticsearch.distributed`                                   |
| ES client    | `co.elastic.clients:elasticsearch-java` 8.12.2                    |
| Purpose      | Senior SWE study project — Elasticsearch Distributed Systems team |

> Every source file doubles as study material. Preserve and extend Javadoc study notes
> whenever you touch a class.

---

## Codebase Map

```
src/main/java/com/elasticsearch/distributed/
├── Main.java                          # Entry point / wiring demo
├── client/
│   └── ElasticsearchClientFactory.java  # Sole place to construct ES clients
├── config/
│   └── ElasticsearchConfig.java         # All connection params via env vars
├── model/
│   ├── ClusterState.java                # Immutable cluster snapshot (Builder)
│   ├── Node.java                        # Node record + NodeRole enum
│   └── ShardRouting.java                # Shard record + ShardState / ShardType
├── service/
│   ├── ConcurrentIndexingService.java   # StampedLock, OCC, replica fan-out
│   ├── RaftLeaderElection.java          # Full Raft consensus implementation
│   └── ShardAllocationService.java      # Shard allocation + rebalancing
└── util/
    ├── AsyncNetworkChannel.java         # Async request/response correlation
    └── TranslogWriter.java              # WAL with fsync and crash recovery
```

---

## Build & Verify Commands

Always use these exact Maven commands — never compile files manually or use `javac` directly.

| Goal                               | Command                                    |
| ---------------------------------- | ------------------------------------------ |
| Compile (no tests)                 | `mvn clean package -DskipTests`            |
| Run unit tests                     | `mvn test`                                 |
| Full verify (unit + coverage gate) | `mvn verify`                               |
| Integration tests                  | `mvn verify -Pintegration-tests`           |
| Static analysis                    | `mvn verify -Pstatic-analysis -DskipTests` |
| Dependency tree                    | `mvn dependency:tree -Dverbose`            |

**After every code change:** run `mvn clean package -DskipTests` to confirm the build
is clean before running tests.

---

## Autonomous Task Workflow

1. **Read before writing.** Read the file(s) you plan to edit first; understand the
   existing style, imports, and Javadoc structure.
2. **One concern per class.** If a class would exceed 300 lines after your edit, split
   the responsibility into a new class.
3. **Build check loop.** After editing, run `mvn clean package -DskipTests`. If it
   fails, fix the errors before proceeding.
4. **Test check.** After the build is clean, run `mvn test`. Fix any regressions before
   finishing your task.
5. **Preserve study notes.** Never delete or shorten `<h2>Study Note</h2>` or
   `<h3>Interview Talking Points</h3>` blocks in Javadoc.

---

## Adding New Code — Required Patterns

### New service class

```java
/**
 * One-line summary.
 *
 * <h2>Study Note</h2>
 * <p>Distributed systems concept this implements.</p>
 *
 * <h3>Interview Talking Points</h3>
 * <ul>
 *   <li>Insight 1</li>
 *   <li>Insight 2</li>
 * </ul>
 */
public class MyService implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(MyService.class);
    private final ElasticsearchClient client;   // injected, never constructed here

    public MyService(ElasticsearchClientFactory factory) {
        this.client = factory.getClient();
    }

    @Override
    public void close() { /* release resources */ }
}
```

### New Elasticsearch operation

```java
// Always: CircuitBreaker + Retry wrapping
CircuitBreaker cb = registry.circuitBreaker("es-operation");
Retry retry = Retry.of("es-operation", retryConfig);
var response = Retry.decorateCheckedSupplier(retry,
    CircuitBreaker.decorateCheckedSupplier(cb, () -> client.someApi(req))).get();
```

### New model type

```java
// Use a record; include a Builder for multi-field construction
record MyState(String id, long version, List<String> nodes) {
    MyState {
        nodes = List.copyOf(nodes); // defensive copy in canonical constructor
    }

    static Builder builder() { return new Builder(); }

    static final class Builder {
        // fields + withXxx methods + build()
    }
}
```

---

## Concurrency Cheat Sheet

| Situation                               | What to use                                   |
| --------------------------------------- | --------------------------------------------- |
| Incrementing a per-shard seq number     | `AtomicLong.getAndIncrement()`                |
| Counting indexed docs (high write rate) | `LongAdder`                                   |
| Protecting shard read/write             | `StampedLock` — optimistic read first         |
| Atomic role + term transition (Raft)    | `ReentrantLock` over the compound update      |
| I/O-bound fan-out (replica writes)      | `Executors.newVirtualThreadPerTaskExecutor()` |
| Structured replica fan-out              | `StructuredTaskScope.ShutdownOnFailure`       |

---

## Hard Rules — Never Violate

| Rule                                        | Why                                                        |
| ------------------------------------------- | ---------------------------------------------------------- |
| No Spring Boot / Quarkus / DI framework     | Plain Java project by design                               |
| No Lombok                                   | Records cover all DTO needs; no annotation magic           |
| No `System.out.println`                     | Use `LoggerFactory.getLogger(getClass())`                  |
| No `catch (Exception e)` broadly            | Catch the specific checked type                            |
| No mutable collections from public APIs     | Return `List.copyOf()` or `Collections.unmodifiableList()` |
| No ad-hoc `ElasticsearchClient` in services | Must go through `ElasticsearchClientFactory`               |
| No hardcoded hosts, ports, credentials      | Read from `ElasticsearchConfig.fromEnv()`                  |
| No `from/size` pagination > 10 000          | Use `search_after` + Point-in-Time                         |
| No mismatched ES client/server versions     | Both must be the same minor version                        |
| No inline dependency versions in pom.xml    | All versions live in `<properties>`                        |
| No class > 300 lines                        | Split into smaller, focused classes                        |
| No bare TODO/FIXME                          | Always add context or issue reference                      |

---

## Testing Rules

### Unit tests (`*Test.java`)

- JUnit 5 + AssertJ + Mockito — never mix in JUnit 4 or Hamcrest.
- Mock `ElasticsearchClient` at the service boundary; never call real ES.
- Place test class in the same package under `src/test/java`.
- Coverage target: ≥ 70 % lines (JaCoCo enforced at `mvn verify`).

### Integration tests (`*IT.java`)

- Use `@Testcontainers` + `@Container static ElasticsearchContainer`.
- Each IT creates **and deletes** its own index — no shared state.
- Activate with `mvn verify -Pintegration-tests`.

---

## Git Conventions

- Branch prefixes: `feat/`, `fix/`, `chore/`, `refactor/`.
- Commit subject: imperative mood, ≤ 72 characters.
- Never commit `target/`, `*.class`, or `*.jar`.
- Every PR must pass `mvn verify` before merge.

---

## Security Checklist (before any commit)

- [ ] No credentials, API keys, or PII in log statements.
- [ ] TLS required for all non-localhost ES connections.
- [ ] Env vars documented in `README.md` for any new config key.
- [ ] `_source_excludes` applied where sensitive fields exist.

---

## Dependency Addition Checklist

Before adding any library:

1. Run `mvn dependency:tree -Dverbose` — confirm it isn't already transitive.
2. Add the version to `<properties>` in `pom.xml`, not inline.
3. Ensure ES client and server versions remain in sync.
4. Run `mvn verify -Pstatic-analysis` to confirm no conflicts.
