# Quasar — Pulsar Sans-io Rust Driver — Research Dossier

Status: IN-PROGRESS. Sections will be filled incrementally.

## 1. Goal restatement

Build **quasar**, a new Apache Pulsar client driver in Rust, hosted at `/home/florentin/Sources/github.com/me/quasar` (currently empty). Architecture: **sans-io core** (pure-Rust protocol layer, no async runtime, no sockets — like `quinn-proto`, `h2`, `rustls`) wrapped by a thin I/O engine. **Default engine: moonpool** (PierreZ's async runtime ecosystem). Feature target: parity with the Apache Pulsar **Java client** surface (producer/consumer/reader, schemas, transactions, auth plug-ins, chunking, batching, key-shared, transactions, regex/topic-watcher subscriptions). Tests: deterministic **unit tests** against the sans-io state machine + **e2e tests** against a real broker (Docker `apachepulsar/pulsar`). The sans-io split de-risks the engine choice: if moonpool's API proves insufficient or unstable, we still ship the protocol with a tokio (or smol) adapter, with moonpool migrating to a `quasar-runtime-moonpool` crate on equal footing with the others.

## 2. Pulsar binary protocol surface

Wire framing (see `pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java:1870-1934` and `:2090-2120`):

- **Simple command frame** (no payload): `[TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand bytes]`. `total_size = cmd_size + 4`; the outer 4-byte length is exclusive of itself. Build at `Commands.java:1866-1883`.
- **Send/Message frame** (with payload): `[TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand bytes][MAGIC u16=0x0e01][CRC32C u32][METADATA_SIZE u32][MessageMetadata bytes][PAYLOAD bytes]`. Producer constructs at `Commands.java:1885-1934`; the magic + crc are part of the metadata+payload region the broker forwards verbatim, computed over `[METADATA_SIZE][METADATA][PAYLOAD]`.
- **Broker-entry metadata wrapper** (broker-injected; v16+): prepended to dispatched messages as `[MAGIC u16=0x0e02][BEM_SIZE u32][BrokerEntryMetadata bytes]` before the standard message frame. Defined at `Commands.java:138-141`, `:1936-1965`. Detect via `getShort(readerIndex) == 0x0e02` and skip / parse at `Commands.java:1974-2038`.
- Magic constants: `magicCrc32c = 0x0e01` (`Commands.java:138`), `magicBrokerEntryMetadata = 0x0e02` (`Commands.java:140`), `checksumSize = 4` (`Commands.java:141`). CRC32C polynomial.
- ChecksumType enum (Rust port): `None`, `Crc32c` (Castagnoli — must use the same poly as Java's `io.netty.handler.codec.compression.snappy.Crc32C` / SSE 4.2 crc32c). Declared at `Commands.java:2340`.
- Decoder reference (Netty `LengthFieldBasedFrameDecoder`-style): `PulsarDecoder.java` (`pulsar-common/src/main/java/org/apache/pulsar/common/protocol/PulsarDecoder.java`) and `FrameDecoderUtil.java` show the 4-byte-prefix split rules.

BaseCommand types and their proto lines (from `pulsar-common/src/main/proto/PulsarApi.proto:1144-1342`). All commands in one tagged union `BaseCommand` with `required Type type = 1` selector:

| Wire type id | BaseCommand | Proto line |
|---|---|---|
| 2 | CONNECT | proto:1146 / msg 282 |
| 3 | CONNECTED | proto:1147 / msg 322 |
| 4 | SUBSCRIBE | proto:1148 / msg 358 |
| 5 | PRODUCER | proto:1150 / msg 491 |
| 6 | SEND | proto:1152 / msg 533 |
| 7 | SEND_RECEIPT | proto:1153 / msg 551 |
| 8 | SEND_ERROR | proto:1154 / msg 558 |
| 9 | MESSAGE | proto:1156 / msg 565 |
| 10 | ACK | proto:1157 / msg 573 |
| 11 | FLOW | proto:1158 / msg 619 |
| 12 | UNSUBSCRIBE | proto:1160 / msg 627 |
| 13 | SUCCESS | proto:1162 / msg 682 |
| 14 | ERROR | proto:1163 / msg 707 |
| 15 | CLOSE_PRODUCER | proto:1165 / msg 662 |
| 16 | CLOSE_CONSUMER | proto:1166 / msg 669 |
| 17 | PRODUCER_SUCCESS | proto:1168 / msg 688 |
| 18 | PING | proto:1170 / msg 716 |
| 19 | PONG | proto:1171 / msg 718 |
| 20 | REDELIVER_UNACKNOWLEDGED_MESSAGES | proto:1173 / msg 676 |
| 21/22 | PARTITIONED_METADATA / RESPONSE | proto:1175-1176 / msgs 421, 436 |
| 23/24 | LOOKUP / LOOKUP_RESPONSE | proto:1178-1179 / msgs 448, 468 |
| 25/26 | CONSUMER_STATS / RESPONSE | proto:1181-1182 / msgs 721, 728 |
| 27 | REACHED_END_OF_TOPIC | proto:1184 / msg 645 |
| 28 | SEEK | proto:1186 / msg 634 |
| 29/30 | GET_LAST_MESSAGE_ID / RESPONSE | proto:1188-1189 / msgs 773, 778 |
| 31 | ACTIVE_CONSUMER_CHANGE | proto:1191 / msg 614 |
| 32/33 | GET_TOPICS_OF_NAMESPACE / RESPONSE | proto:1194-1195 / msgs 784, 799 |
| 34/35 | GET_SCHEMA / RESPONSE | proto:1197-1198 / msgs 998, 1005 |
| 36 | AUTH_CHALLENGE | proto:1200 / msg 335 |
| 37 | AUTH_RESPONSE | proto:1201 / msg 329 |
| 38 | ACK_RESPONSE | proto:1203 / msg 604 |
| 39/40 | GET_OR_CREATE_SCHEMA / RESPONSE | proto:1205-1206 / msgs 1014, 1021 |
| 50/51 | NEW_TXN / RESPONSE | proto:1209-1210 / msgs 1047, 1053 |
| 52/53 | ADD_PARTITION_TO_TXN / RESPONSE | proto:1212-1213 / msgs 1061, 1068 |
| 54/55 | ADD_SUBSCRIPTION_TO_TXN / RESPONSE | proto:1215-1216 / msgs 1080, 1087 |
| 56/57 | END_TXN / RESPONSE | proto:1218-1219 / msgs 1095, 1102 |
| 58/59 | END_TXN_ON_PARTITION / RESPONSE | proto:1221-1222 / msgs 1110, 1119 |
| 60/61 | END_TXN_ON_SUBSCRIPTION / RESPONSE | proto:1224-1225 / msgs 1127, 1136 |
| 62/63 | TC_CLIENT_CONNECT_REQUEST / RESPONSE | proto:1226-1227 / msgs 1036, 1041 |
| 64-67 | WATCH_TOPIC_LIST / SUCCESS / UPDATE / CLOSE | proto:1229-1232 / msgs 810, 819, 826, 833 |
| 68 | TOPIC_MIGRATED | proto:1234 / msg 649 |
| 70-78 | SCALABLE_TOPIC_* | proto:1236-1246 |

Critical enums (cite line):

- `CompressionType` (proto:92): `NONE=0, LZ4=1, ZLIB=2, ZSTD=3, SNAPPY=4`. Implies four codec adapters needed.
- `ProducerAccessMode` (proto:100): `Shared, Exclusive, WaitForExclusive, ExclusiveWithFencing`.
- `CommandSubscribe.SubType` (proto:359): `Exclusive=0, Shared=1, Failover=2, Key_Shared=3`.
- `CommandSubscribe.InitialPosition` (proto:389): `Latest=0, Earliest=1`.
- `CommandAck.AckType` (proto:574): `Individual=0, Cumulative=1`.
- `CommandAck.ValidationError` (proto:588): `UncompressedSizeCorruption, DecompressionError, ChecksumMismatch, BatchDeSerializeError, DecryptionError`.
- `KeySharedMode` (proto:347): `AUTO_SPLIT=0, STICKY=1`.
- `ServerError` (proto:206): 26 codes incl. `ChecksumError`, `TooManyRequests`, `IncompatibleSchema`, `ProducerFenced`, `TransactionConflict`.
- `ProtocolVersion` (proto:254): client should claim **v21** (highest current); negotiation hits `CONNECTED.protocol_version`.
- `ChecksumType` (java-side, `Commands.java:2340`): `None, Crc32c`. Wire defines magic+CRC32C only.

Message-bearing types and structural notes:

- `MessageMetadata` (proto:107-178). Required: `producer_name`, `sequence_id`, `publish_time`. Optional carries chunking (`uuid`, `num_chunks_from_msg`, `total_chunk_msg_size`, `chunk_id` — proto:160-163), batching (`num_messages_in_batch` proto:126, `highest_sequence_id` proto:156), compression (`compression`, `uncompressed_size` proto:120-121), ordering key (proto:141), schema version (proto:137), encryption (proto:132-136), txn ids (proto:152-153), null-value flags (proto:159, 166), deliver-at (proto:144), marker type (proto:149), `compacted_batch_indexes` (proto:176), `schema_id` (proto:177).
- `SingleMessageMetadata` (proto:180-198): one per item inside a batch payload; carries per-item `event_time`, `sequence_id`, `partition_key`, `ordering_key`, `null_value`, `null_partition_key`, `compacted_out`.
- `BrokerEntryMetadata` (proto:201-204): `broker_timestamp`, `index`. Prepended by broker for v16+ subscribers that opt in via `FeatureFlags.supports_broker_entry_metadata` (proto:313).
- `FeatureFlags` (proto:311-320): `supports_auth_refresh`, `supports_broker_entry_metadata`, `supports_partial_producer`, `supports_topic_watchers`, `supports_get_partitioned_metadata_without_auto_creation`, `supports_repl_dedup_by_lid_and_eid`, `supports_topic_watcher_reconcile`, `supports_scalable_topics`. Quasar must negotiate these on CONNECT.
- `MessageIdData` (proto:59-69): `ledger_id`, `entry_id`, `partition` (-1 for non-partitioned), `batch_index`, `ack_set` (cumulative-batch bitset), `batch_size`, `first_chunk_message_id` (chunk reassembly anchor).
- `Schema` + `Schema.Type` (proto:25-57): 23 enum variants from `None` to `External`/`AutoConsume`.

Markers (`pulsar-common/src/main/proto/PulsarMarkers.proto:25-83`): `REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST/RESPONSE/SNAPSHOT/UPDATE` + `TXN_COMMITTING/COMMIT/ABORT`. Marker payloads flow as ordinary messages with `MessageMetadata.marker_type` set; a sans-io decoder must surface them but most clients ignore them.

**Sans-io implication**: the entire surface above is a pure encode/decode + state-machine job. Every frame layout, magic check, CRC32C verification, batch-payload split, broker-entry-metadata peel, chunk reassembly, and request-id correlation map cleanly to deterministic table-driven tests. No I/O required for codec or state tests.

## 3. Java client architecture

Module map (under `/home/florentin/Sources/github.com/apache/pulsar/`):

- `pulsar-client-api/` — public interfaces (`PulsarClient`, `Producer`, `Consumer`, `Reader`, `Message`, `MessageId`, `Schema`, `Authentication`).
- `pulsar-client/` — single jar that ships everything: client impl, lookup, schema, oauth2, message-crypto bridge.
- `pulsar-client-admin-api/` + `pulsar-client-admin/` — REST admin (JAX-RS / Jersey).
- `pulsar-client-auth-sasl/`, `pulsar-client-auth-athenz/`, `pulsar-client-messagecrypto-bc/` — pluggable auth + crypto bridges (split out so the core jar stays light).

Load-bearing classes (path / sans-io vs I/O boundary):

- **`PulsarClientImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/PulsarClientImpl.java:105`. Top-level façade. Owns `ConnectionPool`, `LookupService`, `EventLoopGroup`/`Timer`, `Schema` cache, producer/consumer registries. Mixed I/O + orchestration. Sans-io counterpart in quasar: a config struct + a "client core" that holds maps; runtime owns the event loop + DNS.
- **`ConnectionPool`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java:67`. Keyed by `(logicalAddress, physicalAddress, randIdx)` (`:240-272`), backed by Netty `Bootstrap`. Heavy Netty coupling — entirely engine-side in quasar. Sans-io counterpart: a host->session-id map carried by the application, not the protocol layer.
- **`ClientCnx`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ClientCnx.java:117` (extends `PulsarHandler` which is a Netty `ChannelInboundHandler`). The single broker session state machine. Maintains: `pendingRequests` map keyed by requestId (`:132-134`), `producers` map keyed by producerId (`:141`), `consumers` map keyed by consumerId (`:147`), `transactionMetaStoreHandlers` (`:152`), `topicListWatchers` (`:158`). Dispatch entry points: `handleConnected` (`:432`), `handleAuthChallenge` (`:464`), `handleSendReceipt` (`:515`), `handleSendError`, `handleMessage`, `handlePing`/`handlePong`, etc. **This is the canonical sans-io boundary**: pure logic = state transitions + correlation tables; I/O = Netty channel write/read. Quasar's `quasar-proto::Connection` should mirror this class as a sans-io state machine.
- **`HandlerState`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/HandlerState.java:27-105`. Tiny abstract base for producer/consumer connection-state lifecycle: `Uninitialized, Connecting, Ready, Closing, Closed, Terminated, Failed, RegisteringSchema, ProducerFenced`. Pure logic — direct port.
- **`ConnectionHandler`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionHandler.java`. Owns reconnect/backoff for a producer or consumer over a connection. Sans-io counterpart: emit a "reconnect needed" event with a backoff hint; engine schedules.
- **`ProducerBase`** + **`ProducerImpl<T>`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerBase.java`, `:ProducerImpl.java:113`. Implements `ConnectionHandler.Connection`. Owns `batchMessageContainer` (`:135`), `lastSequenceIdPublished` (`:153`), `lastSequenceIdPushed` (`:158`), `msgCrypto` (`:163`). `sendAsync` (`:419`) builds `OpSendMsg`, optionally chunks (`ChunkedMessageCtx` at `:1570`), batches, encrypts, then ships. Sans-io: separate the OpSendMsg state machine + dedup tracking from the actual write; engine pushes ready frames.
- **`PartitionedProducerImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/PartitionedProducerImpl.java` (24k). Multiplex producer fan-out + topic metadata watcher + per-partition `ProducerImpl`. Pure logic except for the watcher subscription.
- **`MultiTopicsConsumerImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/MultiTopicsConsumerImpl.java` (79k). Fans subscribe across multiple topics / partitions; round-robin internal incoming queues. Pure logic.
- **`ConsumerBase`** + **`ConsumerImpl<T>`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerBase.java`, `:ConsumerImpl.java:143`. Owns `acknowledgmentsGroupingTracker` (`:174`), `negativeAcksTracker` (`:175`), `seekStatus` (`:185`), `deadLetterPolicy` (`:209`), `retryLetterProducer` (`:213`), `chunkedMessagesMap` (`:219`), `incomingMessages` queue (`:528-531`). Receiver-queue + flow-permit logic, ack tracker dispatch, redelivery, seek, dead-letter routing, batch index acks, chunk reassembly. Sans-io: heavy — split into receiver-queue state, ack tracker state, chunk reassembly state, each pure.
- **`ZeroQueueConsumerImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ZeroQueueConsumerImpl.java`. Subclass for `receiver_queue=0` semantics.
- **`PatternMultiTopicsConsumerImpl`** + **`PatternConsumerUpdateQueue`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/PatternMultiTopicsConsumerImpl.java`, `PatternConsumerUpdateQueue.java`. Regex-topic subscriptions and broker watcher reconciliation.
- **`ReaderImpl`** + **`MultiTopicsReaderImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/ReaderImpl.java`, `MultiTopicsReaderImpl.java`. Reader is a thin wrapper over a non-durable Consumer.
- **`TableViewImpl`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/TableViewImpl.java`. Compact table backed by a topic.
- **`BinaryProtoLookupService`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java:56` (`implements LookupService`). `getBroker` (`:146`) sends `CommandLookupTopic` over `ClientCnx`, handles `LookupType.Redirect` recursion. `getPartitionedTopicMetadataAsync` (`:260`) sends `CommandPartitionedTopicMetadata`. Sans-io candidate: produce request frames + parse responses; I/O picks the connection.
- **`HttpLookupService`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/HttpLookupService.java:58`. Same surface via REST. Bypass for quasar v1 unless we ship `quasar-admin`.
- **`PulsarServiceNameResolver`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/PulsarServiceNameResolver.java`. Resolves the `pulsar://host:port` multi-URL string and rotates.
- **`Backoff`** — `pulsar-client/src/main/java/org/apache/pulsar/client/impl/Backoff.java` (under `util/` originally, used everywhere). Truncated exponential backoff with jitter.

Auth (under `pulsar-client/src/main/java/org/apache/pulsar/client/impl/auth/`):
- **`AuthenticationDisabled`**, **`AuthenticationBasic`**, **`AuthenticationToken`** (`AuthenticationToken.java`), **`AuthenticationTls`** (`AuthenticationTls.java`, 5.3k), **`AuthenticationKeyStoreTls`** (`AuthenticationKeyStoreTls.java`, 5.2k).
- **OAuth2**: `oauth2/AuthenticationOAuth2.java` (17k), `AuthenticationFactoryOAuth2.java` (14k), `ClientCredentialsFlow.java` (8k), `TlsClientAuthFlow.java` (6.6k), `Flow.java`, `FlowBase.java`, `KeyFile.java`. Plus `oauth2/protocol/` token endpoint impl + `oauth2/AuthenticationDataOAuth2.java`.
- **SASL** (`pulsar-client-auth-sasl/src/main/java/org/apache/pulsar/client/impl/auth/`): `AuthenticationSasl.java` (15k), `PulsarSaslClient.java` (6.6k), `SaslAuthenticationDataProvider.java` (2.5k).
- **Athenz** (`pulsar-client-auth-athenz/src/main/java/org/apache/pulsar/client/impl/auth/`): `AuthenticationAthenz.java`, `AuthenticationDataAthenz.java`.
- Common contract: `org.apache.pulsar.client.api.Authentication` + `AuthenticationDataProvider` (in `pulsar-client-api/`). Sans-io candidate: every `AuthenticationDataProvider` is pure if the IO of refreshing tokens is delegated; the AUTH_CHALLENGE handshake (`pip-30.md`) is pure state.

Schema (under `pulsar-client/src/main/java/org/apache/pulsar/client/impl/schema/`):
- Primitive: `BooleanSchema`, `ByteSchema`, `ShortSchema`, `IntSchema`, `LongSchema`, `FloatSchema`, `DoubleSchema`, `BytesSchema`, `ByteBufferSchema`, `ByteBufSchema`, `StringSchema`, `DateSchema`, `TimeSchema`, `TimestampSchema`, `InstantSchema`, `LocalDateSchema`, `LocalTimeSchema`, `LocalDateTimeSchema`.
- Struct: `AvroBaseStructSchema`, `AvroSchema` (7k), `JSONSchema` (5.5k), `ProtobufSchema` (5.5k), `ProtobufNativeSchema` (6.2k), `NativeAvroBytesSchema`, `StructSchema`, `AbstractStructSchema`, `KeyValueSchemaImpl` (18k).
- AutoConsume / AutoProduce: `AutoConsumeSchema.java` (16k), `AutoProduceBytesSchema.java` (4.4k).
- GenericRecord side under `schema/generic/`, builder under `schema/reader/` + `schema/writer/`.
- Sans-io: most schemas are pure encode/decode wrappers. The schema-version cache + lookup against `GET_OR_CREATE_SCHEMA` and `GET_SCHEMA` should be pulled into protocol state.

Transactions (under `pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/`):
- **`TransactionImpl`** — `TransactionImpl.java:54-244`. Lifecycle (begin/commit/abort), produced topics + cumulative-ack consumers registry.
- **`TransactionCoordinatorClientImpl`** (11k) + **`TransactionBuilderImpl`** + **`TransactionBufferHandler`**.
- Sans-io: TC client is a request/response state machine over the same connection — clean fit. Producer's per-msg txn ids land in `MessageMetadata.txnid_*`.

Trackers (`pulsar-client/src/main/java/org/apache/pulsar/client/impl/`):
- **`NegativeAcksTracker`** — `NegativeAcksTracker.java:44-216`. Timer-driven set of (messageId -> deadline) → emits REDELIVER once timer fires.
- **`UnAckedMessageTracker`** — `UnAckedMessageTracker.java:45-...`. Sliding-window time buckets, expire → REDELIVER.
- **`PersistentAcknowledgmentsGroupingTracker`** — `PersistentAcknowledgmentsGroupingTracker.java`. Batches Individual/Cumulative ACKs over `ackGroupTimeMs`. Last-cumulative-ack at `:707-742`.
- **`NonPersistentAcknowledgmentGroupingTracker`** — no-op variant.
- Sans-io: every tracker is a pure state machine driven by `tick(now)` returning an action set. Direct match for quinn-proto-style polling.

Crypto:
- **`MessageCryptoBc`** — `pulsar-client-messagecrypto-bc/src/main/java/org/apache/pulsar/client/impl/crypto/MessageCryptoBc.java:85`. AES-GCM data-key wrap over BouncyCastle. The data key (`encryptionKey`) is rotated per call; the encrypted key list lives in `MessageMetadata.encryption_keys`. Pure logic on top of a crypto provider — for Rust, use `ring` or `aws-lc-rs`.

Other:
- **`AutoClusterFailover`** + **`ControlledClusterFailover`** — `AutoClusterFailover.java` (18k), `ControlledClusterFailover.java` (13k). Switch between Pulsar cluster URLs.
- **`MemoryLimitController`** — `MemoryLimitController.java:5.3k`. Application-side backpressure budget.
- **`PulsarChannelInitializer`** — `PulsarChannelInitializer.java` (12k). Netty pipeline (frame decoder, TLS, proxy protocol).
- **`OptionalProxyProtocolDecoder`** — `pulsar-common/src/main/java/org/apache/pulsar/common/protocol/OptionalProxyProtocolDecoder.java`. HAProxy PROXY protocol v1/v2 — likely defer in quasar v0.

**Sans-io split summary** — for each above, "pure" = encode/decode + state machine; "engine" = sockets, TLS, timers, DNS, async runtime, thread-pool.

## 4. Test inventory

The Java client has three layers of tests we need to mirror; each maps to a quasar layer.

### Unit tests — pure-logic over Mockito-mocked I/O

Location: `pulsar-client/src/test/java/org/apache/pulsar/client/impl/`. Mockito is the framework (`mock(...)`, `when(...).thenReturn(...)`, `spy(...)`, `verify(...)`). What is mocked: `PulsarClientImpl`, `ClientCnx`, `Channel`/`ChannelHandlerContext`, `ConsumerImpl`. The mocks stand in for the broker connection so each tracker/state machine can be ticked in isolation.

- `AcknowledgementsGroupingTrackerTest.java:22-26` — Mockito imports; `AcknowledgementsGroupingTrackerTest.java:254-280` constructs `mock(PulsarClientImpl.class)`, `mock(ConsumerImpl.class)` to drive the ACK grouping tracker. Direct sans-io fit: `quasar-proto::AckTracker::poll(now)`.
- `ClientCnxTest.java:22-26, 254-280` — uses `mock(PulsarClientImpl.class)` to fake the client, then asserts `ClientCnx` state transitions on `handleConnected`/`handleAuthChallenge`/`handleSendReceipt`. This is the canonical template for `quasar-proto::Connection` unit tests.
- `LastCumulativeAckTest.java` — pure logic test on the cumulative-ack value object (no Mockito needed); same shape as quasar's batch-index ack ledger.
- `BatchMessageContainerImplTest.java` — packs N `MessageImpl` items, drives the batch container, asserts the produced frame. Direct sans-io fit: encoder unit tests.
- `OpSendMsgQueueTest.java` — in-memory FIFO of outstanding sends; pure logic.
- `MessageIdCompareToTest.java` (19k) — exhaustive MessageId ordering matrix. Mirror as proptest.
- `ChunkMessageIdImplTest.java`, `BatchMessageIdImplTest.java`, `TopicMessageIdImplTest.java`, `MessageIdAdvUtilsTest.java`, `MessageIdSerializationTest.java` — message-id type matrix.
- `BinaryProtoLookupServiceTest.java` (24k) — mocks `ClientCnx`, expects `CommandLookupTopic`, replays `CommandLookupTopicResponse` (incl. `Redirect`). Direct template for sans-io lookup state machine.
- `PulsarServiceNameResolverTest.java` — URL parsing + rotation.
- `TopicListWatcherTest.java` — PIP-145 regex-watcher state machine.
- `PatternConsumerUpdateQueueTest.java` — regex-watcher reconciliation queue.

There is no full broker-process spawn for these — tests run in a single JVM in ms. Quasar's sans-io tests should run in microseconds with no async runtime.

### Broker-side integration tests — in-JVM standalone broker via `MockedPulsarServiceBaseTest`

Location: `pulsar-broker/src/test/java/org/apache/pulsar/client/api/`. These spin up a real broker inside the JUnit/TestNG JVM and exercise the *Java client* against it. Common bases:

- `ProducerConsumerBase` (e.g., `BrokerServiceLookupTest.java`, `AuthenticatedProducerConsumerTest.java`).
- `SharedPulsarBaseTest` (e.g., `ClientDeduplicationTest.java`, `DeadLetterTopicTest.java`, `KeySharedSubscriptionTest.java`).
- `MockedPulsarServiceBaseTest` (e.g., `PatternConsumerBackPressureMultipleConsumersTest.java`).
- `MockBrokerService.java` (17k) + `MockBrokerServiceHooks.java` — a hand-written Netty-server broker stub with per-command hooks (`CommandConnectHook`, `CommandProducerHook`, `CommandSubscribeHook`, `CommandSendHook`, `CommandFlowHook`, `CommandAckHook`, `CommandCloseProducerHook`, …). Used by `ClientErrorsTest.java` to fault-inject specific replies. This is the closest analogue to what we'd implement in Rust as a sans-io **broker fake** (no I/O — just frame in, frame out) for protocol-level "broker says X, what does client do" tests.

These are not quasar's primary test surface — we'd rather drive `quasar-proto::Connection` directly without a fake — but `MockBrokerService.java` is a great spec source for what bad-broker behaviors to cover.

### End-to-end / system tests — Docker via Testcontainers

Location: `tests/integration/src/test/java/org/apache/pulsar/tests/integration/`. 23+ subpackages (`admin`, `auth`, `messaging`, `schema`, `transaction`, `compaction`, `proxy`, `standalone`, `tls`, `io`, `functions`, …). Base class: `PulsarTestSuite` (extended at `cli/CLITest.java:53`, `compaction/TestCompaction.java:46`, `cli/tenant/TenantTest.java:35`, etc.). Containers under `containers/`:

- `PulsarContainer.java:61-62` declares the image:
  ```
  public static final String DEFAULT_IMAGE_NAME = System.getenv().getOrDefault(
      "PULSAR_TEST_IMAGE_NAME", "apachepulsar/pulsar-test-latest-version:latest");
  ```
  Pinned tag at `:66`: `apachepulsar/pulsar:3.0.7`. Historical pins for `2.5.0`/`2.4.0`/`2.3.0` at `:68-70`.
- `StandaloneContainer.java:28-41` — single-node broker, the right thing to launch for quasar e2e in CI.
- Docker compose alternative for multi-broker: `tests/compose/simple/docker-compose.yml:28` and `tests/compose/multi/docker-compose.yml`, image `apachepulsar/pulsar-test-latest-version:latest`.

**Recommendation for quasar e2e in CI**: `apachepulsar/pulsar:3.3.x` (latest LTS, but pin) via either:
- `testcontainers-rs` crate (mirrors Java's Testcontainers), or
- `docker compose up` started by a `justfile`/`xtask`-style runner before `cargo test --features e2e`.

Use 3.0.x as minimum supported broker (matches what Apache's own test harness still pins for back-compat at `PulsarContainer.java:66`).

### Test class names to mirror in Rust (10 representative)

1. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/ClientCnxTest.java` → `quasar-proto::tests::connection_handshake`.
2. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/AcknowledgementsGroupingTrackerTest.java` → `quasar-proto::tests::ack_tracker`.
3. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/BatchMessageContainerImplTest.java` → `quasar-proto::tests::batch_container`.
4. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/BinaryProtoLookupServiceTest.java` → `quasar-proto::tests::lookup`.
5. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/MessageIdCompareToTest.java` → `quasar-proto::message_id::cmp_proptest`.
6. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/ChunkMessageIdImplTest.java` → `quasar-proto::chunk::id_roundtrip`.
7. `pulsar-client/src/test/java/org/apache/pulsar/client/impl/TopicListWatcherTest.java` → `quasar-proto::watcher::reconcile`.
8. `pulsar-broker/src/test/java/org/apache/pulsar/client/api/ClientErrorsTest.java` (`MockBrokerService`-based) → `quasar-proto::tests::fault_injection`.
9. `pulsar-broker/src/test/java/org/apache/pulsar/client/api/ClientDeduplicationTest.java` → `quasar-e2e::dedup`.
10. `pulsar-broker/src/test/java/org/apache/pulsar/client/api/DeadLetterTopicTest.java` (74k — exhaustive DLQ matrix) → `quasar-e2e::dead_letter`.

## 5. PIPs that change client wire behavior

PIPs are at `/home/florentin/Sources/github.com/apache/pulsar/pip/pip-<N>.md`. 305 PIPs total; below is the subset that changes wire framing, BaseCommand semantics, MessageMetadata, FeatureFlags, or client-visible protocol behavior. Verified by reading title + opening paragraph of each.

| PIP # | Title | Wire-affecting | Path | One-sentence intent |
|---|---|---|---|---|
| PIP-4 | Pulsar End to End Encryption | Y | `pip/pip-4.md` | Adds `MessageMetadata.encryption_keys` + `encryption_algo` + `encryption_param`; producer encrypts payload, consumer decrypts. |
| PIP-13 | Subscribe to topics represented by regular expressions | Y | `pip/pip-13.md` | Original regex consumer; client polls `GetTopicsOfNamespace`. Superseded for live updates by PIP-145. |
| PIP-22 | Pulsar Dead Letter Topic | Y | `pip/pip-22.md` | Adds client-driven DLQ producer + property `RECONSUMETIMES`; broker is mostly unaware, client republishes after maxRedeliveryCount. |
| PIP-26 | Delayed Message Delivery | Y | `pip/pip-26.md` | Adds `MessageMetadata.deliver_at_time`; broker holds delivery in shared subscriptions until deadline. |
| PIP-30 | Change authentication provider API to support mutual authentication | Y | `pip/pip-30.md` | Introduces `AUTH_CHALLENGE` / `AUTH_RESPONSE` BaseCommand pair for in-band token refresh and SASL-style multi-step auth. |
| PIP-31 | Transaction Support (Transactional Streaming) | Y | `pip/pip-31.md` | Adds TC client commands `NEW_TXN`, `ADD_PARTITION_TO_TXN`, `ADD_SUBSCRIPTION_TO_TXN`, `END_TXN*` + `MessageMetadata.txnid_*`. |
| PIP-33 | Replicated subscriptions | Y | `pip/pip-33.md` | Adds marker commands (`REPLICATED_SUBSCRIPTION_SNAPSHOT_*`) + `CommandSubscribe.replicated_subscription_state_enabled`. |
| PIP-34 | Add new subscribe type Key_shared | Y | `pip/pip-34.md` | Adds `SubType.Key_Shared=3`; broker hashes per-key dispatch. |
| PIP-37 | Large message size handling (chunking) | Y | `pip/pip-37.md` | Adds chunk fields to `MessageMetadata`: `uuid`, `chunk_id`, `num_chunks_from_msg`, `total_chunk_msg_size`; producer splits, consumer reassembles. |
| PIP-39 | Namespace Change Events & Topic Policy | N | `pip/pip-39.md` | Internal system-topic mechanism; clients consume but no new command. |
| PIP-54 | Acknowledgement at batch index level | Y | `pip/pip-54.md` | Adds `MessageIdData.ack_set` (bitset over batch indices) + `MessageIdData.batch_size`; ACK now carries a batch-index bitset. |
| PIP-58 | Custom retry delay (retry topic) | Y | `pip/pip-58.md` | Adds `RECONSUMETIMES`, `DELAY_TIME` properties + client retry-topic producer; broker unchanged. |
| PIP-68 | Exclusive Producer | Y | `pip/pip-68.md` | Adds `ProducerAccessMode.Exclusive` + `WaitForExclusive` to `CommandProducer`; broker fences other producers. |
| PIP-70 | Lightweight broker entry metadata | Y | `pip/pip-70.md` | Server-side feature; serialized header that prepends dispatched messages. |
| PIP-74 (n/a) | — | — | — | (gap; not present) |
| PIP-79 | Reduce redundant producers from partitioned producer | N | `pip/pip-79.md` | Optimization in client lookup; no wire change. |
| PIP-90 | Expose broker entry metadata to the client | Y | `pip/pip-90.md` | Adds `FeatureFlags.supports_broker_entry_metadata`; consumer parses `0x0e02` framed `BrokerEntryMetadata` before standard frame. |
| PIP-91 | Separate lookup timeout from operation timeout | N | `pip/pip-91.md` | Client-config only. |
| PIP-96 | Message payload processor for Pulsar client | N | `pip/pip-96.md` | Client SPI hook; no protocol change. |
| PIP-105 | Pluggable entry filter in Dispatcher | N | `pip/pip-105.md` | Server-side only. |
| PIP-107 | Chunk message ID | Y | `pip/pip-107.md` | Adds `MessageIdData.first_chunk_message_id` so seeking by chunked msg-id returns the first chunk, not the last. |
| PIP-119 | Consistent hashing by default on KeyShared | N | `pip/pip-119.md` | Server default change; client may still advertise `KeySharedMode`. |
| PIP-121 | Pulsar cluster level auto failover | Y* | `pip/pip-121.md` | Client SPI: `AutoClusterFailover` / `ControlledClusterFailover`. Wire-neutral but reconnect-driver code. |
| PIP-124 | Init subscription before sending to DLQ | Y | `pip/pip-124.md` | Adds an internal Subscribe before producer-publish to DLQ so retention applies. |
| PIP-131 | Resolve produce chunk messages failed when topic-level maxMessageSize is set | Y | `pip/pip-131.md` | Client must bypass topic-level limit when chunking; affects when client decides to chunk. |
| PIP-137 | Pulsar Client Shared State API | N | `pip/pip-137.md` | Client API surface (table view). |
| PIP-145 | Pulsar Regex Subscription improvements (topic-list watcher) | Y | `pip/pip-145.md` (file absent in this snapshot, but command exists at proto:1229-1232) | Adds `WATCH_TOPIC_LIST`, `WATCH_TOPIC_LIST_SUCCESS`, `WATCH_TOPIC_UPDATE`, `WATCH_TOPIC_LIST_CLOSE`; broker streams topic-list diffs. |
| PIP-160 | Transactions efficiency (aggregation) | N | `pip/pip-160.md` | Server-side TC optimization. |
| PIP-180 | Shadow Topic | Y | `pip/pip-180.md` | Read-only topic ownership; `CommandSubscribe` carries shadow-source. |
| PIP-186 | Two phase deletion protocol | N | `pip/pip-186.md` | Server-internal lifecycle. |
| PIP-188 | Cluster migration / Blue-Green | Y | `pip/pip-188.md` | Adds `TOPIC_MIGRATED` command (proto:1234) so broker can redirect client to new cluster mid-session. |
| PIP-264 | OpenTelemetry metrics | N | `pip/pip-264.md` | Broker/client metrics surface; no wire change. |
| PIP-282 | Key_Shared subscription initial position support | Y | `pip/pip-282.md` | Adds initial-position semantics for Key_Shared so first attach can replay backlog. |
| PIP-292 | (token refresh-related) | Y | `pip/pip-292.md` | Use `AUTH_CHALLENGE` for ongoing token refresh; client must implement refresh path. |
| PIP-296 | `getLastMessageIds` API for Reader | Y | `pip/pip-296.md` | Extends `GET_LAST_MESSAGE_ID_RESPONSE` so partitioned readers can retrieve all partitions' last IDs in one call. |
| PIP-313 | Force unsubscribe from consumer API | Y | `pip/pip-313.md` | Extends `CommandUnsubscribe` with `force` flag. |
| PIP-337 | SSL Factory Plugin | N | `pip/pip-337.md` | Java SPI; client-side TLS abstraction only. |
| PIP-344 | Correct `getPartitionsForTopic` semantics | Y | `pip/pip-344.md` | `CommandPartitionedTopicMetadata` gains a "create-if-missing" toggle (linked to `FeatureFlags.supports_get_partitioned_metadata_without_auto_creation`). |
| PIP-359 | Custom message listener executor per subscription | N | `pip/pip-359.md` | Client API only. |
| PIP-379 | Key_Shared Draining Hashes for Improved Message Ordering | Y | `pip/pip-379.md` | Broker-driven hash draining; client must tolerate the new dispatch semantics. |
| PIP-389 | Producer config compressMinMsgBodySize | N | `pip/pip-389.md` | Client policy. |
| PIP-391 | Improve Batch Messages Acknowledgment | Y | `pip/pip-391.md` | Extends per-batch ACK with finer-grained state vs PIP-54. |
| PIP-409 | Producer configuration for retry/DLQ producer | N | `pip/pip-409.md` | Client API only. |
| PIP-415 | Get message ID by index | Y | `pip/pip-415.md` | Adds a new lookup-style command to fetch a MessageIdData by entry index. |
| PIP-421 | Java 17 minimum for Java client | N | `pip/pip-421.md` | Client JVM only — informs our MSRV thinking (Java 17 ≈ "established stable") but no protocol effect. |
| PIP-460 | Scalable Topics (Topics v5) | Y | `pip/pip-460.md` | Introduces `SCALABLE_TOPIC_*` BaseCommands (proto:1236-1246); not yet released. Watch list. |
| PIP-466 | New Java Client API (V5) with Scalable Topic Support | Y | `pip/pip-466.md` | Client surface for PIP-460. Defer for v0. |

**Quasar v0.1.0 PIP coverage line**

MUST implement on day 1 (anything below is required for "talks to a modern Pulsar 3.x broker"):
- PIP-30 (`AUTH_CHALLENGE`/`AUTH_RESPONSE` — needed for token refresh).
- PIP-37 + PIP-107 + PIP-131 (chunking, including chunk-message-id and oversized-topic edge case).
- PIP-34 + PIP-119 + PIP-282 + PIP-379 (Key_Shared full surface — clients of modern brokers can't ignore consistent hashing / draining hashes).
- PIP-54 + PIP-391 (batch-index ACK; ACK_RESPONSE).
- PIP-22 + PIP-58 + PIP-124 + PIP-409 (DLQ + retry topic — heavily used in production; client-driven).
- PIP-26 (delayed delivery — pure metadata field).
- PIP-68 (exclusive producer; broker rejects otherwise and clients need to surface the error correctly).
- PIP-90 (broker entry metadata frame detection — opt-out is fine, but consumer **must** detect and skip the `0x0e02` envelope or it will mis-parse).
- PIP-145 (topic-list watcher — required for regex subscriptions, which is table-stakes).
- PIP-188 (TOPIC_MIGRATED — client must reconnect gracefully).
- PIP-296 (getLastMessageIds for partitioned readers — small surface, used by Pulsar Functions tests).
- PIP-313 (force unsubscribe — tiny add).
- PIP-344 (don't auto-create on getPartitionsForTopic — feature-flag aware).

Defer for v0.1.0 (post-v0.x; flag clearly in the API as "not yet supported"):
- PIP-4 (end-to-end encryption — significant crypto integration, mirrors `MessageCryptoBc`).
- PIP-31 (transactions — large surface; ship after producer + consumer + lookup are stable).
- PIP-33 (replicated subscriptions — opt-in, niche).
- PIP-180 (shadow topic — niche).
- PIP-415 (get message ID by index — recent, niche).
- PIP-460 + PIP-466 (scalable topics — too new; design still in flight).
- PIP-121 (cluster failover — schedule after core reconnect works).

## 6. moonpool ecosystem

Source: `gh api repos/PierreZ/moonpool/contents/Cargo.toml` (workspace at v0.6.0, edition 2024, Apache-2.0, `keywords = ["simulation","testing","distributed-systems","deterministic","chaos"]`) and `gh api repos/PierreZ/moonpool/contents/README.md`.

**Crates** (all v0.6.0, all updated 2026-03-28, all Apache-2.0):

| Crate | Recent dl | Purpose | URL |
|---|---|---|---|
| `moonpool` | 83 | Facade, re-exports everything | https://crates.io/crates/moonpool |
| `moonpool-core` | 240 | Provider trait abstractions (`TimeProvider`, `NetworkProvider`, `TaskProvider`, `RandomProvider`, `StorageProvider`) + `UID`, `Endpoint`, `NetworkAddress` | https://crates.io/crates/moonpool-core |
| `moonpool-sim` | 193 | Simulation engine, chaos testing (`buggify!`), assertions (`assert_always!`, `assert_sometimes!`) | https://crates.io/crates/moonpool-sim |
| `moonpool-transport` | 134 | FDB-style transport: RPC, peer connections (auto-reconnect+backoff), `NetTransport`, **its own length-prefixed CRC32C wire format** | https://crates.io/crates/moonpool-transport |
| `moonpool-transport-derive` | 178 | Proc-macro `#[service]` for RPC trait codegen | https://crates.io/crates/moonpool-transport-derive |
| `moonpool-explorer` | 218 | Fork-based multiverse exploration for sim | https://crates.io/crates/moonpool-explorer |

Repo (`gh api repos/PierreZ/moonpool`): https://github.com/PierreZ/moonpool. The README labels the project "**hobby-grade project under active development**". Workspace also contains `moonpool-sim-examples`, `moonpool-transport-sim`, `xtask` (not published).

**Runtime model**: provider-pattern. Application code depends on *traits* in `moonpool-core` (`TimeProvider`, `NetworkProvider`, `TaskProvider`, `RandomProvider`, `StorageProvider`). Production: a `TokioProviders` bundle in `moonpool-transport`. Tests: a deterministic sim bundle in `moonpool-sim`. Inspired by FoundationDB's testing approach and Antithesis (README — https://github.com/PierreZ/moonpool#readme).

**Networking primitives**: `moonpool-core::NetworkProvider` abstracts "creating network connections and listeners"; the actual stream surface is custom (not `tokio::AsyncRead`/`AsyncWrite`). `TokioNetworkProvider` is the production impl. **TLS is not mentioned in any moonpool crate** — quasar will integrate `rustls` (or `tokio-rustls` in the moonpool engine) independently.

**Sans-io fit**: `moonpool-core` is the right adapter point for a sans-io quasar core — its `NetworkProvider` + `TimeProvider` give us the two things a sans-io engine needs (bytes in/out, a clock for timeouts/keepalive). `moonpool-transport`'s wire format (length-prefixed + CRC32C packets) is **incompatible with Pulsar's wire format** (Pulsar has its own framing — see §2), so we use `moonpool-core` directly and bypass `moonpool-transport`'s NetTransport/RPC layer. The deterministic sim becomes our gold-standard unit harness: drive `quasar-proto::Connection` under `moonpool-sim` with fault injection and deterministic seeds.

**MSRV / edition**: edition = "2024" (Rust 1.85+; very recent). No explicit MSRV declared. Async-trait is used (provider traits are `async fn` in `async_trait`).

**Maturity verdict**:
- Strengths: active (last release 2026-03-28), Apache-2.0, author owns the FoundationDB-Rust client and has deep distributed-systems pedigree (`PierreZ` → see `fdb-etcd`, `circus`, `learn-dist-sys`, `kafka-tutorial` repos at https://github.com/PierreZ?tab=repositories).
- Risks: "hobby-grade" self-label; v0.6.0 with no API stability commitment; no TLS; bespoke async style (custom providers + async-trait) that does not interop with bare `tokio::AsyncRead` ecosystem; documentation is thin (https://docs.rs/moonpool-core).
- **Conclusion**: usable as an *adapter* behind a sans-io core, but **must not become a load-bearing dependency of the protocol layer itself**. The sans-io split lets us ship a tokio engine first (battle-tested + TLS via tokio-rustls), then add a moonpool engine for deterministic simulation testing of the same `quasar-proto`. If moonpool's API churns (likely, at 0.x), only the moonpool engine crate breaks — the rest of quasar is untouched.

## 7. Sans-io references

For each: URL + role + the API surface we'll mirror in `quasar-proto`.

**`quinn-proto`** (https://docs.rs/quinn-proto). The QUIC state machine; `quinn` is the tokio engine on top. Verified signatures via WebFetch:
```rust
pub fn poll_transmit(&mut self, now: Instant, max_datagrams: usize, buf: &mut Vec<u8>) -> Option<Transmit>
pub fn handle_event(&mut self, event: ConnectionEvent)
pub fn poll_endpoint_events(&mut self) -> Option<EndpointEvent>
pub fn poll_timeout(&mut self) -> Option<Instant>
pub fn handle_timeout(&mut self, now: Instant)
pub fn poll(&mut self) -> Option<Event>
```
Canonical sans-io shape: bytes in via `handle_event(Datagram)`, events out via `poll()`, bytes out via `poll_transmit()`, timers via `poll_timeout`/`handle_timeout`.

**`rustls`** (https://docs.rs/rustls). The TLS handshake state machine; users own the socket. Key methods on `ClientConnection`:
```rust
pub fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize>
pub fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize>
pub fn process_new_packets(&mut self) -> Result<IoState, Error>
pub fn wants_read(&self) -> bool
pub fn wants_write(&self) -> bool
```
Read/write split is exactly what we need at the byte boundary of `quasar-proto`. `rustls` is also Pulsar's TLS for free in the moonpool/tokio engines.

**`h2`** (https://docs.rs/h2). HTTP/2. Less aggressively sans-io (top-level `Connection` is async), but its internal `Codec` module decodes frames in pure Rust. Useful as a flow-control prior-art reference (Pulsar's `FLOW` permit logic is conceptually similar to HTTP/2 windows). Public types: `Connection`, `SendStream`, `RecvStream`, `FlowControl`, `PingPong`, `Ping`, `Pong`, `Error`, `Reason`, `StreamId`.

**`hickory-proto`** (https://docs.rs/hickory-proto). DNS wire format as a pure encode/decode crate. Modules: `op` (`Message`, `Query`, `UpdateMessage`), `rr` (`Name`, `Record`, `RData`), `serialize` (binary + txt codecs). Relevant as a "pure protocol crate" decomposition pattern — small dedicated modules per concern.

**`russh`** (https://docs.rs/russh). SSH 2.0 client/server. Provides a `Session` state machine and pluggable transport, illustrating how a multiplexed-channel sans-io protocol (Pulsar's producers + consumers share a single connection like SSH channels) maps to an event/poll API.

**`quasar-proto` API sketch** (target):
```rust
pub struct Connection { /* … */ }

impl Connection {
    pub fn new(config: ConnectionConfig) -> Self;
    pub fn handle_bytes(&mut self, now: Instant, bytes: &[u8]);
    pub fn poll_transmit(&mut self, buf: &mut Vec<u8>) -> Option<usize>;
    pub fn poll_event(&mut self) -> Option<ConnectionEvent>;
    pub fn poll_timeout(&self) -> Option<Instant>;
    pub fn handle_timeout(&mut self, now: Instant);

    // High-level orchestration (still sans-io):
    pub fn open_producer(&mut self, req: OpenProducerRequest) -> ProducerHandle;
    pub fn subscribe(&mut self, req: SubscribeRequest) -> ConsumerHandle;
    pub fn send(&mut self, h: ProducerHandle, msg: OutgoingMessage);
    pub fn ack(&mut self, h: ConsumerHandle, ack: Ack);
    // … etc.
}
```
`ConnectionEvent` is the union of: `Connected`, `ProducerReady`, `Subscribed`, `Message`, `SendReceipt`, `SendError`, `AckResponse`, `Reconnect`, `Closed`, `AuthChallenge`, etc.

## 8. Existing Rust Pulsar drivers

**`pulsar-rs`** — https://github.com/streamnative/pulsar-rs, crates.io: `pulsar` v6.7.2 (2026-03-30, 1.86M total downloads, 276k recent). Architecture from README: tokio + async-std dual-runtime via a configurable executor abstraction; URL-based connection (`pulsar://`, `pulsar+ssl://`); multi-topic regex consumer; TLS; auto-reconnect with backoff; LZ4/zlib/zstd/snappy compression; tracing. License: MIT/Apache-2.0 dual. Last push 2026-04-11 (active). 407 stars, 142 forks, 74 open issues.

Notable: **Florentin Dubois (`@FlorentinDUBOIS`) is a listed maintainer** of pulsar-rs (README maintainers list). Quasar is therefore a deliberate re-architecture from an existing maintainer, not a competing fork — the planner should note this for license attribution and to avoid duplicating effort on features pulsar-rs already covers well.

Quasar improvements over pulsar-rs (target):
- **Sans-io split**: pulsar-rs is async-first (top-to-bottom tokio/async-std), no pure protocol crate. Quasar will publish `quasar-proto` separately, making the codec consumable by anyone (sync apps, embedded, FFI, fuzz harnesses).
- **Deterministic simulation testing** via `moonpool-sim`: pulsar-rs has unit tests, but no FoundationDB-style fault-injection harness.
- **Schema parity gap closure**: pulsar-rs supports bytes + simple Avro/Json; quasar plans full parity (KeyValue, AutoConsume, AutoProduce, Protobuf-native, schema-registry GetSchema/GetOrCreateSchema flow).
- **Chunking, key_shared (full incl. PIP-282 / PIP-379), batch-index acks, dead-letter, retry topic, transactions, TopicListWatcher, broker-entry-metadata detection, TopicMigrated handling, ProducerAccessMode** — items present in Java but historically thin or missing in pulsar-rs (verify against `pulsar-rs/src/` before claiming "missing").
- **Modern Rust**: edition 2024, MSRV around 1.85, dropping `async-trait` where possible (native `async fn` in traits), `&[u8]` + `bytes::Bytes` everywhere, no `Box<dyn Future>` in hot paths.

Other Rust Pulsar attempts (sampled):
- `pulsar-client` on crates.io — not the StreamNative one; check before naming clashes.
- No other actively maintained Pulsar-in-Rust drivers of note (search `github.com/search?q=pulsar+client+rust&type=repositories` returns mostly forks of pulsar-rs).

## 9. Quasar repo state

Verified locally (`git -C /home/florentin/Sources/github.com/me/quasar status`):

- Path: `/home/florentin/Sources/github.com/me/quasar`
- Initialized git repo on branch `main`, **zero commits**.
- No `Cargo.toml`, no `src/`, no LICENSE, no README, no CI config.
- Bootstrap will need to: pick a name (see below), choose license, create Cargo workspace, set up CI, write README, push to GitHub.
- Worktrunk note: since there are no commits and no `main` to protect, the **first commit goes directly to `main` without a worktree** (the pre-edit hook protects existing default-branch state; an empty repo has none). All subsequent feature work uses `wt switch --create <branch> -y` per `~/.claude/CLAUDE.md`.

**Crate-name availability** (`curl https://crates.io/api/v1/crates/quasar`):
- `quasar` v0.0.1 is **TAKEN** since 2017-01-28 by `anowell`: "An experimental rust-to-{wasm,asmjs} frontend framework". 8 recent downloads — effectively abandoned but not yanked. We **cannot publish** under `quasar`.
- Fallback names to validate with the user: `quasar-client`, `quasar-pulsar`, `pulsar-quasar`, `apache-pulsar-quasar`, or a new project name entirely (e.g., `nebula`, `magnetar`, `corvus`). Internal repo name can stay `quasar`; published crate name(s) will differ.

## 10. Guidelines & constraints (from ~/.claude/CLAUDE.md)

The planner must obey:

- **Code style**: `ToOwned::to_owned()` over `Clone::clone()` (ownership intent clarity).
- **Worktree-first**: any code change post-bootstrap goes through `wt switch --create <branch> -y` → make changes → `wt step diff --stat` → `wt merge -y` (after user OK). Edits on `main`/`master`/`trunk`/`develop` are blocked by `~/.claude/hooks/pre-edit-default-branch.sh`.
- **Commits**: conventional commits (`feat(scope): …`, `fix(scope): …`, `refactor(scope): …`, `chore(scope): …`, `docs(scope): …`); always `git commit -s -S` (signed-off + GPG-signed).
- **No "Generated by Claude" trailers** anywhere — commits, PR titles/descriptions, MR titles/descriptions, issue comments. Florentin's GPG signature is the sole authorship marker.
- **Validation chain (Rust)**: `cargo build --all-features && cargo clippy --all-features && cargo +nightly fmt`. Hook `~/.claude/hooks/post-edit-format.sh` auto-runs `cargo fmt` on edited files; **clippy + tests stay manual** and must pass before a task is "done".
- **Docs are code**: every behavior/API/architecture change updates its docs in the *same* changeset. Stale docs = bugs.
- **Branch naming**: `feat/<scope>`, `fix/<scope>`, `refactor/<scope>`, `chore/<scope>`, `docs/<scope>`.
- **Approval gates**: pushes, MR/PR creation, branch deletion, destructive git ops — all require explicit user OK.
- **Sources tree**: project clones live under `/home/florentin/Sources/<host>/<org>/<repo>`.

Pulsar-specific (no project `GUIDELINES.md` yet in `me/quasar` — empty repo). The planner should create `GUIDELINES.md` in the repo capturing protocol-correctness rules (CRC32C verify, framing magic guard, request-id correlation, no panics in `quasar-proto`, etc.).

## 11. Open questions for the user

These are the ambiguities the planner cannot resolve alone. The plan will assume defaults but flag each as approval-gated:

1. **Crate name on crates.io** — `quasar` is taken (anowell, 2017, abandoned). Pick one of: `quasar-client` / `quasar-pulsar` / `pulsar-quasar` / `apache-pulsar-quasar` / a new project name (e.g. `magnetar`, `nebula`, `corvus`). Recommended default: `quasar-pulsar` (search-engine friendly, makes intent obvious).
2. **License** — Apache-2.0 only (matches Pulsar upstream) or MIT/Apache-2.0 dual (matches Rust ecosystem default + pulsar-rs)?
3. **Engine crates from day 1** — only `quasar-runtime-moonpool`, or also `quasar-runtime-tokio` (battle-tested, immediate TLS) and/or `quasar-runtime-sim` (moonpool-sim wrapper for tests)?
4. **Crate split granularity** — proposed: `quasar-proto` (sans-io) + `quasar` (top-level façade re-exports) + `quasar-runtime-{tokio,moonpool}` (engines) + `quasar-admin` (REST). Confirm vs. single-crate-with-features.
5. **Minimum broker version** supported — Pulsar 3.0 LTS, 3.3 latest, or older (2.10)?
6. **Schema scope for v0.1.0** — (a) bytes + String + Json + raw Avro/Protobuf (no registry), (b) full Java parity including KeyValue + AutoConsume + AutoProduce + schema registry round-trips? Default proposal: (a) for v0.1.0, (b) for v0.2.0.
7. **Transactions** in v0.1.0 or deferred? Default proposal: defer to v0.2.0 (PIP-31 surface is large, broker-side TC complex).
8. **Admin REST client** (`quasar-admin`) in v0.1.0 or post-v0.1.0? Default proposal: separate crate, started after producer/consumer parity.
9. **CLI binary** like pulsar-rs ships none — do we want one (`quasar-cli` for ad-hoc produce/consume/inspect)? Default proposal: library-only first, CLI in v0.2.0.
10. **Auth scope for v0.1.0** — (a) token + TLS (mTLS) only, (b) add OAuth2 (ClientCredentialsFlow), (c) full set incl. SASL + Athenz? Default proposal: (a) for v0.1.0, OAuth2 for v0.2.0, SASL/Athenz behind feature flags later.
11. **Encryption (PIP-4)** in v0.1.0 or deferred? Default proposal: deferred — large crypto surface, mirror `MessageCryptoBc` later.
12. **E2e CI broker provisioning** — `testcontainers-rs` driving `apachepulsar/pulsar:3.0.x`, or `docker compose up` invoked by `xtask`, or both? Default proposal: testcontainers-rs as the primary harness, with a fallback `docker-compose.yml` for local dev.
13. **Repo hosting** — push to `github.com/me/quasar` (personal) or `github.com/CleverCloud/quasar` (org) or a separate Clever Cloud OSS org? Affects CI infra and OWNERS.
14. **moonpool risk acceptance** — moonpool labels itself "hobby-grade". OK to take it as the *primary* engine name, or should `quasar-runtime-tokio` be the public-facing default and moonpool be opt-in? Default proposal: tokio is the public default; moonpool is the *testing* engine (deterministic sim).
15. **Coexistence with pulsar-rs** — Florentin is a pulsar-rs maintainer. Plan needs to clarify: does quasar replace pulsar-rs for Florentin, exist alongside it, or aim to be eventually upstreamed? Default proposal: ship as a separate project; revisit upstreaming after v0.1.0 ships.
