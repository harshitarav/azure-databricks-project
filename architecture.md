# Azure Databricks enterprise retail data platform architecture

**Revision A4.2 — 30 October 2026 — one DEV environment**

**Status: proposed architecture for review; implementation is stopped.**  
**Destination: harshitarav/azure-databricks-project, verified default branch main, root architecture.md.**

This document applies targeted corrections to A4.1 in place, supersedes only the decisions identified below, and retains its verified source evidence, business products and justified infrastructure. The earlier A2 audit remains as traceable history. It preserves eight logical integrations and four genuine interfaces: PostgreSQL, SFTP, REST and Kafka. Large ingestion, validation, joins, reconciliation and transformations use distributed Spark workers. Small serialized control work does not require a large Spark cluster.

Only this Markdown document is authorized for updating and recording. Nothing here approves implementation, fault execution, paid resources, deployment or production readiness. Original files and existing implementation artifacts remain in their current locations. Their existence does not establish that their runtime behavior satisfies this architecture.

**Evidence labels:** VERIFIED = read in the supplied authorities, retained evidence or official documentation; SELECTED = chosen project/business policy; PROPOSED AMENDMENT = an explicit material change awaiting architecture approval; QUALIFICATION REQUIRED = execution, funding or external fact not established here. Every future runtime test is NOT EXECUTED. Architecture completeness, implementation completeness, representative tests, full-source qualification and production-style recovery are separate statuses.

## 0. A4.1 → A4.2 targeted corrections

This revision preserves the eleven source authorities, eighty source-specification slots, eight independent integrations, PostgreSQL/API/SCD2/metadata/controls and all prior physical tables. It implements the user's latest source-specific decisions; only architecture.md changes. No implementation or proposed test has run.

### 0.1 Correction and contract-deviation register

| ID | Existing A4.1 → revised decision / reason | Authority and reliability impact | Complexity/resources/cost | Qualification before its success claim |
|---|---|---|---|---|
| A42-01 / SP-01 | Remove metadata-only binaryFile/custom Python Blob SDK SFTP readers entirely. Actual cloudFiles CSV/JSON/Parquet/text parses native source deliveries. | Source-publication identity overlay R3/3.0.0 to D02/D03/D05/D06: ordinals carried by publisher or joinable ordered TOA5 metadata, not reconstructed from Spark row order. Original seeds/schema versions stay evidence; source payload changes are explicit. | No receiver download/parser framework, executor SDK credential propagation, receiver Parquet layer or new staging table. | Publisher identity/image/multiset verification; native parser/checkpoint/corrupt/rescue tests (§7). |
| A42-02 | Universal SFTP callback MERGE → checkpointed native Delta append for all five, with only stateless technical projections/TOA5 transforms before sink. | Source manifest/path/revision admission prevents retransmission duplication; source_deliveries binds exact complete delivery certificates. Silver handles logical revisions/idempotency. No file/whole-delivery marker protocol in SFTP Bronze. | Fewer sink paths; no new table/control authority. | Partial file batches, uncertain native commit, empty delivery, isolated checkpoint-loss recovery/ACK repair. |
| A42-03 | Mostly registry-level evolution → actual native Auto Loader modes and twenty publisher-to-consumer experiments. | Physical Bronze evolution does not approve business semantics. CSV/JSON/Parquet have typed modes; text has fixed schema; OFF VARIANT payload is not typed-field evolution. | Isolated demonstration targets/checkpoints only when needed; normal 33-table inventory unchanged by temporary tests. | DBR16.4+ widening preview, Delta feature/readers, preserved schema/checkpoints and controlled restart (§7.8/16). |
| A42-04 | Routing incorrectly says REV42/DAT41/API38 → exact source catalogue: REV36/DAT40/OPR45. | Original IDs/predicates unchanged. Sum36+36+36+36+40+45+46=275; OFF30project mappings separate. D05 hash formatting is outside this correction. | Documentation correction, no invented/removed rule. | Exact original R07 ID sets and routing/summary agreement (§14.2). |
| A42-05 | REES46 routing5 minute discovery → user-selected10 minute ADF SFTP discovery throughout. | D02/D09 source calendar and R/B/S/G/consumer deadlines remain;10 minute phase consumes part of Raw60 minute budget. Other sources retain their own clocks. | Fewer ADF checks; no universal10 minute clock. | Missed/duplicate tick catchup, source calendar and actual full pipeline latency. |
| A42-06 / KSG-01 | Kafka Bronze-only/operational views → independent Kafka Bronze/Silver/Gold. | Project downstream extension preserves D11 producer-to-Bronze exactly; separate source generations/products from SFTP, no default UNION/combined population. | Add only silver.rees46_kafka_events and gold.rees46_kafka_activity_daily:10B/10S/8G/5C=33. | Logical versus physical duplicates, late/out-of-order events, schema/DQ, exact stage reconciliation and replay (§10/15). |
| A42-07 | Reconsider storage topology → keep two accounts, same subscription/DEV. | Supplier SFTP and lake account remain distinct. No per-source accounts or identity proliferation; API WORM stays isolated from SFTP paths. | Reuses selected accounts/network/Key Vault. | Actual connector/permissions/namespace compatibility, not another topology decision. |
| A42-08 / DEV-RET-01 | Mandatory365day SFTP Raw WORM → temporary active-project retention/explicit closeout; no SFTP WORM. | Proposed DEV deviation for D02–D06R09 and corresponding SFTP payload-bearing target retention; original enterprise365/90/400day requirements remain recorded, not falsely satisfied. D10 untouched. | Avoids year-long SFTP lock/storage liability; retain unresolved/replay/evidence dependencies before deletion. | Closeout authorization, dependency inventory, completed recovery tests and explicit loss-of-replay declaration (§12). |
| A42-09 / DEV-KAF-01 | Managed Standard as default → temporary real single-broker KRaft + Schema Registry on shared source VM. | Explicit DEV RF1/minISR1/internal-topic deviation from enterprise RF3/minISR2/HA. Preserve transactions/fencing/read_committed/native append; no replica-failover or host-loss zero-RPO claim. | No default additional VM/managed eCKU charge; measured shared host and persistent disk budget. | Exact broker/Registry/runtime compatibility, process restart/abort/fencing, resource contention and cold-state restore (§10). |
| A42-10 | Planned4 worker historical sessions → start modest Jobs compute;2workers for distributed qualification, optional scale only after evidence. | Premium PAYG is the user's accepted billing direction, not verified credit/entitlement or provisioning approval. | No speculative4/8 worker allocation, idle interactive cluster or always-on warehouse; serialize workloads. | Actual task/utilization/spill/lag/DBU/VM quote and budget/session gates (§11/19). |

SP-01, KSG-01, DEV-RET-01 and DEV-KAF-01 are explicit proposed implementation/DEV contract overlays. They do not edit the original Word/JSON authorities or certify compliance with superseded enterprise guarantees. Retain A4.1 AM-13 SCD effective-time and AM-14 representative-scope policies unless specifically superseded here.

### 0.2 Four distinct ingestion patterns and source independence

| Integration pattern | Acquisition/Bronze | Downstream / scheduler / authority |
|---|---|---|
| PostgreSQL | Real seeded database; sealed article journal plus native Debezium snapshot/WAL/Kafka; actual managed Bronze | Source journal/Connect/native checkpoints; ADF onboarding/articles/finite recovery, native CDC lifecycle; SCD2/current facts and G01/G02. |
| Five SFTP integrations | Simulated native-format supplier → Azure Blob SFTP → ADF Binary Copy → verified native accepted landing → native Auto Loader → Bronze append | ADF finite workflows, persistent source-specific file/schema checkpoints; logical completion/ACK in source_deliveries; source-specific Silver/Gold. |
| REST API | Actual REST → D10 serialized Python → immutable HTTP evidence/one ledger → accepted receipt Auto Loader/guarded window Bronze | ADF15 minute acquisition and independent5 minute receipt/business admission; distributed Silver/G06; API is never SFTP. |
| Independent REES46 Kafka | Independent transactional producer → actual Kafka/Registry → native Structured Streaming/append Bronze | Native source/progress and query checkpoints; Kafka-owned Silver/G08 streaming analytics; ADF lifecycle/monitor/recovery, never per-record Copy/Auto Loader. |

REES46 SFTP batch/incremental analytics and REES46 Kafka streaming analytics intentionally share seed history but are separate datasource implementations. Similar measures remain separate products/lineage/publication/readiness. A comparison view may validate aligned source scopes; it never represents a combined distinct population.

All persisted structured processing/runtime control tables are managed Delta. Native seeds/source deliveries, manifests/READY/ACK/ordinal metadata, API evidence/receipts, configuration releases and native checkpoints remain genuine artifacts. No standalone receiver Parquet processing dataset or sixth generic control table is selected.

## 1. Retained A2 audit and A3 history with A4 resolutions

A2 was recovered in full from the existing conversation “Design source simulation and ingest (2),” architecture revision A2, 30 October 2026. The older local ARCHITECTURE.md dated 29 October and the identical older Downloads/version1 architecture copies are supporting history, not A2. The superseded repository/path mentioned in A2 is not this document's destination.

Severity P0 blocks correctness or safe activation; P1 blocks the stated operational objective; P2 is an improvement or deferred qualification. KEEP, CORRECT, SIMPLIFY, ADD and DEFER describe the audit outcome, not execution status.

| ID | Severity / class | A2 concern and evidence | Selected resolution | Sections |
|---|---|---|---|---|
| A01 | P0 CORRECT | A2 assumes OpenSSH SFTP on a source VM; user requires Azure Blob SFTP | Azure-managed supplier endpoint, five logical folders, actual ADF SFTP source connector | 4, 7 |
| A02 | P0 CORRECT | A2 combines SFTP and protected immutable API evidence in one account; Microsoft feature matrix marks account-level SFTP incompatible with container WORM | Two storage accounts: SFTP supplier account and HNS lake account with SFTP disabled. Keep the existing API immutability guarantee | 4, 7, 12 |
| A03 | P1 SIMPLIFY | A2 assumes a 1 TiB source disk without a working-set calculation | Shared execution host;128 GiB representative starting tier, measured capacity admission and30%free reserve (§4); no fixed512 GiB default | 4, 5 |
| A04 | P1 CORRECT | A2 puts PostgreSQL, supplier workloads, Connect and the Kafka producer on one host | Select managed PostgreSQL Flexible Server; share one small execution VM for publishers/producer/Connect and the selected temporary KRaft/Registry; managed PostgreSQL remains separate | 4, 6 |
| A05 | P1 KEEP with justification | VNet injection/NAT are not universal requirements, but private JDBC to the actual PostgreSQL source must work | Keep one small VNet and explicit egress for this selected private database topology; compare less costly public alternatives explicitly | 4 |
| A06 | P1 CORRECT | E16 drivers/four E16 workers for every history and E8 streams are unmeasured starting assumptions | A4.2 starts modest Jobs compute/two-worker distributed qualification; no default4/8 worker history allocation; scale only after measured bottlenecks | 11 |
| A07 | P1 CORRECT | A2 API control adds two workers despite the approved API decision specifying single-node compute | Restore separate single-node acquisition and receipt Jobs; preserve ownership and independence | 8, 11 |
| A08 | P0 KEEP | Newer API decision separates REST acceptance from Bronze and rejects export bootstrap | Keep REST-only Python, one guarded Delta ledger, immutable evidence, accepted receipts and Auto Loader | 8 |
| A09 | P0 CORRECT in A4 | A3 removed the required Auto Loader stage after SFTP acquisition | A4 restored Auto Loader for all five sources; A4.1 selects native source-specific query/sink scopes and persistent checkpoints (§7), superseding universal Parquet/markers | 7, 9, 13 |
| A10 | P1 CORRECT | A2 mostly assigns ADF SFTP duties and leaves other enterprise responsibilities implicit | ADF owns file acquisition, article scheduling, finite onboarding/recovery/reconciliation and batch dependencies; native Jobs own continuous capture; ADF owns the approved API acquisition/receipt schedules | 9 |
| A11 | P0 CORRECT | Timed-out ADF activity is not proof a Databricks run stopped | Deterministic Jobs run-now token, run-ID lookup, actual terminal evidence, then controlled repair/new execution | 9, 16 |
| A12 | P0 CORRECT | Separate Delta table writes cannot provide a single ACID publication | Same-table completion markers where needed; bounded stage receipts and frozen input versions; no global transaction claim | 13–15 |
| A13 | P0 CORRECT | A2's “305 source rules” suggests 30 executable OFF predicates | Preserve 275 identified rules across seven contracts plus OFF's 30 prose categories. New OFF executable definitions need an explicit reviewed mapping | 3, 14 |
| A14 | P0 KEEP | REES46 and review payload repetition represents original occurrences | Never use content-hash deduplication for business occurrence identity | 3, 5, 14–15 |
| A15 | P0 KEEP | H&M R3 records six exponent prices and 243,127 values needing more than 18 fractional digits | Retain lexical amount + unconstrained source NUMERIC; exact fit/round-trip before DECIMAL(38,21) projection | 6, 14–15 |
| A16 | P0 ADD | A2 lacks fully specified Gold grains, KPIs, correction and publication behavior | Seven original Gold products plus explicitly required independent Kafka G08 and business views; dimension and funnel definitions below | 13, 15 |
| A17 | P1 CORRECT | Five-minute discovery nearly exhausts a five-minute READY-to-Raw target | One-minute active polling for OFF, Amazon metadata and Campbell; explicitly amend their previous five-minute choice and Campbell Event Grid choice | 9, 17 |
| A18 | P1 CORRECT | 30-minute API acceptance + 15-minute receipt processing ignores receipt-publication delay | Allocate an explicit publication budget and a combined latency inequality; monitor acceptance-to-receipt debt | 8, 17 |
| A19 | P0 KEEP | Two REES46 topics, transaction/progress semantics, raw-preserving inspection and native Bronze recovery are accepted | Preserve R2 exactly; H&M/Connect topics are distinct native CDC state, not REES46 additions | 10 |
| A20 | P1 CORRECT | Pricing one eCKU as a service total ignores traffic, storage and possible higher SLA capacity | Price billed eCKU-hours plus all usage; do not purchase or claim the 99.99% option without its capacity requirement | 4, 19 |
| A21 | P0 ADD | H&M cross-table CDC completeness/order and native offset loss need an actual design | One Connect worker/connector, explicit internal and CDC topics, transaction metadata, baseline and transaction-apply gates | 6, 13–14 |
| A22 | P0 ADD | A2's failure table does not specify executable Spark failure demonstrations | Risk-based catalogue with isolated fixtures, observations, recovery and pass criteria | 16 |
| A23 | P1 CORRECT | Four days does not prove a full 85.2-hour stream plus setup and all-source qualification | Four-day implementation sequence, representative gates and a separate full-scale qualification window | 20–21 |
| A24 | P0 KEEP | Native checkpoints, source journals and the API ledger have different owners | Preserve their boundaries; shared controls report milestones and never replace source/native progress | 13–14 |
| A26 | P0 ADD | Article journal cadence can lag the faster purchase CDC publication | Publish exact purchase facts with explicit pending article enrichment; gate attribute-complete analytics separately | 14–17 |
| A25 | P2 DEFER | Always-on large SQL warehouses, private-endpoint mesh, HA Kafka operations and extra environments | No additional environment, control DB, custom service or permanent BI compute. Add only through a justified future decision | 4, 19–20 |

## 2. Source authorities and traceable evidence

Original contract root: `C:\Users\harsh\OneDrive\Desktop\DE PREP\AZURE PROJECT\datasources\SOURCE SIDE CONTRACT RULES`. Documents D01–D08 are in `Source profiling`; D09–D11 are in `Source frequency decision`.

All eleven supplied files were read or matched in the retained source review against the existing complete extraction and SHA-256 index. The original file hashes and retained extracted-text hashes agree with that index. That is retained document verification, not dataset re-profiling. A4.2 reuses it and checks the targeted authoritative DQ ID sets; the D05displayed-SHA formatting issue is deliberately not repaired or used as a new full-hash-pass claim. D11 is **UTF-8 Markdown with a .docx extension**, not an OOXML ZIP; its actual text was read accordingly.

| Doc | Exact authority | SHA-256 |
|---|---|---|
| D01 | POSTGRESQL_HM_ENTERPRISE_SOURCE_CONTRACT_R3.docx | dac3fb8360ee1e142282a459f80ecc29d28ea9221aad264a5b0101ec7284cd4e |
| D02 | REES46_SFTP_CSV_ENTERPRISE_SOURCE_CONTRACT_R1.docx | 71f47a8174daa9cb6a650bb7ce5d93debde6029bc910bb2d2dcdecf993e7e8ee |
| D03 | SFTP JSON.docx | 8f485571b28d714efb68afff2565650138fba133831d2c1ad38d02a757264ce3 |
| D04 | AMAZON_ELECTRONICS_SFTP_PARQUET_ENTERPRISE_SOURCE_CONTRACT_R1.docx | b4105ee9bfb21a43cdbbb3b6d86cb4c4a01bda4db49b45a7e5ad09c7a27cae83 |
| D05 | AMAZON_ELECTRONICS_REVIEWS_JSONL_ENTERPRISE_SOURCE_CONTRACT_R1.docx | 94c49156cdb5d68cd26f675d9907d56e314463b4a1bdd2d9a934c68a7862093 d |
| D06 | CAMPBELL_TOA5_DAT_ENTERPRISE_SFTP_SOURCE_CONTRACT_R1.docx | 75a3f0bd80a3e0d2001d513e7394b70ccf81abc4b6df36182eaaf9d62aa9f1b6 |
| D07 | OPEN_PRICES_API_ENTERPRISE_SOURCE_CONTRACT_R1.docx | b159b13afdf2f0d386f222b2cdd6c1dcbcb062b06c2d98bc2ce6b9907cd2ef34 |
| D08 | REES46_KAFKA_STREAMING_ENTERPRISE_SOURCE_CONTRACT_R1.docx | 17251856048c618d2d9d55c33c6e5f121be2f720c85aac20b659821016d3ff84 |
| D09 | Source frequency decision.docx | c8cb5f93c526f52a08a018ea8ece995afce508707f49f38c8338b90a3277ec61 |
| D10 | API CODEX IMPLEMENTATION DECISION.docx | 2af2702b08db1d40bd27abe67d8775debde522cf10e1e2237733204c5ff235b5 |
| D11 | KAFKA CODEX IMPLEMENTATION DECISION.docx | b600caf945db56b3ec5d387c1de5b77af501043cdc51ca20350b12fdcf0996e9 |

Precedence: this user's explicit boundaries govern; D10 supersedes conflicting older API proposals; D11 supersedes conflicting older Kafka proposals; D09 supplies downstream cadence/business objectives; otherwise D01–D08 retain their source semantics. A4.2 and retained unsuperseded A4.1/A4/A3 amendments are proposed, not silently active source-contract edits. A4-01 still supersedes A3's removal of SFTP Auto Loader; A42 native ingestion/source-publication overlays supersede A4.1 receiver SDK/callback paths and A4's receiver Parquet/marker protocol. OA-API-02 changes D10 scheduling; OA-API-03 adds only downstream distributed processing.

Existing work remains in the original workspace, including `rees46-kafka`, source contracts/evidence and `azure-databricks-data-platform`. No migration or implementation correction occurs in this phase. The inaccessible historical `source-contracts/contracts.v1.json` catalogue **has not been recovered or bytewise reconciled**. Existing extracted YAML and generated rules do not establish that it has.

### 2.1 Recovered historical catalogue and precedence reconciliation

VERIFIED on 30 October 2026: original-workspace source-contracts/contracts.v1.json exists,148243bytes, SHA256 e24c65b09ff129140eb366e3a45160acd9d411e90378d373fbc356694db74a27. Its artifact_id is enterprise-source-contracts-R3, artifact version3.0.0 despite the v1 filename;21schema entries,7 station bindings,42 rules. Removing content_sha256_without_this_field and hashing UTF-8 canonical JSON (sorted keys, compact separators, non-ASCII preserved) reproduces its declared9f9e3962bb21d7b6a029576eab77630c227cf5841482c4b20fda4d392d6479c8.

This is recovered legacy evidence, not a current complete executable catalogue. Its H&M purchase price decimal(38,18) conflicts with D01 exact lexical/NUMERIC and qualified decimal(38,21); OFF still describes a10000-row early profile instead of the later full canonical/exception evidence; REES evidence says only October fully profiled; API/Kafka v1/1.1 envelopes precede D10/D11 material R2. It lacks the complete review schema/rule family, all 275 identified current source rules and A4/A4.1 routing/receiver/SCD overlays. Its APPPROVED-style design labels do not establish runtime tests or override newer authorities.

Reconcile each legacy schema/field/rule against D01–D11 and these explicit overlays into one reviewed future metadata release; preserve original bytes/hash/history and approved older replay decoders. Do not merge its42 rules with275as if they were disjoint, silently reactivate stale gates, or claim all eleven Git categories already exist in that file. Catalogue migration/implementation is NOT EXECUTED. All eleven retained original/extracted-text hashes still match document-index.json in this review; no dataset profiling rerun.

## 3. Source-contract-to-architecture traceability

R01 discovery/business rules; R02 registry; R03 schema; R04 behavior; R05 compatibility; R06 drift; R07 DQ; R08 freshness; R09 retention/replay; R10 operational metadata. These matrices cover all 80 source-side requirement slots. Locations refer to the named requirement in the source document, not guessed rendered page numbers.

### 3.1 H&M PostgreSQL — D01 R3, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 | 105,542 articles, 1,371,980 customers, 31,788,324 purchase occurrences; verified cutover allocation; source files are not historical CDC. Initialize real PostgreSQL (§5–6). |
| R02 | Version physical PostgreSQL/catalog expectations, journal envelope, CSV identities and decoder releases in Git; inspect actual pg_catalog before activation. |
| R03 | Exact ID strings, source flags/nulls, price lexeme and unconstrained NUMERIC; native purchase occurrence ID; candidate exact DECIMAL(38,21), never DECIMAL(38,18) rounding (§6,14). |
| R04 | Baseline + transactional articles journal; customers/purchases real snapshot/WAL CDC; first-use master activation and one source day per ten elapsed minutes (§6). |
| R05 | Reviewed compatible additions; identity/required/type/CDC semantic changes need migration and material versioning; no fabricated master history. |
| R06 | Compare actual CSV, DDL, journal and Debezium envelopes with pinned release; stop incompatible apply; retain raw CDC. |
| R07 | All 36 HM-ART/HM-CUS/HM-TRX/HM-CDC/HM-JNL/HM-ALL predicates and dispositions remain authoritative; initialize, validate, reconcile and fault-test at original scope (§14). |
| R08 | Articles 15-minute polling; p99 B/S/G/consumer 30/40/50/60 minutes. CDC continuous, 5/8/12/15 minutes; operational commit clock (§17). |
| R09 | Seeds active+365 d; unacknowledged journal never purged; acknowledged images ≥90 d; WAL capacity ≥7 days measured peak; Kafka 14 d; Raw/Bronze365 d; native recovery (§12,18). |
| R10 | Source action/version/journal membership, actual WAL/transaction identity, target UUID/version and true ACK. MAX(sequence), t_dat or updated_at is never the journal authority (§6,13). |

### 3.2 REES46 SFTP CSV — D02 R1, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 | Seven months, 411,709,736 independent occurrences, 17,668,345,567 gzip bytes / 56,577,978,747 decoded bytes; accepted repeats/blanks/bursts (§5). |
| R02 | Original nine-column v1 retained as source evidence; SP-01 adds publisher ordinal to delivery schema, package manifest and approved April v2 decoder/fingerprints retained. |
| R03 | UTC event-time lexeme; exact string identifiers and price; optional brand/category/session; original payload v2 adds nullable sim_export_quality; SP-01 metadata is separately versioned. |
| R04 | Monthly event exports; November r2 full-month replacement changes ordinal108 price566.30→566.31; delayed/missing/empty/replay cases retained (§5,7). |
| R05 | Registered nullable April addition is compatible; identity/order/type changes require explicit migration. |
| R06 | Check gzip/CRC, header order, width, version and source membership; retain rejected attempt; no mergeSchema. |
| R07 | REE-001…036 remain intact; no content dedup or invented currency/product FK; source-channel separation enforced (§14). |
| R08 | Next-month first day02:00 UTC, ADF10 min; R/B/S/G60/120/180/240 min, dashboard270 min, business08:00 UTC (§17). |
| R09 | Seed365 d after run, acknowledged supplier90 d, Raw/Bronze365 d, quarantine90 d, audit400 d; same revision exact bytes; retain r1/r2 (§12,18). **Enterprise horizon; DEV-RET-01 explicitly replaces cloud SFTP payload retention/closeout in temporary DEV (§12), no365day SFTP WORM claim.** |
| R10 | Month, delivery/revision/attempt, original gzip hash + one-based logical CSV ordinal, source manifest/native part SHA, native file checkpoint, exact whole-delivery reconciliation/control state/ACK; later months cannot close a March gap. |

### 3.3 OpenFoodFacts JSONL — D03 R1, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 | 4,832,131 objects, 13,230,181,102 gzip / 83,825,990,194 decoded bytes; 4,832,069 nonblank distinct codes; 61 conflicting groups + one blank; 4,832,008 canonical keys and123 exception occurrences. |
| R02 | Git catalogue for closed delivery envelope and open publisher content, separate from publisher schema_version; no new registry service. |
| R03 | Exact code strings/zeros, full original nested JSON and unknown optional members retained; no exhaustive flattening of334,949 observed paths. |
| R04 | Immutable FULL + ten hourly journal-backed DELTAs; INSERT/UPDATE/RETIRE/DELETE; canonical keys only for keyed source mutations; no snapshot-diff CDC (§5,7). |
| R05 | Nullable sim_supplier_quality and sim_supplier_context.review_note reviewed as simulated additions; protected identity/type changes are material. |
| R06 | Open content captured; typed protected activation gated; schema99 held; tombstones explicit, absence never deletion. |
| R07 | Preserve all 30 category statements; they are not30 original executable IDs. Complete operators, scope and tolerances before activation (§14). |
| R08 | D09 FULL12/24/30/36 h to R/B/S/G, consumer40 h target/48 h deadline; hourlyDELTA; active polling amended to1 min to address5 min Raw target (§17). |
| R09 | Seed active+365 d; supplier/journal90 d after ACK, unacknowledged retained; Raw/Bronze365 d; audit400 d; RTO4 h objective only. **Enterprise horizon; DEV-RET-01 explicitly replaces cloud SFTP payload retention/closeout in temporary DEV (§12), no365day SFTP WORM claim.** |
| R10 | Source generation/snapshot, source sequence/version/transaction, exact parts/membership, predecessor, empty flag, revisions; receiver receipts separately owned. |

### 3.4 Amazon metadata Parquet — D04 R1, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 | Ten original SNAPPY shards,1,610,012 products,1,955,009,715 bytes,1,620 row groups; unique exact parent_asin, including six lowercase identifiers. |
| R02 | Git subjects full_native, delta_envelope and delta_after; native type/fingerprint/version retained; no SFTP registry service. |
| R03 |16 native fields; rating_number int64, average_rating double, price/details/bought_together strings, images/videos native structs of lists; no cast to invented price/currency (§14). |
| R04 | FULL boundary0 then ten transactional journal exports; sealed endpoints2,10,12,13,15,17,18,18,20,21; empty eighth export; preserve every committed operation. |
| R05 | V2 nullable after.sim_supplier_quality; schema99 string rating_number is breaking and held; reserialize same sealed membership for repair. |
| R06 | Footer plus full page decoding, exact nested types and compatible versions; physical footer readability alone is insufficient. |
| R07 | All AMZ-DQ-001…036 retain predicates/tolerances/dispositions, including exact FULL keyset/multiset and journal atomicity. |
| R08 | Hourly sealHH:30/publish followingHH:00; D09 stage targets; polling amended5→1 min active; source fault calendar remains visible (§17). |
| R09 | Source/Raw/decoder retention and original keyset/multiset identities unchanged; replay same operations, explicit revisions; no current-state replacement for journal history. **Enterprise horizon; DEV-RET-01 explicitly replaces cloud SFTP payload retention/closeout in temporary DEV (§12), no365day SFTP WORM claim.** |
| R10 | Source action/change/version/transaction/sequence, exact source range and after/tombstone, manifest parts, predecessor, target commit and idempotent ACK. |

### 3.5 Amazon reviews JSONL — D05 R1, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 |43,886,944 objects /22,616,233,652 original bytes; FULL43,883,164 +3,780 original occurrences over13 days;477,968 excess repeated payload occurrences retained. |
| R02 | Original/full/daily/fixture schemas retained in Git; SP-01 delivered ordinal/raw-image fields separately versioned; native JSON schema/replay compatibility. |
| R03 | Ten source fields; epoch milliseconds, boolean verified_purchase, original strings/nested images/rating/helpful_vote; no verified standalone review ID. |
| R04 | Source SHA + delivered SP-01 original global ordinal is occurrence identity; daily membership by UTC timestamp; synthetic CREATE/EDIT/RETRACT isolated and labeled. |
| R05 | Optional envelope sim_export_quality compatible after review; timestamp unit/key/nested breaking changes held. |
| R06 | Nonmonotonic timestamps are allowed; preserve unknown optional content pending typed review; bad types retain raw. |
| R07 | REV-001…036 intact; preserve two zero ratings, four negative helpful_vote values, blanks and full105,328-byte maximum observed record (§14). |
| R08 | One original day per10elapsedmin; ADF1 min active/15 min recovery-only; initial FULL and daily stage targets fromD09 (§17). |
| R09 | Original source active+365 d; exports≥90 d after ACK; Raw/Bronze365 d; stable original ordinal across replay; original history ends13Sep2023. **Enterprise horizon; DEV-RET-01 explicitly replaces cloud SFTP payload retention/closeout in temporary DEV (§12), no365day SFTP WORM claim.** |
| R10 | Exact original-day/ordinal membership, immutable revision/parts/manifest, native Auto Loader checkpoint, per-file/whole-delivery reconciliation and actual Bronze receipt before ACK. |

### 3.6 Campbell TOA5 DAT — D06 R1, cadence D09

| Requirement | Preserved source decision and architectural realization |
|---|---|
| R01 | Seven files45,438 observations; initial41,105 +190 station-days4,333; native gaps/anomaly lexemes preserved (§5). |
| R02 | Station/logger/program/header registry/four schema families; SP-01 bounded native parts and ordered-content ordinal mapping; no registry service. |
| R03 | Exact four TOA5 header records; station-specific20/21/27 columns and units; raw naive time + timestamp_ntz; original RECORD not globally unique. |
| R04 | Historical cutover10Jun2016 logger time; daily station packages/explicit empty files; GNLA2 v2 from13Jun appends nullable sim_quality_flag. |
| R05 | V2 adds header name, empty unit and Smp processing entry; registered station-family change only; no universal normalized header. |
| R06 | Compare all four native header records before typing; preserve verified wind-direction unit conflict; explicit UTF-8 text handling prevents reader-induced mojibake. |
| R07 | DAT-DQ-001…040 intact; NAN/-99/+999 preserved/flagged by field; RECORD999 is legitimate; never interpolate missing source observations. |
| R08 | Source due02:00 UTC; replace Event Grid +5 min backup with1 min activeADF polling; maintain station-specific completeness and D09 targets (§17). |
| R09 | Source active+365 d; acknowledged deliveries90 d; Raw/Bronze365 d; exact old headers for replay; no fabricated new logger epoch. **Enterprise horizon; DEV-RET-01 explicitly replaces cloud SFTP payload retention/closeout in temporary DEV (§12), no365day SFTP WORM claim.** |
| R10 | Station/date/revision, logger identity, original file/ordinal and four-header fingerprint, bytes/SHA/rows, native file checkpoint and verified delivery completion before independent station ACK. |

### 3.7 Open Prices REST — D07 R1 superseded where specified by D10

| Requirement | Preserved decision and architectural realization |
|---|---|
| R01 |335,600-record export is pinned historical profile only; current live floor/population discovered through REST. No native CDC or historical snapshot claim (§8). |
| R02 | Pin actual provider OpenAPI/schema separately from internal manifest/envelope schemas and exact source release. |
| R03 | Positive integral price.id; original numeric lexemes, nullable/type-specific fields and nested product/location/proof content retained; export schema not imposed on REST. |
| R04 | REST-only fresh bootstrap + bounded created-time tail,30 min overlap, weekly REST re-observation; no export bridge; one serialized requester. |
| R05 | Material acquisition changes recorded asR2/2.0.0; source field evolution separately classified; no automatic MINOR. |
| R06 | Missing pages/invalid IDs/bounds/envelope block acceptance; noncritical item failures retained with qualification, no loss of raw. |
| R07 | OPR-DQ-001…045 retained at proper scope; export-only baselines do not become live count assertions; actual compared API/export fields only. |
| R08 | ADF exclusively schedules the existing acquisition/receipt Jobs at 15/5 minutes under OA-API-02; five-minute horizon and 30/15/45-minute targets retain all latency gates (§17). |
| R09 | Raw≥365 d, evidence≥400 d, receipts≥365 d, native checkpoint protected; no source refetch for same retained accepted response. |
| R10 | One ledger HEAD/commands/events, exact attempt/page/window/root manifests, post-acceptance receipts, target observation and WINDOW_COMMIT; separate native Auto Loader checkpoint. |

### 3.8 REES46 Kafka — D08 R1 superseded where specified by D11

| Requirement | Preserved decision and architectural realization |
|---|---|
| R01 | Same411,709,736 original events, separate interface; source-derived peak estimate8,415/s, not measured Kafka capacity. |
| R02 | Actual Schema Registry catalog and IDs; FULL_TRANSITIVE; no invented registry IDs. |
| R03 |R2 retail.rees46.ingest.BehaviorEvent; nine original fields + required source_file_month/scenario_id and essential source/run/attempt clocks. |
| R04 | One independent transactional producer; behavior and compacted control topics;60×; bounded real-source ordinal fixtures, not a general CDC engine. |
| R05 | R2/2.0.0 material change; v2 only nullable sim_device_context with defaultnull; old/new decoders pinned. |
| R06 | Unknown IDs/bad Avro/DQ failures preserve raw Kafka record and error class in Bronze; no per-record registry request. |
| R07 | KAF-DQ-001…046 retained, with D11 application-attempt clarification for 023; source, inspection, global reconciliation and isolated fault scopes remain distinct. |
| R08 |6 partitions,14 days,10sec trigger,150000max offsets; broker/publish→Bronze p95≤30 s, p99≤120 s; no historical-event-time freshness claim. |
| R09 | Native checkpoint/control summaries/isolated recovery retained; DEV-KAF-01 explicitly limits broker availability/early closeout in temporary DEV, no RF3/HA claim; new topic generation never silently continues lost history. |
| R10 | Behavior+summary+next progress in one transaction; exact monthly ordinal/content reconciliation; physical offsets and logical IDs counted separately. |

### 3.9 Controlled gaps and amendments

| Amendment | Type / affected authority | Exact selected change; approval/qualification boundary |
|---|---|---|
| AM-01 | Material transport/deployment; five SFTP contracts R04/R10 | Azure Blob SFTP replaces OpenSSH. Local publisher fsync is allowed; remote fsync/rename-overwrite is not assumed. Close, read-back integrity, unused-name finalization and READY establish publication. |
| AM-02 | Material storage topology; A2 and D10 deployment binding | Separate SFTP account from WORM-capable lake account; preserve API semantics/retention. No payload schema change. |
| AM-03 | Superseded A3 receiver decision | **Revoked by A4-01.** Auto Loader is mandatory for all five SFTP sources; persistent native file checkpoints supplement receiver delivery/target completion evidence. |
| AM-04 | Scheduling/SLO; D03/D04/D06 + D09 | OFF/metadata5→1 min active discovery; Campbell Event Grid primary removed in favor of1 min polling. Keep targets as runtime gates, not achieved SLAs. |
| AM-05 | Existing simulated selection, not source evidence; D03 R04 | Ten OFF DELTAs selected as2,8,2,1,2,2,1,0,2,1 operations; endpoints2,10,12,13,15,17,18,18,20,21. Detailed key/action/fixture manifest remains a pre-activation gate. |
| AM-06 | D03R07 project predicate mapping | OFF-A41-001…030 are selected executable project definitions (§14.2.1), not original authority IDs; implement/verify them after approval against pinned source categories/profile. |
| AM-07 | New business design | Eight physical Gold products and governed views replace A2's five incompletely specified aggregates; business approval is part of architecture review. |
| AM-08 | Compute/deployment only | Restore D10 single-node API Jobs; lower unqualified E16/E8 defaults; retain R2 Kafka benchmark baseline until measurements justify change. |
| AM-09 | Existing D10/D11 material overlays | API REST-only/ledger/receipt R2 and Kafka envelope/attempt/recovery R2 remain2.0.0; no claim they are schema MINOR changes. |
| AM-11 | New business completeness policy; D01/D09 | Preserve timely purchase facts with pending article dimensions; attribute-complete views follow article freshness. Never drop facts through an inner join. |
| AM-12 | Rule/metric qualification; D06 R07 and Gold | Preserve original sensor-quality predicates; specify the field-specific trusted-statistics dictionary/test oracle before advertising those metrics as qualified. |
| AM-10 | Resolved availability; unresolved current release migration | contracts.v1.json is now bytewise/canonical-hash verified (§2.1). Preserve this older artifact; reconcile its42 rules/21 schemas with D01–D11/current overlays before activation. |

Retain A4's decisions only where unsuperseded. A42 receiver R4/4.0.0 supersedes A4.1 receiverR3 SDK/callback paths. SP-01 source-publication R3/3.0.0 explicitly versions necessary identity/image/part metadata while preserving original business payload versions; no provider schema rewrite. OA-API-02 scheduling and OA-API-03 distributed business routing are explicit D10 overlays. Retain unsuperseded A4.1 policies, including AM-13/AM-14; A42/SP-01/KSG-01/DEV-RET-01/DEV-KAF-01 are the targeted proposed overlays in this review. All targeted A42 overlays remain proposed until the user approves this architecture; source contract documents are unchanged. AM-13 explicitly versions simulated effective-time/action metadata (§14.6); it does not assert pre-existing historic master versions. Payload schema ordinals, semantic contract versions, infrastructure binding revisions and architecture revision are separate version domains.

## 4. Corrected topology, hosting and resource decisions

```mermaid
flowchart TB
  Local["Restricted original files and existing evidence"] --> Seeds["Pinned seed copies in lake; originals preserved"]
  Seeds --> PG["Real PostgreSQL Flexible Server"]
  Seeds --> Host["One shared VM: KRaft / Registry / independent producer / Connect / publishers"]
  PG --> Journal["Transactional article journal"]
  PG --> Connect["Debezium Connect on shared host"]
  Host --> Supplier["Azure Blob SFTP account: five supplier folders"]
  Supplier --> ADF["ADF: actual SFTP connector, Binary Copy"]
  ADF --> Raw["Lake account: original Raw and manifests"]
  Raw --> Prepare["Validate exact native source files and ordinal/header metadata"]
  Prepare --> AcceptedSFTP["Accepted original native files; incomplete transfers excluded"]
  AcceptedSFTP --> SFTPAL["Five SFTP source adapters; source-specific native checkpoints"]
  Journal --> Articles["ADF 15-minute wake-up; Databricks journal reader"]
  Connect --> HMTopics["H&M CDC + transaction + Connect internal topics"]
  Host --> REESTopics["REES46 behavior + compacted progress topics"]
  HMTopics --> Native["Native Kafka Structured Streaming"]
  REESTopics --> KNative["Independent REES native Kafka capture / checkpoint"]
  KNative --> KB["bronze.rees46_kafka: native append"]
  KB --> KS["silver.rees46_kafka_events: logical conformance"]
  KS --> KG["gold.rees46_kafka_activity_daily: independent G08"]
  ADF -.->|15-minute bounded Job dispatch| Python["Serialized Python acquisition Job"]
  REST["Open Prices REST"] --> Python
  Python --> Evidence["WORM-capable lake HTTP evidence"]
  Python --> Ledger["One Delta acquisition ledger"]
  Ledger --> Receipts["Accepted leaf receipts only"]
  Receipts --> AL["API receipt Auto Loader; own native checkpoint"]
  ADF -.->|independent 5-minute dispatch| AL
  SFTPAL --> Bronze["Source-specific Bronze Delta inventory: 10 total including Kafka"]
  Articles --> Bronze
  Native --> Bronze
  AL --> Bronze
  Bronze --> Silver["Source-specific Silver Delta inventory: 10 total including Kafka"]
  Silver --> Gold["8 managed Gold products; separate SFTP/Kafka analytics"]
  Gold --> Consumers["Approved consumer queries; future dashboard"]
  Registry["Git release metadata; actual Kafka Schema Registry"] -.-> Host
  Registry -.-> ADF
  Registry -.-> Bronze
  ADF -.->|onboarding / readiness / recovery| Connect
  ADF -.->|bootstrap / monitoring / bounded recovery| Host
  ADF -.->|eligible bounded processing and domain publication| Gold
  Control["4 shared operational controls; no offset DB"] -.-> ADF
  Control -.-> Silver
```

### 4.1 Minimum selected resource inventory

One DEV does not mean one storage account regardless of feature conflicts. The second account below preserves the required SFTP and WORM capabilities without adding a service or weakening the API contract.

| Resource / size assumption | Why required and dependencies | Alternative considered / selected outcome | Billing, idle and active requirement |
|---|---|---|---|
| One project DEV resource group | Ownership/tags/budget; workspace also has service-managed resources | Reuse suitable existing group | No group fee; resources need separate cost authorization |
| One Premium Databricks workspace + one discovered UC catalog | Distributed Spark, Jobs, managed Delta/UC | No additional workspace, environment or metastore | VM+DBU during compute; storage persists when compute stops |
| One HNS GPv2 lake account, SFTP disabled | Raw, protected HTTP/evidence/receipts, seeds, mutable state and managed-table root if needed | Reuse compatible project ADLS account | GiB-month, operations, retrieval/egress; no hourly compute; retention costs persist |
| One HNS GPv2 supplier account, SFTP enabled only in active windows | Actual managed SFTP endpoint; separate deliveries and ACK containers | Reusing lake conflicts with WORM; choose this minimum extra account | SFTP-enabled hours + storage/operations; disabling SFTP stops its hourly feature charge, not storage |
| One ADF + Azure IR in selected region | PostgreSQL onboarding/articles, five SFTP/Auto Loader lifecycles, API15/5-minute dispatch, domain Gold, reconciliation and recovery | No SHIR unless public SFTP reachability is later prohibited | Activity/copy runtime/meters; no paid Mapping Data Flows selected; stop triggers outside service windows |
| One Key Vault | Shared scoped credential references | No per-source vault | Operation/storage charges; retain required secrets safely |
| One Access Connector MI/storage credential | UC lake access | Reuse suitable connector | No separate compute; do not mistake it for arbitrary SDK credentials |
| One PostgreSQL Flexible Server, GP D2ds_v5-class,2 vCPU/8 GiB, initial128 GiB storage | Real H&M source, durable WAL/slots and backups; private connectivity | Shared self-managed database is cheaper nominally but increases contention/recovery work; managed selected (§6) | Compute while running, provisioned storage/backups while stopped; grow capacity before activation if WAL budget requires |
| One shared execution VM, D4as_v5-class,4 vCPU/16 GiB,64 GiB OS +128 GiB representative data disk, admitted by measured working set | KRaft broker/Registry, native Connect, independent REES46 producer, scheduled publishers and durable state; phase heavy source work (§10) | PC only for attended tests; containers considered below | VM billed running; managed disks persist/bill when deallocated; active during source simulation |
| Temporary real KRaft broker + compatible Schema Registry on the same execution VM | Real REES46 transactions/Registry and H&M native CDC transport | DEV-KAF-01 single-broker/RF1 deviation; enterprise RF3/minISR2 design retained separately | Included source VM compute, persistent Kafka/state disk/operations; no default managed cluster/eCKU subscription (§10/19) |
| One small VNet:2 Databricks subnets,1execution subnet,1PostgreSQL delegated subnet | Private articles JDBC, source-to-Connect traffic and native private Kafka/Registry clients | Public DB with changing client egress rejected for this selection | VNet itself no hourly fee; size/address/quota verified before deployment |
| One private PostgreSQL DNS zone/link | Resolve managed private database from VM and Databricks | No private-endpoint mesh | DNS query/zone fees; persists while stopped |
| One NAT gateway + one Standard outbound IP, unless a compatible shared egress exists | Explicit outbound from private execution/Databricks compute, stable API/storage/SFTP egress; private Kafka remains in the VNet | No standalone VM public IP; existing approved egress may replace NAT | Gateway/IP hours + processed bytes even when idle; cannot call “VM stopped” complete cost shutdown |
| Shared Monitor/action group + bounded diagnostics destination | ADF/Jobs/WAL/Kafka/SLO/cost incident evidence | Native service notifications first; no custom monitoring service | Alert/log ingestion/retention charges; sample/redact verbose Spark logs |
| GitHub repository + federated deployment identity | Reviewed immutable releases to DEV | No new repository/workflows in this phase | Runner/artifact use subject to account quota |
| Optional future on-demand SQL endpoint | Only if dashboard/query access actually requires it | Consumer validation can initially use approved Jobs/queries | Not provisioned or included as an always-running requirement |

All creation, paid enablement and capacity increases require explicit authorization. These are selected resources, not claims they already exist.

### 4.2 Network alternatives and selected boundary

| Alternative | Works when | Assessment |
|---|---|---|
| Default Databricks managed network + public PostgreSQL TLS with exact egress allowlists | Stable actual Databricks outbound addresses are known and maintained | Could remove customer VNet/NAT. New job clusters/worker egress can change; do not use “allow all Azure” or unrestricted5432 as a workaround. Not selected. |
| Existing compatible private network/egress | User supplies it and permissions/routes/quota work | Reuse it; do not duplicate NAT or subnets. No such host/network is assumed verified. |
| Private Flexible Server + injected classic compute | Direct private JDBC, stable outbound Kafka/API/SFTP connectivity required | Selected. Two dedicated Databricks subnets; separate delegated PostgreSQL subnet and DNS; explicit egress. |

Databricks documents explicit outbound connectivity for new VNets after 31March2026. NAT is the selected method for this topology, not a requirement for every possible Databricks design. [VNet injection](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/vnet-inject)

Database5432 accepts only Databricks/execution-subnet traffic and TLS with hostname/certificate verification. No public PostgreSQL exposure. Execution VM has no public inbound SSH/SFTP/Connect8083; use approved control-plane administration such as Azure Run Command. Connect REST remains loopback or protected internal access.

SFTP supplier endpoint remains authenticated public SSH2 on 22; allow the chosen Azure IR regional addresses and NAT egress, plus the user's current IP only for approved attended publishing. Storage endpoint DNS is used; no invented static storage IP. Lake HTTPS uses authenticated access and supported selected-network/trusted-service configuration for ADF/UC. Actual firewall/routing/host-key tests are pre-activation gates; no assumption that “trusted services” permits arbitrary SDK access. Kafka/Registry use TLS and actual scoped credentials; selected DEV Kafka/Registry is private to the source VNet.

### 4.3 Source publisher execution options

| Option | Availability, staging and durable state | Cost/tradeoff | Decision |
|---|---|---|---|
| Development PC | Original files already local; SQLite/journals on durable local disk; SFTP outbound22 allowed; resumable publication | Lowest incremental hosting cost; stops when PC sleeps/offline and depends on upload bandwidth | Supported for explicitly attended DEV slices. Never claim unattended SLOs while off. |
| Shared scheduled containers/jobs | Stage seeds to Blob; each job must use a valid durable source catalog and exclusive ownership | Cheap when idle, but SQLite on an ephemeral filesystem is invalid; durable-filesystem/cross-process fencing work adds dependencies | Defer as the continuous Connect/85 h producer already needs durable hosting. Do not add a database/file service just for these jobs. |
| Existing compatible host | Proven uptime, supported runtime/disk/network/backup and sufficient capacity | Potentially cheaper; ownership/availability cannot be assumed | Reuse if actually supplied and qualified. |
| One shared cloud execution VM | Durable source journals/caches, service supervision, independent producer/Connect; managed identity pulls approved seeds and secrets | Modest continuous CPU/disk cost; bounded resource contention must be tested | Selected for unattended simulation. It runs the selected temporary broker/Registry and source clients, **no SFTP server** and no managed PostgreSQL process. |

The VM is justified by continuous source clients and durable publisher state, not by the removed OpenSSH design. Keep OFF and Amazon supplier catalog/journal databases separate on its durable disk. Single-owner SQLite WAL with synchronous durability is an implementation choice inherited from existing work; immutable action receipts, backups/seals and fencing are required. Do not put active SQLite databases on Blob or an unqualified shared network filesystem.

SELECTED representative starting tier is128 GiB data disk, not a claim full history fits. Required data capacity = ceiling((active source catalogs + indexes + SQLite WAL/checkpoints + Kafka/KRaft/Registry/Connect durable logs + one bounded export + cached seed window + unACKed retained work + backup/log allowance)/0.70), all measured in GiB. OS64 GiB is separate. For the first bounded DEV population, admit only when the working set stays below70%of128 GiB; otherwise reduce the representative population or seek a separately approved larger disk. No source GC deletes unACKed/replay-required state.

Known full-source raw populations include83.83GB OFF JSON,22.62GB reviews,56.58GB REES decoded CSV and1.96GB metadata; they are **not** a VM disk-sum requirement. Keep pinned originals in lake, cache one needed input/export at a time. Full OFF/Amazon SQLite catalogs/indexes/undo/WAL may exceed this representative tier; full-scale sizing is a real measured capacity gate. A fixed512 GiB default is revoked, not replaced with an assertion that128 GiB fits everything.

## 5. Full seed-to-interface initialization and delivery plan

Immutable original files remain in the original local source folders. After later authorization, hash-matched seed copies may be staged to restricted lake seed storage for the source systems. This staging is not target ingestion: all eight integrations still cross their mandated genuine interface.

### 5.1 Verified allocations

| Source | Exact allocation / source evidence |
|---|---|
| H&M | Articles102,847initial+2,695reserved=105,542; customers1,354,442+17,538=1,371,980; purchases30,990,055+798,269=31,788,324. CSV bytes36,127,865+207,135,859+3,488,002,253=3,731,265,977. |
| REES46, both interfaces | Oct42,448,764; Nov67,501,979; Dec67,542,878; Jan55,967,041; Feb55,318,565; Mar56,341,241; Apr66,589,268. Sum411,709,736. Views385,746,849; carts19,114,063; purchases6,848,824. Repeated payload excess1,384,422. |
| OFF | FULL4,832,131 includes4,832,008 canonical occurrences plus 123 identity exceptions. Distinct nonblank codes4,832,069 includes61 ambiguous codes. These counts are not interchangeable. |
| Amazon metadata | FULL1,610,012 unique case-sensitive parent_asin in ten original shards; no original historical CUD chronology. |
| Amazon reviews | FULL43,883,164 before 2023-09-01 UTC; reserved3,780; total43,886,944. Source line order is not event-time order. |
| Campbell |41,105 historical +4,333 incremental=45,438;190 station-day files across7stations and4schema families. |
| Open Prices | Historical profile335,600; live count/floor deliberately unknown until legitimate REST discovery. No bulk-export bootstrap. |

Reviews daily counts for 1–13September2023 are **749,609,518,495,393,366,250,158,84,82,44,28,4**. Preserve disjoint original ordinal membership across FULL and these days; do not derive original ordinals from filtered output positions.

| Campbell station / family | Initial rows | Increment rows | Daily packages |
|---|---:|---:|---:|
| BLTA2 / GLBA_BARTLETT |832|1,745|73|
| GNLA2 / GLBA_NUNATAK_QUEEN |6,949|1,069|45|
| GQIA2 / GLBA_NUNATAK_QUEEN |6,922|1,045|44|
| CKPA2 / KLGO_T107 |1,212|57|9|
| DWSA2 / KLGO_T109 |7,724|109|5|
| GOTA2 / KLGO_T107 |8,740|131|6|
| SPCA2 / KLGO_T107 |8,726|177|8|

### 5.2 H&M database integration — twelve initialization obligations

1. **Have:** articles.csv, customers.csv, transactions_train.csv and the existing verified split/evidence references; source byte hashes are in D01.
2. **Seeds:** restricted unchanged originals; later hash-matched lake copies and bounded importer cache.
3. **System first:** real PostgreSQL database, roles, source tables/journal/action receipts, publication/slot, actual Kafka and Connect.
4. **Initialize:** quote-aware bounded COPY/import into source staging; durable source-owned import receipts; activate exact initial allocation and FK-consistent master population only after reconciliation.
5. **Interface:** articles via PostgreSQL sealed journal/JDBC; customers/purchases via actual Debezium pgoutput/WAL and Kafka.
6. **FULL:** repeatable-read article baseline with fixed visible J0 membership; Debezium consistent snapshot of initial customers/purchases.
7. **Changes:** source transactions activate first-used reserved masters before purchases; approved CREATE/UPDATE/DELETE/rollback/late-commit fixtures use source-owned action IDs.
8. **Publication:** one original purchase day per10elapsedmin for 1–22Sep2020; no invented within-day retailer transaction time.
9. **Discovery:** ADF15 min articles; connector and native Kafka capture continuously during active source operation.
10. **State:** source journal seals/ACK; Connect Kafka offsets and replication slot; native consumer checkpoints. Shared controls are evidence only.
11. **Layers:** raw baseline/journal/CDC Bronze; ordered SCD2 masters/current purchase occurrences in Silver; exact-unit purchase products in Gold.
12. **Recovery:** retained seals/WAL/Kafka/native checkpoint first; lost slot/history requires declared generation and reconciled resnapshot, never silent JDBC substitution.

### 5.3 REES46 SFTP CSV

1. **Have:** seven pinned original monthly files/evidence.
2. **Seeds:** local originals; restricted lake copies; supplier cache retains exact gzip identities.
3. **System:** managed SFTP endpoint and source publisher.
4. **Initialize:** verify seed release; retain original seed bytes unchanged and publish October's registered SP-01 native month package with exact source occurrence provenance.
5. **Interface:** supplier SFTP → actual ADF SFTP Binary Copy → landed native Raw → validated native CSV/gzip with SP-01ordinal → native CSV Auto Loader → Bronze Delta.
6. **FULL:** October42,448,764; historical onboarding can explicitly backfill the remaining contracted months without claiming a normal monthly run.
7. **Changes:** subsequent month-close files; November r2 full-month replacement at ordinal108; April v2 sim_export_quality.
8. **Publication:** next-month first day02:00 UTC on the simulated source calendar; original late/missing/empty cases retained.
9. **Discovery:** ADF10 min.
10. **State:** source immutable delivery/revision; persistent native Auto Loader checkpoint/schema location per compatible scope; receiver source_deliveries and actual per-file/delivery target completion.
11. **Layers:** all captured revisions in Bronze; eligible revisions in Silver with one approved active revision per month; SFTP-only business activity/funnel Gold.
12. **Recovery:** retry same revision exact bytes; new attempt identity; raw replay with stable ordinals; gaps hold contiguous completeness, not evidence retention.

### 5.4 OpenFoodFacts JSONL

1. **Have:** original openfoodfacts-products.jsonl.gz and existing complete profile.
2. **Seeds:** restricted original/copy; no new dump or reprofiling substituted.
3. **System:** independent source catalog/journal and managed supplier endpoint.
4. **Initialize:** import only4,832,008 unambiguous keys into mutable canonical catalog; retain all 123 exceptions in FULL provenance.
5. **Interface:** actual SFTP → ADF Binary Copy → original Raw → validated native JSONL/gzip with SP-01identity/image → native JSON Auto Loader → Bronze Delta.
6. **FULL:** all 4,832,131 original occurrences, original nested payloads intact; bounded package parts may be generated only with exact original ordinal mapping.
7. **Changes:** ten source-journal DELTAs; INSERT/UPDATE/RETIRE/DELETE; selected21-operation allocation explicitly simulated; no upstream historical CDC assertion.
8. **Publication:** hourly, within 10 min after window close.
9. **Discovery:**1 min while active, explicit amendment of5 min.
10. **State:** source transactional sequence/action receipt/sealed membership; native Auto Loader file checkpoint and receiver per-file/whole-delivery completion receipts.
11. **Layers:** original JSON/envelope Bronze; canonical current state plus identity-exception view in Silver; approved product view and catalog quality Gold.
12. **Recovery:** same action returns same receipt; re-export same sealed membership; schema99/revision conflict holds range; no arbitrary duplicate-code winner.

### 5.5 Amazon metadata Parquet

1. **Have:** ten original SNAPPY Parquet shards and verified keyset/record multiset evidence.
2. **Seeds:** restricted exact shard copies; source cache only.
3. **System:** independent source catalog/journal and managed SFTP.
4. **Initialize:** exact1,610,012-key import; initial versions0, snapshot boundary0, no synthetic original INSERT history.
5. **Interface:** real SFTP → ADF Binary Copy → original Parquet Raw → validated byte-identical native accepted Parquet → native Auto Loader/Delta append → Bronze.
6. **FULL:** all ten original shards; validate all footers/pages/native nested types and keyset/multiset.
7. **Changes:** hourly journal exports preserve INSERT→DELETE and repeated UPDATE commits; explicit tombstones and RETIRE state; approved title-update fixture remains simulated.
8. **Publication:** normal sealHH:30, followingHH:00; exact ten-export fault calendar retained.
9. **Discovery:**1 min active by amendment.
10. **State:** source head lock/sequence/version/action receipt, seal and distinct ACK high; source-specific native parsing/schema checkpoint plus source_deliveries/reconciliation/ACK.
11. **Layers:** complete physical snapshots/operations Bronze; case-sensitive current Silver; product dimension view, review relation and catalog quality Gold.
12. **Recovery:** original same-revision retransmit; corrected Parquet schema/parts with new revision and same business membership; new business correction uses new journal sequence.

### 5.6 Amazon reviews JSONL

1. **Have:** original22.616 GB Electronics review JSONL and complete occurrence/day profile.
2. **Seeds:** exact restricted original/copy.
3. **System:** occurrence-preserving publisher and managed SFTP.
4. **Initialize:** one streaming source pass assigns original line ordinal before selecting FULL/daily membership; no natural-key or payload dedup.
5. **Interface:** actual SP-01 SFTP JSONL gzip → ADF → original Raw → validated native JSONL/gzip carrying original global ordinal → native JSON Auto Loader → Bronze.
6. **FULL:**43,883,164 pre-cutover occurrences.
7. **Changes:**3,780 reserved original occurrences over13UTCdays; fixture-only CREATE/EDIT/RETRACT is not original edit history.
8. **Publication:** one original day per10elapsedmin in the approved controlled replay; expectations stop at 13Sep2023.
9. **Discovery:**1 min active;15 min recovery-only, restoring1 min when time-sensitive work resumes.
10. **State:** source exact day/original-ordinal mapping, immutable revisions, native JSON Auto Loader checkpoint and logical source_deliveries completion.
11. **Layers:** full original lines/types/ordinal Bronze; occurrence Silver; original review activity/rating distribution and explicitly current-metadata association Gold.
12. **Recovery:** stable original source hash+ordinal; disjoint baseline/day coverage; retransmit/replay cannot create another business occurrence.

### 5.7 Campbell TOA5 DAT

1. **Have:** seven original DAT files and exact station/header evidence.
2. **Seeds:** unchanged originals/copies, native bytes/headers retained.
3. **System:** station-aware gateway publisher and managed SFTP.
4. **Initialize:** cut at 10Jun2016 raw logger time; seven historical files41,105 rows;190 station/day packages4,333 rows.
5. **Interface:** actual SFTP → ADF binary DAT acquisition → original Raw → bounded native DAT parts → native text Auto Loader/TOA5 column transforms → Bronze.
6. **FULL:** seven contracted historical packages, retaining four native TOA5 header records each.
7. **Changes:** original station-day observations and approved revisions/v2; explicit header-only CKPA2 zero days; no invented missing hours.
8. **Publication:** next simulated day02:00 UTC; raw observation time remains timezone unknown.
9. **Discovery:**1 min activeADF; Event Grid removed by amendment.
10. **State:** one persistent source Auto Loader query with independent station/date/revision completion and ACKs; logger reset requires verified new epoch context.
11. **Layers:** lexical/header provenance Bronze; station-specific NTZ/units/quality Silver; comparable measurement/day summaries Gold.
12. **Recovery:** exact old schema/header replay; later valid station packages may capture while the affected station's continuity is held.

### 5.8 Open Prices REST

1. **Have:** public documented prices endpoint and historical profile; old export is not a live ingestion input.
2. **Seeds:** no export seed for acquisition; pin schema/contract artifacts and retain prior profile offline.
3. **System:** real REST access, protected lake evidence, one Delta ledger, acquisition/receipt Jobs.
4. **Initialize:** freeze H0 and discover live earliest created timestamp through REST; explicit empty discovery if empty.
5. **Interface:** real HTTP GET /api/v1/prices; no alternate bulk export.
6. **FULL:** bounded REST historical windows from discovered A0 throughH0, live enumeration consistency only.
7. **Changes:** overlapping created-time tail and weekly full historical REST re-observation; revisions observed, not full CDC/deletion history.
8. **Publication:** provider-controlled, no provider availability/update SLA invented.
9. **Discovery:** ADF schedules the approved acquisition Job every 15 minutes and independent receipt Auto Loader Job every 5 minutes; the five-minute API horizon is unchanged.
10. **State:** one authoritative acquisition_ledger; distinct receipt-consumer native checkpoint and Bronze window markers.
11. **Layers:** exact acquired item occurrences Bronze; observed representations Silver; latest observed price.id and currency-separated observation products Gold.
12. **Recovery:** retained evidence reseal/receipt repair/target idempotency; new live traversal after interruption if incomplete; no stale-export bridge.

### 5.9 REES46 Kafka

1. **Have:** same original seven downloaded REES46 CSV/gzip identities.
2. **Seeds:** original immutable files; hash-matched shared-host cache, not Bronze-fed replay.
3. **System:** temporary real KRaft/Schema Registry on the shared source host under DEV-KAF-01; independent transactional producer.
4. **Initialize:** verify all source hashes, register/verify real schema IDs, fence lane, recover committed control state.
5. **Interface:** actual transactional Kafka publication; direct native Databricks connector.
6. **FULL:** finite replay of411,709,736 original occurrences at 60×.
7. **Changes:** original replay only plus isolated declared duplicate/late/schema/failure fixtures; no general corrections engine.
8. **Publication:** original event-time displacement divided by60 from S0; preserve original/scheduled/enqueued/broker clocks.
9. **Discovery:** continuously active native micro-batches,10sec trigger and150000max offsets.
10. **State:** compacted producer summaries/progress in same Kafka transaction; independent native Bronze checkpoint.
11. **Layers:** D11 raw-preserving native Bronze → independent silver.rees46_kafka_events → G08 gold.rees46_kafka_activity_daily; source-scoped Kafka streaming analytics, no default UNION with SFTP G03.
12. **Recovery:** same checkpoint; otherwise isolated target/reconciliation/frozen canonical-view cutover; reseed new physical generation if retained history expired.

## 6. Real PostgreSQL and Debezium source design

### 6.1 Hosting comparison and selection

| Option | NUMERIC / WAL / snapshot / restart | Four-day cost and effort | Decision |
|---|---|---|---|
| Flexible Server GP2 vCPU/8 GiB | Native NUMERIC/pgoutput, durable slots/storage, managed backup/PITR; actual parameters/slot restart must be proven | Compute+storage+backup; minimal DB OS work; separate from publisher contention | Selected; initial128 GiB is not proof of7-day WAL capacity |
| Self-managed PostgreSQL on shared VM | Fully valid if fsync/full_page_writes/synchronous durability, persistent WAL, slots, TLS and backups are correctly operated | Avoids managed DB compute, but needs extra disk/memory/backup/restore and competes with Connect/producer | Supported fallback only through a documented change and recovery/capacity gates |
| PostgreSQL on development PC | Real interface/NUMERIC/CDC possible | No incremental Azure DB cost; off/sleep/reboots violate unattended operation; network reachability/slot retention still required | Attended bounded demos only |
| Serverless free/ephemeral hosted database | Depends on persistent logical slots, pgoutput, adequate storage, no forced sleeping/reset | Nominally cheap; exact limits/retention/roles unverified | Not selected; a JDBC-readable service alone does not meet WAL CDC requirements |

Known CSV input is3.73 GB; PostgreSQL heap/index/WAL/backup footprint is larger and not measured. Budget formula: initial source/staging+indexes+temporary load space+7 days measured peak WAL+catch-up reserve+free-space reserve. Import duration cannot be inferred from bytes alone; COPY, constraints/indexes, logical snapshot and Kafka egress must be measured. Initial load uses bounded committed source import units with persisted receipts; an uncertain unit resolves by its receipt, not blind reinsertion.

Use PostgreSQL17 as a supported baseline subject to managed-service/connector compatibility; no HA standby is purchased for this DEV. Set actual logical-replication parameters supported by Flexible Server, pgoutput publication for customers/purchases, replication role, SELECT grants and adequate slots/wal_senders. Check wal_level and actual slot/catalog settings. Do not copy obsolete Single Server CLI parameters without checking the current service. [Flexible Server logical decoding](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical) [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)

Restarting the same healthy database must preserve its slot; restore/failover is a different gate. PITR availability does not prove that original logical slot/offset continuity survives a restore. An invalidated slot or unavailable WAL becomes an integrity incident and declared recovery generation.

### 6.2 Baseline, journal and exact monetary representation

Seed application columns retain original CSV strings. Purchases add deterministic purchase_id from original seed SHA+one-based logical ordinal; identical source payloads remain different purchases. Initial masters include first-use activation allocation and required dormant originals from D01; future masters activate in the same source transaction before referencing purchases.

Use unconstrained PostgreSQL NUMERIC and a separate exact price_lexeme. Match their mathematical values on every import/mutation; scientific notation is valid. Preserve all six original exponent tokens. Finite exact conversion to Silver DECIMAL(38,21) is gated by no-overflow/no-rounding round-trip; future out-of-range values retain lexical evidence and fail only the trusted projection. Channel1/2 are categorical codes without invented channel labels; price units have no verified currency.

Articles baseline is a repeatable-read snapshot plus a fixed visible journal-member set J0. Materialize/seal the baseline images or retain the consistent source snapshot until capture completes; independent mutable JDBC reads are not a shared snapshot. Subsequent export selects committed unacknowledged members and seals their exact IDs. Allocation order is not commit order: a low sequence committing later must remain eligible. Source mutation+journal image+version+action receipt commit together; seal high and ACK high are separate. Source ACK requires exact sealed membership and actual Bronze commit evidence.

Customers/purchases use a consistent Debezium initial snapshot of the approved baseline. Pause new simulated mutations until snapshot identity/count/FK handoff is reconciled; then begin the reserved replay. Historical purchase t_dat is not source COMMIT time or initial-load freshness.

### 6.3 Connect placement, topics and state

One pinned supported Debezium PostgreSQL connector, tasks.max=1, on a **distributed-mode Kafka Connect worker** on the shared execution host. One worker is sufficient for DEV; it is supervised and restartable, not a custom CDC service.

| Topic / object | Purpose / selected treatment |
|---|---|
| dev.hm.public.customers | Key customer_id;3provisional partitions; full Debezium before/after/source/op/transaction envelope |
| dev.hm.public.transactions_train | Key purchase_id;6provisional partitions; exact price text retained |
| dev.hm.transaction | One partition; native BEGIN/END and expected event/data-collection counts/order |
| dev.hm.heartbeat | One partition if enabled; operational liveness/LSN, never purchase data |
| dev.connect.configs | Native distributed Connect configuration;1 partition, compacted |
| dev.connect.offsets | Native connector offsets;1provisional partition sufficient for one connector, compacted |
| dev.connect.status | Native worker/connector status;1provisional partition, compacted |
| PostgreSQL publication + named slot | Customers/purchases only; pgoutput; do not drop slot on routine connector stop |

All H&M data/transaction/heartbeat topics use the contracted14-day transport retention and current operational timestamps; internal state topics are compacted without destructive TTL/tombstones during dependent operation. Enterprise RF3/minISR2 remains the full design; selected shared DEV broker uses explicit DEV-KAF-01 RF1/minISR1 including internal topics. No HA equivalence is claimed. Names are selected naming conventions; actual cluster/topic UUIDs and creation are future outputs.

Choose Connect JSON converters with schemas enabled and decimal.handling.mode=string, plus original price_lexeme, to avoid an additional H&M Avro registry programme. The full native schema/envelope is retained. No unwrap transformation discards before/source/transaction/tombstone metadata. Enable actual transaction metadata. Connector configuration, slot identity and offsets are native Connect/Kafka state; a local JSON file or Delta control row does not replace them.

The connector is at least once unless an independently proven connector-specific exactly-once mode is explicitly adopted. Silver must tolerate repeated transport delivery of the same native source operation. Identity uses actual connector-version source transaction/LSN/order fields, not LSN alone. Snapshot read identity is baseline generation+entity+source PK. Validate the exact native identity fields before activation; do not fabricate commit_lsn values or assume a PostgreSQL transaction ID never wraps.

### 6.4 Capture and transaction-aware business application

Customers and purchases retain two separate native Kafka-to-Delta capture queries/checkpoints. Purchases capture also retains native transaction metadata as a distinguishable record_kind in its Bronze table. These two target commits are not atomic with each other.

A native continuously supervised H&M workflow can run capture and a separate conformance task concurrently on its shared stream cluster; no dependency waits for a never-ending capture task to finish. Conformance wakes from Bronze append/CDF work at a selected60-second trigger, processes complete source transactions in captured commit order and checks END membership across both tables. Incomplete transactions remain pending in retained Bronze and are re-evaluated; no missing-member skip or extra pending-events table.

Each entity apply is deterministic and idempotent. A transaction-stage receipt is recorded only after all required entity target markers/commits validate. Partial apply is repaired from native operation IDs; Gold waits for the complete stage and uses frozen Silver versions. There is no cross-table ACID claim. Snapshot completeness is a separate gate from ordinary END-event transaction membership.

Monitor connector state, snapshot state, replication slot restart_lsn/confirmed flush positions, retained WAL bytes, source action receipts, transaction counts, source→broker→Bronze lag and source/target reconciliations. R3 WAL-budget warning/incident/admission protection thresholds remain50/75/85%. Never silently drop a slot to resolve disk pressure.

### 6.5 ADF initialization, initial staging and snapshot-to-CDC cutover

ADF has a real finite PostgreSQL onboarding workflow even though it does not copy every database table to ADLS:
1. Verify pinned source/import release, database/role/schema readiness, network/TLS and source-owned import receipts.
2. Admit or observe the approved initializer that loads the downloaded CSVs into the **real PostgreSQL source**. This is a source initialization action, not ingestion of those CSVs directly into Bronze.
3. Validate exact initial counts, original purchase IDs, source NUMERIC/lexical round-trip and actual master/FK constraints. Keep new simulated mutations paused.
4. Seal the article baseline and fixed visible journal J0; invoke its bounded JDBC capture workflow. Use sealed immutable images or a retained consistent source snapshot, not independent mutable range reads.
5. Configure/activate one native Debezium initial snapshot for customers/purchases, with actual publication/slot/internal topic identity. Start native Databricks capture once its dependencies are ready.
6. Wait for actual connector snapshot-completion evidence, correct source baseline capture, native position continuity and exact customer/purchase/source-membership reconciliation.
7. Confirm article baseline and CDC handoff readiness, then authorize the source simulator's reserved replay. Thereafter Connect and Kafka capture continue natively; ADF does not restart them every 15 minutes.
8. Record actual stage/target versions and cutover evidence in existing controls/evidence, not another CDC watermark.

| Initial/staging option | Selected assessment |
|---|---|
| ADF PostgreSQL Copy → ADLS → Bronze for all customers/purchases, then independent CDC snapshot | Reject as the default: duplicates the approved baseline and needs a separately proven consistent snapshot/LSN handoff. Ordinary Copy partitions do not automatically share one repeatable-read snapshot. |
| Sealed article baseline/journal via JDBC; Debezium's own consistent initial snapshot → Kafka → native Bronze | **Selected.** One native snapshot-to-WAL authority for CDC; bounded article units have source-sealed membership and actual target receipts. |
| Immutable database export files for later huge historical recovery | Conditional future bounded recovery option only if a source-owned snapshot/export and matching position/membership are explicitly sealed and reconciled. Not SFTP, not an automatic second baseline. |

No ADLS bulk staging is required for normal CDC records. Their full raw transport/envelope is durable in Kafka/Bronze; original source CSV seed copies and source audit manifests remain retained separately. Articles may retain bounded extraction evidence, but use their source journal/seal—not Auto Loader—as the acquisition authority.

For a source/connector outage, ADF's recovery mode checks actual slot/offset/horizon and native run state, coordinates an exclusive repair/catch-up and verifies source-to-Bronze results. Re-extraction or resnapshot requires a declared generation/continuity decision, not ordinary JDBC polling presented as WAL CDC. The selected private networking and shared execution host already support these actions; no new service is needed.

## 7. Azure Blob SFTP publication, acquisition and ACK

### 7.1 Endpoint, identities and folders

Use one GPv2 HNS **supplier account**, endpoint `<supplier-account>.blob.core.windows.net:22`. The actual account name and SSH host fingerprints are deployment outputs. SFTP uses SSH2, not HTTPS/TLS. Key material and host-key trust are in Key Vault/configured linked services.

One deliveries container has five folders: `rees46-csv`, `openfoodfacts-jsonl`, `amazon-metadata-parquet`, `amazon-reviews-jsonl`, `campbell-dat`. Delivery namespaces include source generation, logical delivery and revision. A separate ordinary ACK container prevents needing receiver write permission over supplier data.

Only **two shared SFTP local users** are selected: publisher and receiver. Publisher has necessary container read/list/create/write/delete for temporary/final publication and ACK read/list; receiver has read/list on deliveries and create/write/read/list on ACKs. No per-source identities, READY-folder ACLs, local-user ownership hierarchy or authorization DB. Configure explicit container permissions without named/default ACLs on the SFTP paths.

Use a fresh SFTP connection bound to each container; the service does not support cross-container traversal/rename within one session. ADF uses actual SFTP-source and SFTP-sink linked-service bindings for deliveries/ACKs with the same shared receiver identity. This is one endpoint and two ordinary container permission scopes, not five permission schemes.

Local-user SSH keys are selected for supported ADF compatibility. The existence of newer Entra SFTP authentication does not imply that the ADF connector supports its SSH-certificate workflow; no managed-identity SFTP authentication is assumed. [SFTP authentication/permissions](https://learn.microsoft.com/en-us/azure/storage/blobs/secure-file-transfer-protocol-support) [ADF SFTP connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-sftp)

### 7.2 Publication protocol adapted to the actual service

1. Source transaction/action receipt and fixed export membership become durable before external publication.
2. Generate each source file locally with the pinned writer/codec, deterministic row membership and required native headers. Local filesystem fsync is used where supported.
3. Upload under an attempt-specific staging name; close the handle. No simultaneous reader/writer or generic random-write resume assumption.
4. Verify complete remote length and read-back SHA against the local finalized bytes.
5. Rename each staged object to its **unused** immutable final name, within the same container, or upload to an unused final name and withhold READY until verified.
6. If final name already exists, compare complete expected identity/bytes: identical is idempotent success; different is an integrity hold. Never rename-overwrite.
7. Upload/close/verify the exact final manifest, then publish READY referencing its SHA as the last object.
8. A failed finalization leaves staging/final objects unaccepted; a directory is not a multi-object transaction.
9. A later correction uses a new revision/manifest; old finalized bytes remain evidence.

Microsoft documents unsupported OpenSSH fsync extension, rename-over-existing errors, cross-container restrictions, lack of shell access, multiprotocol append limitations, absent default Content-MD5, a two-minute idle disconnect and500 GB maximum upload. Clients must reconnect safely and use supported settings; no source file here is assumed splittable because of SFTP. [Blob SFTP limitations](https://learn.microsoft.com/en-us/azure/storage/blobs/secure-file-transfer-protocol-known-issues)

The supplier account has no WORM claim. Trusted publisher code enforces immutability and receiver hashes detect conflicts; original pinned seeds are outside its writable delivery container. The lake retains accepted evidence under separate protection. Microsoft explicitly lists container WORM supported for HNS but unsupported with SFTP enabled. [Storage feature matrix](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts)

### 7.3 Mandatory native Auto Loader flow and responsibility boundary

**Original downloaded seeds → independent native-format simulated source publisher → actual Azure Blob SFTP → ADF READY/manifest/preflight → actual SFTP Binary Copy → ADLS native Raw/accepted → spark.readStream.format("cloudFiles") → managed Bronze Delta → source-specific Silver → Gold.**

All five SFTP sources use this path. Receiver ingestion protocol R4/4.0.0 supersedes A4.1 receiverR3. A4.1's custom executor Python Blob SDK readers, metadata-only binaryFile ingestion, executor download credentials and receiver ordinal-reconstruction framework are **removed entirely**, including fallback/recovery. There is no receiver Parquet preparation, custom discovery service or SFTP DATA/FILE_COMMIT/DELIVERY_COMMIT protocol. The separate D10 API SDK/evidence mechanism is unaffected.

The publisher owns FULL/DELTA simulation, native row/schema/identity, source journals/revisions, exact membership/ordinal provenance and READY-last. ADF owns readiness/transport/workflow admission. Native Auto Loader owns actual incremental parsing/file state. PySpark performs only needed technical projections/TOA5 interpretation and source-specific downstream transformations. Bronze writes default to native checkpointed Delta append.

### 7.4 SP-01 source-publication identity contract amendment

Native CSV/JSON row output does not supply an original seed ordinal. Identity is solved **before source publication**, never through Spark partition/order/content matching. SP-01 is a material source transport/metadata overlay R3/3.0.0 for D02/D03/D05/D06; original payload v1/v2 schema ordinals remain distinct. It must be included in the approved source/schema release before its simulator executes.

| Source | Deterministic delivered identity and original representation | Contract effect / joinable evidence |
|---|---|---|
| REES46 CSV/gzip | Keep the nine original columns/quoting/lexical values; append reserved _source_record_ordinal as a required string integer. Preserve approved sim_export_quality when the original payload is v2. Seed SHA/source month are fixed by the part manifest. | Original occurrence ID uses the existing pinned-source hash/one-based logical ordinal algorithm. FULL/replacement parts carry actual original ordinal values; exact old9column seed/v1/v2 stays audit evidence. Added metadata column/header is explicit SP-01, not silently called the original header. |
| OFF FULL JSONL | Registered JSON envelope: _source_record_ordinal, _source_record_json (exact original UTF-8 JSON record text), payload (the unchanged original nested object). Source seed identity fixed in manifest. | Native JSON inference has a payload VARIANT hint to avoid expanding334949global paths into columns. Native JSON parses the envelope; original nested content and lexemes remain. Canonical/conflict populations use source identity, not code dedup. |
| OFF DELTA JSONL | Keep existing committed source change/sequence/version/transaction/after/tombstone fields; after uses a native VARIANT hint. Carry the frozen original operation JSON text under the registered transport metadata field. | Existing source operation IDs already identify DELTA members; no new occurrence invented. FULL/DELTA have separate compatible native query/schema scopes and common Bronze projections. |
| Amazon metadata Parquet | Original native FULL shards unchanged; native source DELTA envelope has its existing source change IDs/versions/tombstones. | No extra source ordinal, receiver rewrite or Parquet-to-Parquet conversion. Exact key case/types/nesting and physical row multiset remain. |
| Reviews JSONL | Original10payload fields/nested content remain JSON fields; add _source_record_ordinal and _source_record_json (exact original record text). Original global ordinal is assigned before FULL/day selection. | Added reserved transport fields are SP-01. Repeated identical payloads have different original ordinals; day membership and fixture lifecycle IDs remain distinct. Reserved-field collision fails publication rather than overwriting source fields. |
| Campbell TOA5 DAT | Source publisher emits bounded native DAT parts with the exact four TOA5 headers and original observation lines, in original delivered order. Each part manifest binds a seed file and data-position→original-ordinal mapping (or proved contiguous original range). | Native wholeText gives ordered **file content**, so posexplode position after the four headers joins exact source-controlled membership by (part_id,data_position). This is not unordered Spark row numbering. FULL still has seven logical source packages; bounded native part packaging/mapping is explicit SP-01. |

Publisher validation proves original identity coverage/multiplicity and content versus the pinned seed/snapshot/journal before READY. Same original payload with a different ordinal remains another occurrence. Wrong/missing/nonunique identity mapping blocks successful source completion; never recover it from payload-hash matching. Source part SHA is the delivered-byte hash, distinct from the original seed SHA. Original local files are never edited. An old native seed/legacy delivery lacking SP-01identity must be re-published by the source owner under a registered new transport revision with the same original occurrence identities; the receiver does not invent ordinals or claim the new bytes equal the old9column file.

No receiver processing dataset is introduced. The publisher's small manifest/membership metadata is genuine source delivery metadata, not a generic offsets database. Large populations remain native source files/Delta tables, not ADF activity outputs.

### 7.5 ADF acquisition, integrity and accepted-path eligibility

1. Discover source-specific READY; copy/read small control objects from the actual SFTP connector.
2. Validate exact manifest/hash, source/generation/delivery/revision/schema/adapter allowlists, predecessor, unique file membership and required source identity metadata.
3. ADF SFTP Binary Copy lands an unused Raw attempt outside native Auto Loader input. Supplier retransmission resolves the same winning logical revision, not a new accepted path.
4. Existing validation Job verifies exact landed sizes/SHA/CRC, part membership and headers/footer/schema as required. ADF Copy success alone is not a SHA256 or Bronze success claim.
5. Publish byte-identical validated native files into the source's accepted prefix; verify the whole accepted set, then guarded ACCEPTED_READY in source_deliveries.
6. Invoke the sole configured availableNow Auto Loader workflow; native sink completion plus independent exact reconciliation controls downstream readiness/ACK.

Use native Spark validation on **bounded source parts**, never a Blob SDK ingestion reader: streamed source publisher hash plus landed native Spark sha2(binaryFile.content,256) verification where qualified. This is integrity validation only, not Auto Loader ingestion/ordinal reconstruction; no content collection to the driver. Initial publisher CSV/JSONL part target≤32 MiB decoded/64 MiB encoded; DAT≤1 MiB decoded/2000observations; original Parquet shards stay unchanged with a qualified per-file validation memory cap initially512 MiB. Register actual original shard sizes from retained evidence. If a required native shard exceeds the validator envelope, qualify appropriately sized native validation compute before admitting it; do not convert it or claim an unverified hash. CRC/schema/record counts are independent checks, not substitutes for exact file SHA.

Accepted publication may use ADF native binary copy into unused final names while that source's Auto Loader query is stopped. Serialize **all** publication against active inference/query execution; start availableNow only after the full set is verified. Partial publication is PUBLISHING and cannot start a query. This avoids a custom rename/download framework; no claim of multi-file atomic storage publication. One owning ADF source workflow and actual Job/run/quiescence checks prevent concurrent query/copy. Parent timeout does not terminate a child. Normal accepted paths never overwrite; conflicting same-name bytes hold.

Input prefixes contain data files only; manifests/READY/ordinal metadata stay outside their data glob. Unexpected data paths without an ACCEPTED_READY source manifest fail readiness. Source control metadata is read with native Spark JSON/Delta references for bounded joins, not executable expressions or custom downloader code.

### 7.6 Exact native Auto Loader configurations

Common selected baseline: DBR16.4LTS pinned supported patch; UC managed Delta sinks; directory listing (no notifications/event-resource provisioning); includeExistingFiles=true only at first checkpoint initialization; allowOverwrites=false; cleanSource=OFF; ignoreCorruptFiles=false/ignoreMissingFiles=false. Trigger availableNow directly from ADF after readiness. Each compatible source/mode has a stable accepted root, its own schemaLocation and checkpointLocation **persisted across all runs**, no per-delivery reset.

Define A=staging/dev/sftp/<source>/source_generation=<g>/accepted/mode=<mode>/schema_family=<registered>/, S=state/dev/schema-state/sftp/<source>/<mode>/<schema_epoch>/, C=state/dev/checkpoints/sftp/<source>/<mode>/<consumer_generation>/. Actual account/catalog roots are deployment bindings. A(source/mode) instantiates the source folder and mode partition, not a literal slash in source_id; defaults are rees46-csv/csv, reviews/json and campbell/text. Full/delta roots resolve from their registered schema families. Separate OFF FULL/DELTA and Amazon FULL/DELTA scopes give seven compatible query scopes across five integrations; not seven datasources. The single source Job serializes its compatible scopes/sole target writer.

| Source / input / Bronze target | Native cloudFiles format and options | Schema/inference/evolution and metadata |
|---|---|---|
| REES46 / A(rees46-csv)/*.csv.gz → bronze.rees46_sftp | csv; header=true, sep=comma, quote=doublequote, escape=doublequote as registered, UTF-8, multiLine=true for registered quote-aware records, with bounded source parts; leading/trailing whitespace not trimmed; registered blank/null semantics | Infer strings, hints _source_record_ordinal STRING and _corrupt_record STRING; schemaEvolutionMode=addNewColumns, rescuedDataColumn=_rescued_data, PERMISSIVE/columnNameOfCorruptRecord=_corrupt_record. S/C for rees46/csv. Original nine lexical columns retained; publisher ordinal supplies identity. |
| OFF FULL / A(openfoodfacts-jsonl/full)/*.jsonl.gz → bronze.off_products | json; multiLine=false; native record boundary; source-specific strict validity/size checks before admission | Inferred envelope with hints _source_record_ordinal STRING, _source_record_json STRING, payload VARIANT, _corrupt_record STRING; addNewColumns for envelope only; rescue/corrupt columns. VARIANT inner content is schema-flexible, not typed field evolution. S/C for off/full. |
| OFF DELTA / A(openfoodfacts-jsonl/delta) → bronze.off_products | json; multiLine=false; native source operation envelope; keep complete after/null tombstone | Explicit exact registered D03envelope + SP-01raw-image + after VARIANT + _rescued_data/_corrupt_record STRING; rescue mode (supports first expected-empty DELTA without guessed inference); registered schema updates/restart only. S/C for off/delta; source change ID/version/range remains authoritative. |
| Amazon FULL / A(amazon-metadata-parquet/full)/*.parquet → bronze.amazon_metadata | parquet; native schemas/nesting/nullability; readerCaseSensitive=true, native file metadata | Infer actual native schema with parent_asin STRING hint; failOnNewColumns for fixed original FULL; rescuedDataColumn where supported; independently validate footer/pages/schema. S/C for amazon/full. No loss of original16 native fields or case. |
| Amazon DELTA / A(amazon-metadata-parquet/delta) → bronze.amazon_metadata | parquet; exact registered source-operation/after schema | Inference/native hints per source version; addNewColumns for pre-registered nullable changes; native Delta writer evolves only approved columns. S/C for amazon/delta. Type widening experiment isolated initially. |
| Reviews / A(amazon-reviews-jsonl)/*.jsonl.gz → bronze.amazon_reviews | json; multiLine=false, PERMISSIVE, _corrupt_record retained | Explicit registered10field native payload + SP-01identity/image fields + _rescued_data STRING/_corrupt_record STRING; schemaEvolutionMode=rescue, rescuedDataColumn=_rescued_data. S/C for reviews/json; separate inference/hints/addNewColumns demonstration from same publisher. No loss of unexpected source fields: raw image plus rescue retained. |
| Campbell / A(campbell-dat)/*.dat → bronze.campbell | text; wholeText=true on **bounded** DAT parts; preserve value and native file metadata | Fixed value STRING ingestion; schemaLocation=S configured for the query namespace, checkpoint=C mandatory; no text _schemas field-evolution claim; Auto Loader has no field evolution for text. Registered four-header/parser/family handling is §7.7. S/C for campbell/text. |

Ingestion metadata: actual file_path/name/size/modification_time selected from _metadata, source/instance/generation/delivery/revision/part/schema version/fingerprint/release, original seed SHA/occurrence or operation identity, ingestion/run/consumer-generation lineage, rescued/corrupt classification. File clocks are not original source event time. Native schema inference is discovery, not authority for source key/decimal/nullability/meaning.

PERMISSIVE plus explicit corrupt/rescue columns preserves malformed observations for accounting. Never DROPMALFORMED/skip-corrupt. A type/case mismatch can be rescued without being syntactically corrupt; neither implies business acceptance. Invalid source identity or critical sequence/schema/DQ holds delivery reconciliation/ACK. Expected empty files differ from missing files and may yield zero target rows; source manifest/header/native query evidence establishes that outcome without fabricated DATA.

### 7.7 Campbell native text and minimal TOA5 transformations

Source publishing bounds each DAT part; no huge whole-file allocation is permitted. Native Auto Loader text/wholeText reads a part as one ordered value. Retain its original Raw/part hash. Native Spark column functions split registered line endings, preserve the exact first four header strings, validate them against station/logger/schema-family/units/processing metadata, then posexplode observation lines after those headers. A final file terminator is not a fabricated extra record; an interior blank/malformed observation is accounted/held.

Parse each native observation line with from_csv under that registered family's explicit lexical STRING schema, then retain raw_line, four_headers, family/header hash, zero-based data_position (first observation=0, after the four headers) and source identity. Original source observation ordinal stays one-based. Join source-controlled (part_id,data_position)→original ordinal metadata, or verified contiguous start+position. This position is inside one ordered native file value, not partition/row order. Validate join uniqueness/no missing mappings. No row numbers, content-hash inference, SDK download, external file parser or generic DAT schema evolution.

Native Delta append stores observation rows plus necessary source/header provenance. Header-only valid source parts have zero observations; source_deliveries/evidence retains the exact four headers and expected-empty proof. Silver assigns station-aware types/NTZ/quality/units and revision policy; no universal sentinel→NULL/timezone or field inference. A new logger field/unit changes the registered TOA5 family/parser/Silver policy, not Auto Loader's fixed text schema. [Native text options](https://learn.microsoft.com/en-us/azure/databricks/spark/api-options)

### 7.8 Native schema evolution modes, restart and Delta target contract

The selected main formats above actually use cloudFiles parsing. Isolated mode experiments use the **same source publisher/native files** with dedicated schema/checkpoint/temporary managed Delta targets; never switch destructive modes on the authoritative query or erase its schema/checkpoint. Format/runtime support must be verified; no mode is assumed for text/VARIANT inner fields.

| Mode | Native behavior / selected experiment | Target/restart and business gate |
|---|---|---|
| addNewColumns | Discover an approved new nullable CSV/JSON/Parquet field; update persisted schema, fail, then restart | Verify registered change, enable only scoped approved Delta mergeSchema, resume same C/S; old values remain absent/null. |
| addNewColumnsWithTypeWidening | Approved numeric widening plus nullable addition; unsupported conversions rescued | DBR16.4+ public preview; Delta enableTypeWidening and compatible readers; validate updated S, failure/restart and exact values. |
| rescue | Schema fixed; new/mismatched fields in _rescued_data | Inspect rescue and retained raw; Silver holds/rejects protected semantics. No hidden field adoption. |
| failOnNewColumns | Unexpected new column fails without automatic schema update | Register/update provided schema/read policy, compatible target migration, same checkpoint restart; do not delete source file to hide it. |
| none | Schema unchanged; ignored new fields unless rescuedDataColumn configured | Isolated comparison with and without rescue; prove difference against Raw. Main queries never rely on silent ignore as drift enforcement. |

OFF DELTA uses an explicit registered schema so a first expected-empty JSONL export cannot require a guessed inferred schema. JSON inference/additive-mode demonstrations use the FULL envelope/isolated native source fixtures. CSV/JSON normally infer strings unless inferColumnTypes=true; Parquet retains encoded types. schemaHints guide inference/read types, not business casts. Full explicit schemas cannot use addNewColumns. This plan uses inference+hints for both additive/widening modes, explicit schemas only for rescue/fail/none contrasts; no unsupported full-schema/additive combination is selected. Schema sampling/location persistence must be observed; a sample is not a complete source profile. [Schema configuration](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema)

_rescued_data/rescuedDataColumn and _corrupt_record are retained separately. OFF's payload/after VARIANT hint preserves open nested shapes without column explosion; typed nested evolution is demonstrated on Reviews/registered JSON fixture fields, not falsely advertised inside VARIANT. JSON VARIANT requires DBR15.3+ and supported Delta feature/readers; its documented16MBvalue limit is enforced without truncating source data; oversized/corrupt source remains Raw and holds affected conformance; full raw JSON text remains exact evidence. [VARIANT ingestion](https://learn.microsoft.com/en-us/azure/databricks/ingestion/variant)

Auto Loader physical acceptance is separate from source contract/Silver/Gold compatibility. Added required fields, rename/drop/case/type/semantic changes follow §14 even when native parsing succeeds. One schema-aware task repair owner validates the new persisted schema versus the pinned approved release before restart. Breaking/unknown changes hold, rather than unlimited automatic retries. Widening and Delta protocol are not enabled globally. [Type widening](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/type-widening)

### 7.9 Native Bronze append, complete-delivery reconciliation and ACK

All five source adapters use native checkpointed Delta append after stateless technical projection. Native sink/query IDs own batch retry/commit acknowledgment. Supplier retransmission resolves the original manifest/winning accepted paths; new paths are reserved for an approved new revision. Preserve all legitimate repeated source rows. Revised source observations/serialization revisions remain in Bronze; Silver's source-key/version/revision rules determine logical effects.

Whole packages may span many file batches. Partial Bronze is retained but never eligible for business processing. After successful availableNow completion, finalizer freezes the real Bronze UUID/version, compares exact expected source parts/occurrences/operations/content/multiplicity/DQ and native query evidence, writes deterministic reconciliation_results, and guards source_deliveries→BRONZE_RECONCILED. Required source predecessor/continuity plus actual successful certificate authorizes source ACK. Copy success, native file discovery or a single batch is insufficient.

Control/target writes are not cross-table ACID. A lost finalizer/control/ACK response is repaired from deterministic scope/evaluation and actual target/native evidence after quiescence, even when the next query has no new files. Silver/Gold failure creates separate publication debt, not new supplier operations. No SFTP Bronze custom markers or universal foreachBatch protocol.

Lifecycle: EXPECTED→SOURCE_READY→PLAN_VALIDATED→RAW_VERIFIED→PUBLISHING→ACCEPTED_READY→BRONZE_PARTIAL→BRONZE_RECONCILED→ACKED, with explicit HELD/REJECTED/LATE/MISSING/PENDING_PREDECESSOR/SUPERSEDED. Native checkpoint file progress remains separate from logical delivery state.

### 7.10 Restart, replay and recovery

| Failure | Required outcome / recovery |
|---|---|
| Incomplete copy/invalid hash/manifest | No query/ACK; exact source revision into an unused attempt, compare bytes; no new source identity. |
| Partial accepted publication | PUBLISHING, no query start; repair frozen paths while source query is stopped. |
| Schema discovery stops stream | Inspect persisted S versus approved release; controlled compatible Delta migration/restart with same C/S, or hold incompatible source. |
| Some append batches commit then failure | Keep same native checkpoint/sink; native restart resolves prior commits, finalizer holds incomplete package. |
| Delta acknowledgment uncertain | Prove old query stopped and native log/target outcome; resume same checkpoint. Do not manually append/MERGE guessed retries. |
| Query done/finalizer or ACK failed | Reconcile frozen actual target and repair logical evidence/ACK, including expected-empty scopes; no source refetch if valid Raw retained. |
| Checkpoint/schema state lost | Stop/prove quiescence; coherent supported restore or isolated new consumer target/checkpoint/reconciliation/canonical cutover. Never fresh-checkpoint append all retained files into old target. |
| Wrong decoder/contract version | Preserve Raw/native corrupt/rescue evidence, hold trusted scope; approved migration/isolated replay uses retained historical source version. |
| Data after DEV closeout | Replay rights expire only through explicit closeout declaration; never claim deleted Raw/checkpoints remain recoverable. API retention separate. |

### 7.11 Cadences, retention and native parallelism

REES46 SFTP discovery is **10 minutes** against monthly supplier deadlines. OFF/Amazon metadata/Campbell remain1 minute active; reviews1 minute active/15 minute recovery-only, with original daily simulation cadence. Source-specific task graphs start Auto Loader directly after acceptance, no extra cron.

Native splittability determines parallelism. Publisher emits bounded native CSV/JSONL gzip parts with complete original occurrence ranges; gzip remains unsplittable per part, multiple parts/files run concurrently. Amazon native row groups/files parallelize without conversion; bounded Campbell wholeText parts avoid unbounded file allocation. Source part packaging/identity SP-01 is explicit; more workers cannot accelerate one gzip stream.

SFTP Raw/accepted replay state follows DEV-RET-01 (§12), no365day SFTP WORM. Retain active/failed/unreconciled/recovery-dependent evidence and native checkpoints until authorized closeout. Count source envelope/accepted-copy/storage/validation/Auto Loader work in latency and cost; no measured throughput or SLA claim.

## 8. Open Prices API option audit and selected implementation

Auto Loader discovers cloud files. It does **not** call REST endpoints. ADF also does not need to perform the HTTP extraction merely to appear in the orchestration diagram.

### 8.1 Comparison against the actual contract

| Requirement | A: ADF REST → Raw → Bronze | B: Python → Raw → direct controlled Bronze | C: approved Python → Raw/ledger → accepted receipts → Auto Loader |
|---|---|---|---|
| REST-only history | Possible connector pagination, but custom bounded window/split planner still needed | Exact existing Python planner reusable | Exact existing approved planner |
| Created-time bounds/overlap | Dynamic expressions possible; durable source-specific planning still required | Explicit | Explicit |
| One physical request at a time | Activity concurrency alone does not cover transport retries/old timed-out work | Supervised one-worker/session control | Same approved supervised control |
| Mutable pagination | Generic next-page rules do not prove fixed-window membership | Full validation can be coded | Full approved validation preserved |
| Retry-After/absolute deadline | Standard activity retry is insufficient for all approved persisted attempt/cooldown semantics | Python owns exact policy | Same policy |
| Exact response entity bytes | Connector serializers may transform JSON; must prove byte-preserving behavior | Raw entity capture | Approved raw entity capture |
| Work bounds/time/byte caps | Additional control logic required | Explicit leaf/tick caps | Explicit leaf/tick caps |
| Acquisition authority | Would need a controlled replacement of the approved ledger protocol | Keep one ledger | Keep one ledger |
| Acquisition vs Bronze failure | Must design asynchronous separation | Requires bounded accepted-work discovery and idempotent direct consumer | Already separated by retained receipts |
| Receipt/missed-discovery repair | Not inherent | Must replace with an equivalent durable anti-join planner | Existing acceptance/receipt/Bronze anti-joins |
| Native restart/file discovery | Connector state not the approved native file history | Direct consumer scan/progress must be specified | Native Auto Loader checkpoint |
| Observed overlap/version identity | New implementation required | Preserve accepted occurrence IDs | Preserve accepted occurrence IDs |
| Complexity | Rebuilds custom semantics in ADF+Python or weakens them | Can be simpler, but requires material equivalent recovery amendment | One small retained receipt stream, already specified |
| Cost | ADF activities plus custom processing; not automatically cheaper | Could avoid receipt-consumer discovery overhead | Small scheduled single-node D10 receipt/Bronze consumer plus separate distributed business processing; measure actual startup/DBU cost |
| Existing decision | Replaces D10 | Material removal amendment needed | **Preserves D10** |

**Select C.** Auto Loader independently discovers retained accepted API receipts while acquisition can continue through a Bronze outage. Its callback validates ledger acceptance and reads that receipt's bounded immutable HTTP evidence. SFTP instead discovers actual validated native source files, using native CSV/JSON/Parquet/text parsing, source-published identity and required TOA5 column transformations (§7). PostgreSQL WAL and Kafka do not use Auto Loader.

### 8.2 REST bootstrap and bounded incremental handoff

At initial admission, obtain verified UTC, take latest nominal quarter-hour D0, and freeze H0=D0−5 min. Set tail_anchor=H0 and tail_acquired_through=NULL. Discover earliest currently exposed created timestamp at/beforeH0 using documented created bounds, supported created ordering, size1/page1. Empty discovery is explicit, not an invented historical start.

History uses calendar-month candidate windows clipped to[A0, H0]. Page size100, order_by=id. Recursively split when total>1,000, expected pages>10, decoded total>64 MiB or qualified time budget cannot complete the leaf. Inclusive children[L, M] and[M, H] intentionally re-observe the boundary. An overflowing minimum timestamp bucket becomes CAPACITY_BLOCKED; do not bypass it with export or claim complete history.

Tail has priority over history but shares the single requester. At each tick freezeH=D−5 min. FirstL=tail_anchor−30 min, thenL=last accepted connected tail frontier−30 min. Resume unfinished original bounds before planning later work. All children of a root must accept before its frontier moves. Bootstrap/weekly sweeps do not independently advance tail state. This eliminates the old unbounded stale-export bridge.

Each traversal fixes all parameters except page number; check full envelope consistency, all expected pages/counts, positive integral IDs, strict ID ordering/no repeats, created timestamps inside bounds and exact page/item membership hashes. Count agreement is necessary, not snapshot proof: manifests say **LIVE_ENUMERATION_NOT_SNAPSHOT**.

Weekly Sunday00:00 UTC opens one bounded REST historical sweep only after bootstrap, sharing normal rate budget and not overlapping an unfinished sweep. It observes currently available representations. Old-ID updates between observations, deleted rows and A→B→A transitions never exposed to the reader can be missed. No native CDC, update filter, delete feed or atomic historical snapshot is invented.

### 8.3 HTTP, attempt and work policy retained from D10

| Control | Selected value/behavior |
|---|---|
| Endpoint | Actual documented prices list; not proofs/drafts |
| Session | One persistent supervised HTTP worker, connection pool1, no hidden adapter retries, no unexpected redirects |
| Concurrency / pacing | At most1 physical request active; ≥1 second after prior response finishes |
| Timeouts | Connect10 s, read-idle15 s, absolute70 s enforced by parent; terminate/join worker before next send |
| Body limits |32 MiB encoded and32 MiB decoded per page; stream/hash64 KiB chunks; gzip integrity and strict UTF-8/JSON |
| Leaf bounds |≤1,000items,≤10pages,≤64 MiB decoded |
| Tick envelope |480 s total:300tail,150history,30finalization; actual persistent ledger I/O counts in work time |
| Attempts | Six transient attempts; malformed200 at most two body retries inside that budget |
| Retry conditions | Network/timeout/429/5xx; honor valid Retry-After minimum;400 stops invalid request;401/403 incident; no blind redirects |
| Breaker | Six exhausted transients or two consecutive429; initial max(15 min, Retry-After), one half-open probe; repeated cooldown up to1 h except longer provider delay |
| Traversal restarts | At most3per work/24 h before explicit hold |
| Uncertain request outcome | Reserved attempt remains consumed; lost response headers imply conservative1 h cooldown and recorded uncertainty, not proof of an unknowable Retry-After |
| Authentication | Use only provider-required supported authentication; no fabricated API secret requirement for a public GET. Azure storage SDK authentication is separate. |

Preserve encoded HTTP entity bytes after framing removal, not TLS/chunk packets. Keep separate decoded hash/size, status/complete flag and sanitized response headers. Strict JSON rejects duplicate keys/nonfinite values. Token-aware extraction retains exact item substrings; reserialized JSON/VARIANT is not original byte evidence. No public-provider fault injection or load testing.

### 8.4 One acquisition ledger and no competing watermark

One managed Delta `control.acquisition_ledger`: one mutable HEAD, immutable deterministic COMMAND results and EVENT rows. Key(environment, row_id); record_type HEAD/COMMAND/EVENT; revision, writer_epoch, command_id/hash, work/root/traversal/page/attempt identifiers, fixed bounds, manifest URI/hash, payload JSON/hash, actual job/run identity and recorded UTC.

HEAD retains table UUID, one owner job/run/compute identity, revision/epoch, bootstrap generation/horizon/floor/state, tail anchor/frontier, active weekly generation, due-slot outcomes, next request eligibility/breaker and pinned release hashes.

A command validates exactly one HEAD, expected revision/hash/epoch and actual admitted owner. One Serializable Delta transaction updates that HEAD and inserts its command/events. A stale/missing guard must abort **all** inserts; “conditional update did nothing” is not enough. Initialization is deployment-controlled once. Native Delta uniqueness is not assumed. Concurrent-contender integration proof remains a P0 gate.

Only one configured acquisition Job; max_concurrent_runs1, queue disabled, no automatic whole-job HTTP retry. Manual/backfill/recovery use that same Job. No lease-expiry takeover: prove old run terminal and its job cluster terminated, wait the D10 90 s margin, then claim a guarded new epoch. API acquisition identity alone can mutate this ledger.

For an uncertain MERGE, establish old operation termination and look up deterministic command/hash. Matching row returns prior result; absent result after definite termination may retry from current HEAD; conflicting bytes or duplicated HEAD stops recovery. No Azure SQL or extra watermark table.

### 8.5 Acceptance, receipt consumption and crash recovery

Ordered acceptance:
verified complete bodies → immutable page evidence/manifests → complete traversal/window manifest → deterministic receipt bytes prepared → one ledger WORK_ACCEPTED/command/HEAD transaction → read-back acceptance → conditional receipt publication/read-back → RECEIPT_PUBLISHED event. ROOT_ACCEPTED and its frontier effect commit together only when connected required children accept.

Auto Loader monitors **only** flat `.../openprices/ready/v1/<acceptance_id>.json` accepted leaf receipts. It does not monitor raw page folders, prepared/rejected manifests, roots or exports. Directory listing, explicit binary receipt schema, include existing files on a new checkpoint, availableNow and at most8receipts per micro-batch are retained. Receipts≤16 KiB; each window≤64 MiB. No short maxFileAge, modifiedAfter shortcut, cleanSource or lifecycle deletion of its checkpoint.

The receipt must match actual ledger table UUID/acceptance revision, exact fixed receipt bytes, work/manifest hash/count/membership. Prepared evidence or a forged receipt is not acquisition acceptance. Processing one accepted leaf inserts ITEM rows (including classified DQ failures) and WINDOW_COMMIT with guarded WRITER_HEAD in one Bronze transaction; no duplicate by same accepted occurrence on retry. Failures propagate out of foreachBatch, which is at least once and therefore needs this target idempotency. [foreachBatch guarantees](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/foreach)

Observation identity retains D10's canonical hash inputs: environment, original acceptance_id, page-manifest SHA, selected attempt_id, zero-based item ordinal and raw-item SHA. Same receipt/replay gives same IDs; another accepted overlap/sweep gives new observations. Content hash is comparison evidence, not primary identity. Source price.id remains the business entity key.

| Crash / debt | Recovery without false progress |
|---|---|
| Download/upload incomplete | Attempt consumed; retain incomplete evidence; no acceptance/receipt |
| All complete pages registered | Verify and seal frozen pages offline; no unnecessary GET |
| Acceptance committed, receipt absent | Recreate exact fixed receipt from accepted event; no source refetch |
| Receipt exists, publication event missing | Verify bytes and record event idempotently |
| Bronze window committed, callback/stream checkpoint not completed | Retry receipt, verify same marker/rows and return existing commit; checkpoint completes afterward |
| Auto Loader checkpoint lost | Preserve damaged state; stop writer/prove termination; new consumer generation discovers all retained receipts; target IDs/markers prevent reapplication |
| Poison/unauthorized immutable receipt | Fail batch, investigate; reviewed exclusion of exact path/hash only, never generic ignoreCorruptFiles |
| Ledger lost | Restore valid table/HEAD/events within retained files; otherwise declared new recovery ledger/generation with full evidence reconciliation |

Let A=accepted leaves, R=valid matching receipts, B=verified Bronze windows, F=native file history. A−R is publication debt; R−B is consumer debt; R−A is unauthorized evidence; B−A is an integrity failure. F alone proves neither acceptance nor Bronze completeness. Daily anti-joins plus each scheduled repair tick detect debt. Shared controls mirror these outcomes but do not advance the API frontier.

### 8.6 ADF admission and independent API downstream orchestration

ADF owns two independent schedule bindings under OA-API-02: 15-minute acquisition and 5-minute independent receipt/business admission. The corresponding native Job cron schedules must be absent/paused in the approved deployment; this document phase changes no runtime configuration. The selected OA-API-02 bindings require the same configured D10 Jobs with their actual runtime identities, single-node acquisition/receipt compute, max_concurrent_runs=1, queue disabled and unchanged ledger/native checkpoint boundaries.

An acquisition dispatch passes only allowlisted source/dataset, release, bounded run mode, nominal due-slot and ADF correlation. Normal created-time bounds and catch-up work are planned from the ledger, never supplied as an ADF watermark. Databricks owns one-request HTTP execution, deadlines, persisted retry/breaker, immutable evidence and atomic acceptance.

The receipt dispatch does **not** depend on the current acquisition pipeline succeeding. It drains legitimate retained accepted receipts and repairs window completion independently, then dispatches the distinct eligible distributed api_business Job for Silver/G06. D10's small receipt-to-Bronze Job retains its own single-node execution and window commit/checkpoint. ADF monitors A/R/B/publication debt and initiates controlled replay through the existing configured acquisition/receipt Jobs and the separately designated business owner; it does not re-fetch accepted data or read the API through SFTP.

ADF timeout/response loss requires run-ID/token reconciliation and actual old-owner termination; it cannot bypass D10's90-second margin or uncertain-attempt cooldown. Missed/queued ticks retain their outcome and coalesce from durable accepted coverage. The30/15/45-minute acquisition/Bronze objectives include ADF dispatch/queue time. Separate Silver/Gold/consumer budgets include api_business dispatch/startup and do not reuse the receipt budget.

### 8.7 Verified API behavior, Python/PySpark boundary and OA-API-03

Selected endpoint is GET https://prices.openfoodfacts.org/api/v1/prices; [production documentation](https://prices.openfoodfacts.org/api/docs). Current official [view](https://github.com/openfoodfacts/open-prices/blob/main/open_prices/api/prices/views.py), [filters](https://github.com/openfoodfacts/open-prices/blob/main/open_prices/api/prices/filters.py) and [pagination](https://github.com/openfoodfacts/open-prices/blob/main/open_prices/api/pagination.py) code support public GET, page/size with maximum size100, envelope items/page/pages/size/total, ID default ordering and order_by fields id/price/date/created, and created__gte/created__lte filters. Current pagination source caps page number500; D10's≤10-page leaves are stricter. No updated watermark/snapshot token/delete feed is inferred.

VERIFIED refers to official documentation/source inspection, **not a successful live extraction**. The web reader could not retrieve the live schema/sample response during this review. Pin the deployed schema/source fingerprint and legitimately verify endpoint/filter/envelope/error behavior read-only before activation; no provider load/fault testing. Provider-wide rate allowance is not established;≥1 second serialized pacing is selected D10 client policy, not a promised provider quota/SLA.

ADF REST Copy would require rebuilding D10's planner, evidence bytes and guarded ledger/attempt recovery. Distributed PySpark HTTP would create multiple executor requests/retries and uncontrolled provider concurrency while paying for idle workers. Retain supervised Python **only for serialized HTTP/control**, bounded disk streaming and small ledger transactions. Do not parallelize provider GETs across executors.

OA-API-03 separates large downstream work: D10 configured acquisition and receipt/Bronze Jobs remain single-node as specified; api_business uses the two-worker INCREMENTAL/appropriate HISTORICAL profile for accumulated Silver conformance/DQ/observation history and G06 affected-group computation. One five-minute ADF receipt/business admission first repairs/drains accepted receipts independently of acquisition, then submits ready business debt to its sole configured owner. Downstream failure leaves immutable accepted work/Bronze intact; a subsequent same-owner admission repairs it without HTTP refetch. No extra source frontier, physical table, polling clock or competing G06 writer.

## 9. Enterprise-wide ADF orchestration and exclusive execution ownership

### 9.1 Selected orchestration pattern

**ADF coordinates eligible bounded source workflows and selected domain-product workflows. Databricks Jobs owns each submitted workflow's distributed task graph and data retries. Native services own continuous producer/connector/stream execution.**

ADF is not limited to file movement: it owns database onboarding/article scheduling, five SFTP acquisition/Auto Loader lifecycles, both API schedules, cross-input product dispatch, reconciliation, SLA checks and operator-approved bounded recovery. It does not issue API pages, poll CDC tables, run individual Kafka transactions or invoke activities for Spark micro-batches.

One configured ingestion/job owner per source/query scope; one designated writer per Gold product. ADF transport retries reuse the same execution intent/token. They do not create another data run while the existing one is active or uncertain.

### 9.2 Mandatory ownership matrix

| Source or processing stage | Scheduling owner | ADF responsibility | Databricks/connector responsibility | Authoritative state | Retry/recovery owner | Technical justification |
|---|---|---|---|---|---|---|
| PostgreSQL source initialization | Explicit ADF onboarding run, source owner authorizes mutations | Dependency checks, invoke approved source-init action, wait for exact source import receipts/counts | Source importer loads real PostgreSQL; no direct CSV-to-Bronze | Source import/action receipts, actual DB constraints | Source importer/operator; ADF reattaches execution | Source CSVs initialize the database rather than bypass its interface |
| PostgreSQL initial snapshot | ADF finite onboarding workflow | Seal article baseline; activate one native Debezium initial snapshot; start/observe capture; enforce snapshot/cutover gates | JDBC captures sealed article baseline; Debezium consistently snapshots customers/purchases then WAL | Source seal/J0; connector snapshot/slot/offsets; native Bronze commits | Source/connector and their owning Jobs | ADF coordinates FULL without introducing a second inconsistent snapshot authority |
| H&M articles incremental | ADF every 15 minutes | Submit exact bounded journal Job, monitor completion/reconcile/ACK; no native article cron | Job reads sealed PostgreSQL members, writes Bronze and article Silver; emits completed stage | Source journal/seal/ACK and actual target markers | Job data retries; ADF submission/observation | Fixed journal membership, not timestamp/MAXsequence polling |
| PostgreSQL Debezium CDC | Native supervised connector | Once-per-window initialization/readiness, monitoring and controlled recovery only | Continuous pgoutput snapshot/WAL, native transaction metadata/offsets | Slot/WAL and Connect internal topics | Connect/source operator | No unnecessary scheduled restarts or ordinary table polling |
| H&M Kafka Bronze | Native continuous Job | Coordinate initial dependency readiness, observe status/lag, request controlled recovery | Native Kafka-to-Delta capture/checkpoints | Native query checkpoints and Delta commits | Databricks native restart | Per-event/micro-batch ADF would add no correctness |
| SFTP discovery and Raw staging—all 5 | ADF source-specific cadences | READY/manifest/preflight, exact membership/revision check, actual SFTP Binary Copy, validation dispatch | Existing validation task streams landed size/hash/schema | Supplier source manifest; verified Raw; source_deliveries | ADF transport retries; validator holds invalid input | Genuine SFTP acquisition, no Blob-to-Blob shortcut |
| SFTP accepted-file publication | Sole ADF source workflow | Copy/freeze exact validated native files into unused accepted paths while query stopped; guard ACCEPTED_READY | Databricks native validation; no ingestion downloader/Spark source re-reader | Raw + source manifest/accepted-path set and ACCEPTED_READY state | Same source Job, guarded transitions | Excludes incomplete Raw and preserves original ordinals before cloudFiles |
| SFTP Auto Loader Bronze | ADF directly after ACCEPTED_READY; no second cron | Pass source/scope/release, invoke availableNow Job, verify finalizer/reconciliation, coordinate ACK | Native CSV/JSON/Parquet/text parsing and append/checkpoint; necessary stateless provenance/TOA5 transforms; logical-delivery finalization | Native checkpoint + target UUID/version/reconciliation certificate + source_deliveries | Native query/task recovery; ADF reattaches | Native file progress and source package completeness are different |
| Open Prices acquisition | **ADF every 15 minutes only** | Run approved acquisition Job with bounded mode/due-slot/correlation; observe actual state; no provider Copy | Serialized Python, attempt budget, evidence/ledger/acceptance/receipt repair | Single acquisition_ledger | Same acquisition Job; ADF submission recovery | Preserve D10 technical ownership while extending enterprise scheduling |
| API accepted receipts→Bronze | **ADF every 5 minutes independently** | Invoke existing receipt workflow regardless of last acquisition outcome; monitor A/R/B debt | Auto Loader on API receipts, bounded decode, atomic window capture | Native receipt checkpoint + Bronze window markers | D10 receipt Job; distributed business owner separately | Accepted work remains consumable through an acquisition outage |
| Kafka producer | Native host service +60×clock | One-time approved bootstrap/config/registry readiness; monitor progress; bounded replay/recovery requests | Independent source reader, transactions/fencing/control progress | Kafka committed producer summaries/progress | Producer/supervisor, source operator | ADF never handles individual records/transactions |
| Kafka continuous Bronze consumer | Native continuous Job | Start once after readiness, monitor/failure escalation, controlled recovery | Native Kafka connector/read_committed/Delta append/checkpoint | Native query checkpoint | Databricks; exclusive recovery operator | No ADLS or Auto Loader in Kafka path |
| Kafka Silver/Gold | Native business query30s, same source-window Job lifecycle | Start/monitor/recovery only, no per-batch ADF | Decode/conform/idempotent source-ID MERGE then independent G08 affected-group publication | Native business checkpoint + actual Silver/Gold commits/reconciliation | Same Kafka business task; restore/isolated replay | Genuine independent streaming analytics, D11 Bronze unchanged |
| Bronze→Silver, SFTP/API-business/articles | ADF admits bounded source workflow | Submit only eligible source scope; observe actual stage outcome/debt | Internal task dependencies and source-specific DQ/CDC/SCD | Reconciled SFTP delivery certificate or D10 window/native transaction proof and input versions; Silver commits | Owning processing Job | ADF does not independently re-trigger the same internal Silver task |
| Bronze→Silver, H&M CDC | Native60-second conformance task | Platform monitoring and bounded repair/cutover coordination | Complete native transaction membership/order; apply customer/purchase state | Native CDF/checkpoint + source operation/stage evidence | Native conformance Job | Low-latency continuous dependencies stay close to streaming data |
| Silver→Gold G01/G02 | Native H&M conformance task | Observe publication/freshness; controlled bounded repair only | Sole writer; react to complete CDC and committed article history changes | Bound Silver versions + product commits | Same H&M task | Avoid source article and CDC workflows concurrently writing purchase products |
| Silver→Gold G03/G05/G06 | ADF-owned source workflow | Submit source workflow; observe product task/reconciliation | REES SFTP/Campbell source-product task; API's separate distributed api_business Job | Eligible source Silver and product publication | Same workflow | One owner per bounded source/business stage; API's deliberate separate compute boundary is explicit |
| Silver→Gold G04/G07 | ADF domain-product dispatch, active 1-minute debt check | Admit ready Amazon review/catalog-quality scopes; freeze input readiness; no duplicate source-owned product writer | One designated Job per product, distributed transform/reconcile/publish | Required Silver versions + product marker | Domain-product Job | Shared inputs/products require one owner and explicit multi-input readiness |
| DQ and reconciliation | Inline stage dependencies +ADF scheduled audit | Initiate independent bounded audits, monitor outcomes/holds | Original source-scoped predicates and distributed exact comparisons | Four shared controls/evidence + actual target/source identities | Owning check/task; DE operator for integrity hold | A pass must reference real evaluated scope; audit cannot advance native offsets |
| SLA/freshness monitoring | ADF5-minute platform check; native alerts continuous | Due/calendar/debt/product checks, escalate, observe missed/queued runs | Existing status/check tasks, Spark/connector/broker metrics | Actual timestamps, controls and native telemetry | Named source/DE/business owners | Successful ingestion does not imply fresh Gold/consumer |
| Manual replay/backfill | Explicit ADF operator invocation | Bound source/generation/range/release/target; enforce overlap guard; coordinate pause/recovery | Existing adapter/Job mode, native state or isolated rebuild | Original source/progress authority + declared recovery scope | Same owner after exclusivity/terminal proof | No new offsets database or parallel scheduler |

The same practical ADF source pipeline is parameterized for all five SFTP datasets. Native cloudFiles parsing/options and minimal technical transformations are declared per source; shared orchestration is not duplicated into five unrelated control systems.

### 9.3 Seven reusable ADF pipeline patterns

| Planned pipeline | Native activities and bounded purpose | Explicit ownership boundary |
|---|---|---|
| pl_sftp_discover | Schedule → Lookup immutable release → Filter due active sources → bounded ForEach/GetMetadata/READY control lookup → Execute acquisition | No data parsing/business logic or source operation generation |
| pl_sftp_acquire | Control-object Copy → preflight Job → IfCondition/exact Binary Copy ForEach → processing/Auto Loader Job → verify delivery evidence → correct SFTP ACK | Owns between-service handoffs; Job owns prepare/AL/finalize/Silver/product task graph |
| pl_pg_bounded | Manual onboarding or15-minute article trigger; database/source readiness checks; invoke approved initialization/snapshot/journal mode; Until actual completion | Does not poll customers/purchases for changes or restart Connect every tick |
| pl_api_dispatch | Two parameterized schedule bindings: acquisition15 minutes and receipts5 minutes; run existing configured Job with fixed mode/release/correlation | No provider HTTP loop or second API cursor; no success dependency from acquisition to receipts |
| pl_job_run | Web run-now with deterministic token → Until/Wait/runs-get → qualified terminal output; failure/completion branches | Same token on submission uncertainty; no blind new data attempt |
| pl_product_or_audit | Product/readiness/DQ/reconciliation/SLA mode; Lookup dependencies → invoke the designated bounded Job → publish/alert branch | Only selected product IDs/scopes; does not compete with source-owned/native product writers |
| pl_replay_recover | Explicit source/query/recovery generation and bounded scope; pause/terminal checks → existing recovery Job → reconcile/certify → controlled resume | Manual/approved recovery, not a general custom scheduler |

Use shared SFTP delivery/ACK, lake/Key Vault and Databricks workspace connections; PostgreSQL read/source-operation references are separate appropriate bindings. ADF managed identity authenticates allowed Azure/Databricks management actions; source DB/SFTP/Kafka credentials remain in Key Vault or protected source runtime state.

For private Connect/bootstrap control, use an approved existing source-host management action such as the predeployed lifecycle command invoked through Azure management, or an explicit operator gate. Do not expose Connect REST8083 or add a custom lifecycle service. ADF observes actual connector/snapshot state through existing read-only operational checks.

Use Web→Databricks Jobs run-now for selected classic multi-task Jobs and token control, not an assumed serverless-only shortcut. ADF Web/Lookup outputs stay below documented limits; return small plan/status/evidence references, never source populations. No Mapping Data Flow cluster, Azure SQL metadata DB, Function, Event Grid or external scheduler is added.

### 9.4 Git source configuration, ten-entity routing and release use

**Git defines approved static policy; managed Delta controls record actual execution/state.** Native checkpoints/source journals/Connect offsets/Kafka producer progress retain their separate authorities. The historical catalogue's presence does not make it current (§2.1).

Planned organization after coding approval: source-contracts/catalogue.json for entity registry/schema/behavior/policy references; source-contracts/schemas/ for versioned native/Avro descriptors; source-contracts/policies/ for consolidated compatibility, DQ, delivery/SLA and retention policy; orchestration/routes.json for finite ADF/Job/dependency/resource bindings. These are logical organization decisions, not files created here. Inspect/reconcile existing source-contracts/contracts.v1.json first; no blind overwrite/migration. Eleven categories need not become eleven files.

| Static Git category | Exact content / relationship to runtime |
|---|---|
| Source catalogue / registry | Stable source/entity IDs, interface and approved adapter; runtime source identity must match, never infer routing from filename alone |
| Schema registry | Native schema IDs/ordinals/descriptors/fingerprints and decoder versions; real D11 Avro subject/actual Registry ID bindings remain in Kafka Registry plus pinned release |
| Schema contract | Required/nullable/presence/key/type/nested/semantic requirements; original Word authority/hash and amendments |
| Source behavior contract | FULL/journal/CDC/occurrence, version/update/delete/revision/ACK/source-progress semantics |
| Schema compatibility policy | Backward/forward/full/transitive rules, protected/open fields and explicit version classification |
| Schema drift/evolution policy | Discover/compare/hold/quarantine/approve/decoder migration/target protocol/replay and rollback |
| Static DQ contract | All275 identified rules plus approved OFF category mapping; predicate, scope/severity/tolerance/disposition/owner/oracle; dq_results contains executions only |
| Delivery/freshness SLA/SLO | Source calendar/service window, trigger owner, due-slot and stage/consumer targets; runtime misses are actual outcomes |
| Retention/replay agreement | Native/source/evidence/Delta horizons, ACK-dependent source GC, decoder availability and recovery gates |
| Operational delivery metadata | Source manifest/READY/ordinal-map/CRC/SHA/range/transaction/operation/schema/generation/revision/ACK specifications |
| Pipeline configuration | Approved connections/paths/targets/Jobs/task dependencies, profile/retry/deadline/concurrency/replay and release bindings |

**Normative common entity record:** source_id, dataset_id/entity_id, interface, adapter_ref/version, contract_authority/hash/amendments, registered source_schema_ref/ordinal/fingerprint, connection_ref, credential_ref, source_locator, cadence/trigger_owner/service_window, raw_root, accepted_root, optional_delta_staging=NULL, bronze_target, silver_targets, gold_targets/owner, dq_refs, drift_policy_ref, identity/update/delete policy, scd/tracked_set_ref, native_progress_ref, retry/timeout/concurrency profile, reconciliation/ACK/replay policy and immutable release_id/hash. Combine this common record with each row below; every field is required/resolved or explicitly NOT_APPLICABLE. A deployment-generated Job ID/topic UUID/account name/fingerprint is never invented: release validation must resolve it from actual approved bindings before activation.

| source_id / entity_id (selected stable routing IDs) | Source locator / authority / schema | Adapter and native progress | Bronze → primary Silver → Gold dependencies |
|---|---|---|---|
| PG-HM / articles | Actual PostgreSQL articles + sealed article journal; D01R3 R01–R10; registered baseline/journal versions | pg_articles_sealed_v1; source journal seal/J0/ACK | bronze.hm_articles → silver.hm_article_history → native G01/G02 enrichment |
| PG-HM / customers | PostgreSQL customers → actual Connect customer topic; D01R3, runtime-pinned Debezium envelope | hm_cdc_native_v1; source slot/Connect offsets + native consumer checkpoint | bronze.hm_customers_cdc → silver.hm_customer_history → G01/G02 |
| PG-HM / purchases | PostgreSQL transactions_train → actual Connect purchase + transaction metadata topics; D01R3 | hm_cdc_native_v1; same connector continuity, distinct native capture checkpoint | bronze.hm_purchases_cdc → silver.hm_purchase_current → G01/G02 |
| FILE-REES / rees46_sftp | supplier-deliveries/dev/rees46-csv/...; D02R1, CSVv1/approved Aprilv2 + Novemberr2 | sftp_csv_native_sp01; persistent native CSV file/schema checkpoint | bronze.rees46_sftp → silver.rees46_events → G03 only |
| FILE-OFF / off_products | .../openfoodfacts-jsonl/...; D03R1, open FULL/registered DELTA v1/v2 | sftp_json_native_sp01; native file checkpoint; source journal separate | bronze.off_products → silver.off_product_current → G07 OFF domain |
| FILE-AMAZON / amazon_metadata | .../amazon-metadata-parquet/...; D04R1, native FULL/DELTA schema families | sftp_parquet_native_v3; separate FULL/DELTA native checkpoints, one source Job | bronze.amazon_metadata → silver.amazon_product_current → G04enrichment/G07Amazon |
| FILE-AMAZON-REVIEWS / amazon_reviews | .../amazon-reviews-jsonl/...; D05R1, raw original/review fixture versions | sftp_review_json_sp01; native JSON file/schema checkpoint; delivered original global ordinal | bronze.amazon_reviews → silver.amazon_review_occurrences → G04 |
| FILE-DAT / campbell | .../campbell-dat/...; D06R1, seven station bindings/four families/approved GNLA2v2 | sftp_text_toa5_sp01; native file checkpoint, independent station continuity | bronze.campbell → silver.campbell_observations → G05 |
| API-PRICES / prices | GET https://prices.openfoodfacts.org/api/v1/prices; D07R1 + D10R2, source runtime schema pin | openprices_rest_d10; control.acquisition_ledger; receipt checkpoint separate | bronze.openprices_capture → silver.openprices_observations → G06 |
| STREAM-REES / rees46_kafka | Actual D11 behavior/progress topics and Registry subject; D08R1+D11R2 + KSG-01/DEV-KAF-01 | rees46_native_kafka_r2 capture + rees46_kafka_business_v1; committed producer progress + separate native capture/business checkpoints | bronze.rees46_kafka → silver.rees46_kafka_events → G08 independent Kafka streaming analytics |

| Entity | Scheduler / actual future Job binding | Connection/credential / landing | DQ/SCD/retry/readiness/ACK/replay specialization |
|---|---|---|---|
| articles | ADF15 minutes, logical hm_articles_bounded Job; onboarding explicit | PostgreSQL read-only TLS secret in Key Vault; sealed DB evidence; no accepted files | D01HM-JNL/HM-ART/ALL; article tracked-set SCD2; one Job, bounded task retries; exact sealed members/target certificate before ACK; replay same source seal |
| customers | Native Connect + continuous hm_capture, native60s hm_conform; ADF lifecycle only | Protected DB replication/Kafka credentials; native Kafka envelope in Bronze, no file landing | D01HM-CUS/CDC/ALL; customer SCD2; complete source transaction/schema first; native offsets/isolated resnapshot, no receiver per-message ACK |
| purchases | Same native capture/conform owners; distinct target scope | Same protected source/consumer bindings; no Raw/accepted SFTP prefix | D01HM-TRX/CDC/ALL; ordered current purchase state; original occurrence/FK/exact decimal; same native recovery, no SCD2 fact/ACK |
| rees46_sftp | ADF active10 minute discovery + direct finite rees_sftp_source Job | Shared receiver SFTP key/host trust; raw/dev/rees46...; staging/dev/sftp/rees46.../accepted/ | REE001–036; revision/occurrence policy; one source Job; exact month+revision original ordinal/multiplicity certificate then ACK; native restart/isolated replay |
| off_products | ADF active1 minute discovery + off_source Job | Shared SFTP key; source-specific Raw/accepted roots | D03R07 categories mapped before activation; versioned current/exception views, no SCD2; exact journal range/canonical+exception membership; contiguous ACK/replay |
| amazon_metadata | ADF active1 minute + amazon_metadata_source Job | Shared SFTP; native Parquet Raw/accepted FULL and DELTA roots | AMZ-DQ001–036; ordered current state/case-sensitive keys; same source owner serializes both native queries; exact snapshot/operations then ACK; isolated new-checkpoint replay |
| amazon_reviews | ADF active1 minute,15 minute recovery-only as contracted + review_source Job | Shared SFTP; original JSONL Raw/accepted + source ordinal metadata | REV-001…036; original occurrence/fixture lifecycle; no dedup/SCD2; exact FULL/day disjoint coverage then ACK; same ordinal replay |
| campbell | ADF active1 minute + campbell_source Job | Shared SFTP; DAT Raw/accepted, original header references | DAT-DQ-001…040; independent station/epoch/schema/revision; exact row/header/zero-day reconciliation then ACK; replay station/date |
| prices | ADF15 minute acquire, independent5 minute receipt; finite distributed api_business stage | No API credential for public GET; separate approved Azure SDK identity; HTTP evidence/accepted receipt roots (§8/12) | D07/D10 all 45 rules; observed history not CDC/SCD2; D10 no whole-extractor retry, bounded HTTP budgets; ledger acceptance/receipt/atomic Bronze proof, no supplier ACK; never refetch accepted data for business repair |
| rees46_kafka | Native independent producer60× + native Bronze10s/business30s queries; ADF bootstrap/monitor/recovery only | Private real KRaft/Registry TLS/SASL, credentials in Key Vault; no ADLS event landing | KAF-DQ-001…046; D11 native append/inspection/source reconciliation unchanged; KSG-01 source-ID conformance, late/duplicate physical accounting and independent G08; separate checkpoints, replay/cutover and explicit DEV availability deviation |

Future exact Job IDs/linked-service/account/topic UUIDs are deployment bindings, not new architecture choices. SFTP task retries follow §7/9.5; configured owning Job concurrency=1, source-specific bounded deadlines and actual quiescence required. Source-specific operational timeouts depend on representative/full input scope, never apply an8 minute API timeout to a huge gzip.

**Representative configuration records (architecture notation, not executable configuration files):**

| Interface | Representative values and selected policy |
|---|---|
| PostgreSQL | source=PG-HM; entity=articles; adapter=pg_articles_sealed_v1; authority=D01R3; locator=articles journal/seal; schema=registered D01baseline/journal; schedule=ADF15m; connection=pg_read_tls; credential=KeyVault:pg-reader; raw=sealed extraction evidence; staging=NULL; target=bronze.hm_articles; Silver=hm_article_history; progress=PostgreSQLseal; ACK=exact sealed membership; SCD=article tracked-set; retries=guarded task repair; JobBinding=required actual hm_articles_bounded ID |
| SFTP | source=FILE-AMAZON; entity=amazon_metadata; adapter=sftp_parquet_native_v3; authority=D04R1+receiverR3; schema=registered native FULL/DELTA; source=shared SFTP amazon-metadata-parquet path; connection=supplier_delivery_sftp; credential=KeyVault:sftp-receiver-key+host-key; schedule=ADF1m active; raw=native Raw; accepted=source FULL/DELTA roots; staging=NULL; sink=native append; target=bronze.amazon_metadata; Silver=amazon_product_current; Gold=G04/G07 designated owners; progress=FULL/DELTA native Parquet C/S scopes; ACK=whole logical revision certificate |
| REST | source=API-PRICES; entity=prices; adapter=openprices_rest_d10; endpoint=prices GET; authority=D07+D10; auth=public_read/noAPIsecret; schedule=ADF15m + independent5m; evidence/receipt roots=§12; staging=NULL; target=bronze.openprices_capture; Silver=observations; Gold=G06; progress=one acquisition_ledger; retry=D10 persisted budgets; Jobs=actual configured acquire/receipt IDs + bound distributed business ID |
| Kafka | source=STREAM-REES; entity=rees46_kafka; adapter=rees46_native_kafka_r2; locator=actual behavior/progress topic UUIDs; schema=real AvroR2 Registry subject/IDs; credential=KeyVault producer/consumer/registry refs; schedule=native60×/Bronze10s/business30s; landing/structured staging=NULL; target=bronze.rees46_kafka; Silver=silver.rees46_kafka_events; Gold=G08Kafka; progress=committed producer record/separate native capture-business checkpoints; isolation=read_committed; retries=D11/KSG-01; hosting=DEV-KAF-01private KRaft |

All records inherit the common required fields, exact rule/policy/schema references, source lineage, timeout/concurrency/replay and release hash. Placeholder bindings are unresolved gates and **cannot pass release validation**.

Release process: resolve Git commit + canonical metadata/schema/DQ hashes + D01–D11/overlays + actual binding inventory → validate all ten records and adapter/target/path allowlists → publish immutable small JSON release to existing state/dev/releases/<release_hash>/ → pass its URI/hash to ADF and Databricks. Both verify exact bytes/hash, required fields and source/target identity independently. ADF Lookup/GetMetadata use bounded outputs; full manifests/populations remain referenced evidence/distributed reads. No arbitrary SQL/Python expressions from metadata.

### 9.5 Bounded Job submission, retries and concurrency

The logical source scope is stable across retries. An execution intent adds phase and attempt ordinal; its≤64-character hash becomes the Jobs idempotency token. Acquisition/preflight/processing are different phases, but a transport retry of the same submission uses the same token. Correlate ADF pipeline/run, execution intent, actual Job/run and source scope in existing ingestion_runs and native activity outputs.

ADF owns **submission/observation transport retries**. Databricks owns **task/data retries** and source-specific idempotency. If an ADF activity times out while the child is active, mark ORCHESTRATION_UNKNOWN and reattach; cancellation of the parent does not prove child termination. Reuse the same run token; never launch an overlapping replacement.

After a definite terminal failure, prove data-writer/Spark/SDK quiescence (or terminate the shared cluster if proof is unavailable), inspect target/native state and use controlled task repair or a new execution intent only after old-writer termination. API acquisition keeps D10 max_retries=0 at whole-Job level and persisted per-request retry/breaker budgets; ADF does not override them with generic whole-extractor retries.

Each configured owning Job has max_concurrent_runs=1; API retains queue disabled. ADF pipeline concurrency is bounded. ADF can queue trigger instances: record queue age and have stale runs coalesce or return NO_DUE_WORK based on the actual source authority. Do not claim a missed slot executed successfully. The next API admission plans from its ledger/frontier and current safe horizon, not the old queued trigger's guessed window.

### 9.6 Source workflow task boundaries

| Source workflow | Internal Databricks task graph | ADF completion/readiness decision |
|---|---|---|
| SFTP source | Verify Raw → verify/publish native accepted files → availableNow Auto Loader → delivery finalizer/reconciliation → eligible source Silver → source-owned product if assigned | Require actual delivery completion/ACK boundary; product debt tracked separately |
| Articles | Admit sealed baseline/journal → PostgreSQL capture → exact source/Bronze reconcile → ACK → article SCD2/stage receipt | No direct G01/G02 write; native H&M product owner reacts to committed article history |
| API acquisition | Admit same configured owner → bounded REST planning/request/acceptance/receipt repair → due-slot outcome | Record acquisition outcome only; do not gate receipt pipeline on it |
| API receipts then business | D10 single-node receipt Auto Loader → verified window completion; ADF dispatches eligible debt to distributed api_business Job → Silver → G06 | Separate stages/compute, one owner each;5-minute ADF admission repairs Bronze-to-business debt even if the prior downstream dispatch failed |
| H&M continuous | Native capture plus independent native conformance/product task | ADF starts/monitors lifecycle, not per-trigger data task dispatch |
| Kafka continuous | D11 native direct capture/inspection/append checkpoint plus independent Kafka Silver/G08 business task/checkpoint | Actual source/capture/business progress/reconciliation; no file staging or per-trigger ADF |
| Domain G04/G07 | Admit eligible debt → freeze required Silver versions → distributed business transform → exact reconciliation → one product publish | ADF owns this bounded workflow only; source workflows stop at Silver for these shared products |

For a complete SFTP source revision, a later Silver/Gold failure never invalidates Raw or requires another supplier business operation. A retry must inspect target completion and continue the failed stage. Capture ACK follows its source contract; business publication/freshness has its own outcome.

### 9.7 Single Gold writer and cross-source coordination

| Product | Sole normal writer | Trigger/dependency behavior |
|---|---|---|
| G01hm_purchase_fact / G02hm_purchase_daily | Native H&M conformance/product task | Complete source transactions plus committed article-history changes; preserve pending-article policy and repair affected groups |
| G03rees46_activity_daily | REES46 SFTP processing workflow | Completed approved month revision; Kafka cannot write this product |
| G04amazon_review_daily | ADF-owned Amazon review product Job | Review Silver ready; bind approved metadata version for enrichment/coverage. Neither individual supplier workflow writes G04 directly |
| G05campbell_station_daily | Campbell processing workflow | Eligible station-day revision, approved field dictionary; other stations do not block |
| G06openprices_daily | Distributed api_business Job, dispatched by ADF receipt/business admission | Verified accepted observations and latest-observed selection; acquisition Job never writes Gold |
| G08rees46_kafka_activity_daily | Kafka business task, native bounded incremental stream | Kafka-source Silver only, exact logical/physical duplicates, event-time late correction and independent publication |
| G07catalog_quality_daily | ADF-owned catalog quality product Job | OFF/Amazon domain scopes independent in one table; freeze actual source catalog epoch; source supplier workflows do not concurrently update G07 |

The active 1-minute ADF product debt check is a **wake-up**, not a new business watermark. It admits bounded ready work and returns no-op when current. Immediate dependency dispatch and missed-work sweeps use the same product owner/intent rules. No native cron is enabled for these ADF-owned Jobs.

Cross-source cases:
- One source completes while another is delayed: publish only products whose declared inputs are ready. H&M/Campbell/API do not wait for Amazon/OFF.
- Fresh review occurrences with stale/missing metadata: review activity can remain visible with METADATA_PENDING; enriched product/coverage views explicitly wait for a bound eligible metadata version. Never inner-join away original reviews.
- Multi-input Silver/Gold: freeze exact required commit/transaction/revision versions and approved coverage; no arbitrary latest-table join.
- A correction after Gold: the owning product Job recomputes old and new affected groups and reconciles before publishing; no additive retry.
- Upstream failure when the next tick starts: attach/defer the same owner, inspect actual partial stage commits, and retain publication debt.
- Manual backfill overlaps normal work: same configured owner or exclusive paused recovery scope; no second target writer.
- Continuous stream with periodic products: native stream capture continues; a bounded product owner uses committed snapshots/CDF without controlling source micro-batches. The H&M low-latency native conformance exception remains explicit.
- Bronze fresh but Gold stale: alert by product publication/input scope; do not mark enterprise lifecycle successful from ingestion alone.

### 9.8 ADF and API scheduler migration

OA-API-02 changes the **schedule binding**, not the REST/ledger algorithms: pause the old native acquisition and receipt cron triggers, verify no queued/active conflicting runs, deploy the two ADF schedule bindings stopped, validate actual Job identities/permissions, then enable ADF only after approval and runtime gates. Never leave both schedules active.

Pass source/dataset, approved release, run mode, ADF correlation and nominal due slot; normal request bounds remain planned from the acquisition ledger. Bounded recovery/backfill parameters are separately allowlisted. The actual Databricks job/run identity still controls ledger ownership and shutdown proof. Receipt processing remains independent of acquisition success.

## 10. Kafka service and approved REES46 semantics

### 10.1 Temporary real Kafka hosting and DEV-KAF-01

| Option | Capability/cost finding | Selected outcome |
|---|---|---|
| Apache Kafka KRaft + compatible Schema Registry on shared Azure source VM | Actual transactions, idempotent producer/fencing, read_committed, six partitions, compacted progress and Registry; explicit single-copy durability/availability limits | **Select for temporary DEV**, reuse the planned4 vCPU/16 GiB host/durable disk; no default extra VM. |
| Confluent Cloud | Genuine managed Kafka/Registry, enterprise RF/availability option; capacity/traffic/storage/retained-state charges can continue after demos | Not default; use only if a separately approved quote/capability need justifies it. No managed eCKU or external credit assumed. |
| Event Hubs Kafka endpoint | Tier/transaction/compaction/Registry semantics must actually match D11; Kafka compatibility alone is insufficient | Not selected; no unverified substitution. |
| HDInsight/other managed cluster | Extra persistent cluster capacity/operations without a temporary-demo requirement | Not selected. |

**DEV-KAF-01 material infrastructure/availability overlay** affects D08/D11 hosting/RF/ISR/retention-closeout expectations and D01's shared Kafka CDC transport availability. Full enterprise target remains RF3/minISR2/multi-broker appropriate failure-domain design. DEV selects RF1/minISR1 and one combined KRaft broker/controller. Transactions/fencing/read_committed and exact source identities are preserved, but acks=all means the one broker, **not** redundant replicas or guaranteed host/disk-loss zero RPO. Replica failover/HA tests are NOT_APPLICABLE_DEV and remain NOT EXECUTED for enterprise qualification; no equal-reliability claim.

Selected compatibility baseline: Apache Kafka3.9.2 KRaft and Schema Registry7.9.9 community component (not licensed Confluent Server), supported JDK17 runtime; pin verified release/container digests and current security/license compatibility before implementation deployment. This is a source-stack selection, not installed-resource evidence. Retained D11 producer/client and Databricks Kafka consumer versions must pass actual wire/transaction/Avro compatibility, not be silently replaced. [Apache release](https://kafka.apache.org/blog/2026/02/21/apache-kafka-3.9.2-release-announcement/) [Registry/platform compatibility](https://docs.confluent.io/platform/7.9/installation/versions-interoperability.html)

| DEV broker/Registry property | Exact selected intent |
|---|---|
| process.roles / node.id / cluster ID | broker,controller; one fixed node ID; generated/preserved actual cluster UUID, never format persistent state on restart |
| Controller/listeners | Selected node.id=1; static controller.quorum.voters=1@127.0.0.1:9094; protected loopback CONTROLLER listener; private SASL_SSL client9093 and matching advertised address; actual TLS trust/credentials bound before use |
| Durable state | Dedicated data-disk directories for Kafka logs/KRaft metadata; retain volume on process/VM restart; no ephemeral container writable layer for broker state |
| behavior / compacted progress topics | D11 exactly two source topics;6behavior partitions,1progress partition;RF1/min.insync.replicas1; behavior14day/delete/LogAppendTime; progresscompact/no finite deleteTTL while active |
| transaction internal state | transaction.state.log.replication.factor=1; transaction.state.log.min.isr=1; transaction.state.log.num.partitions=1 for this bounded DEV instance |
| Other internal replication | offsets.topic.replication.factor=1; default.replication.factor=1; explicit RF1 for Connect internal/CDC/transaction topics and Registry _schemas; no broker-default RF3 creation failure |
| Integrity controls | auto.create.topics.enable=false; unclean.leader.election.enable=false; producer acks=all/enable.idempotence and D11 transaction settings unchanged |
| Schema Registry | Same broker, compacted _schemas,1partition/RF1, stable Registry-generation/actual ID→writer bytes bindings, FULL_TRANSITIVE on D11 subject; no per-record lookup |
| Networking/security | Private VM address advertised consistently; SASL_SSL client listener, verified certificate/private address SAN or private DNS; private HTTPS Registry with native client-certificate authentication; protected loopback/controller and Connect REST, no public broker/Registry/8083 port |
| Permission boundaries | Scoped producer transaction/topic ACLs, separate native reader/Connect/Registry credentials in Key Vault/protected service runtime; reuse existing identities, no custom authorization service |
| Initial process budgets | Broker heap2 GiB, Registry512 MiB, Connect1 GiB; bounded producer buffers. Measure total RSS/native/page-cache/disk, not just JVM Xmx; keep OS/cache headroom |
| Admission / disk | Initial Kafka log allowance≤32 GiB inside existing128 GiB working-set plan; no retention.bytes truncation pretending to keep14 days. At70%capacity warn,85%stop new source admission,90%controlled drain/hold. Retain all unacknowledged/required state. |

Same host also runs source publisher/producer/Connect; **do not** run heavy OFF/Amazon import/export/compaction concurrently with Kafka qualification. Pause source mutation/native capture gracefully where their own windows are inactive; preserve WAL/Connect offsets. During Kafka window, broker/Registry/independent producer/native Databricks clients are genuinely active. Qualify CPU/RSS/I/O/lag at the bounded chosen rate first. If contention requires a larger host or separate broker, demonstrate it and price/approve it before changing the selected default; never silently overprovision or fake60× delivery performance.

This shared Kafka service also transports genuine H&M CDC/Connect state; those are platform/source topics distinct from D11's two REES46 topics. Their native snapshot/offset semantics do not become a generic source watermark. Private VNet paths already selected for the source host/Databricks cover the broker; no extra public IP or managed Kafka networking service is introduced.

### 10.2 Preserve the two-topic REES46 source

Behavior:6 partitions, delete policy, retention.ms=1209600000, retention.bytes=-1 subject to paid capacity, LogAppendTime. Control:1 partition, compact policy, unique batch/<sequence> summaries plus progress/lane000; no finite delete TTL. Actual cluster/topic IDs are pinned for a generation; stable transactional.id fences its lane.

One Kafka transaction commits data, the bounded summary and next progress. A normal batch keeps month/file contiguous ordinal range, original/physical counts, actual partition counts/min/max callback offsets and one length-framed source-content SHA across ordinal+nine original strings. A null and an empty string are distinct. No per-offset digest database or third audit topic.

Required client settings remain acks=all, enable.idempotence=true, max.in.flight5, murmur2-compatible user key, zstd, bounded linger/batches/buffer, transaction timeout60 s, message45 s/socket15 s and supervised fatal/abort/uncertain outcomes from D11. Retriable commit retries the same transaction; fatal fencing stops old owner. Never independently advance source position after an ambiguous commit.

Real Schema Registry IDs are deployment outputs verified against pinned writer bytes and subject compatibility. R2 requires month/scenario and essential source/run provenance. Ordinary unknown-ID/Avro/semantic errors produce classified inspection status without filtering raw key/value/headers. Native process failure retries the query rather than falsely accepting a record.

Native Databricks consumer keeps read_committed, includeHeaders, failOnDataLoss=true, startingOffsets=earliest only for a fresh checkpoint, nonshared groupIdPrefix,10sec trigger and150000maxOffsetsPerTrigger. Normal sink is native Delta append, no foreachBatch/MERGE or intermediate raw event landing layer.

### 10.3 Fixture and exact full-source reconciliation retained

Verified October ordinals14 then1 share expected partition0; ordinals3830/3831 are distinct original occurrences with identical source values and expected partition3. Relative releases are0.25 s,30 s,145.666666 s,145.666666 s then one duplicate3830at146.666666 s. A separate600 s delayed1case retains R1's ten-minute scenario. Expected5 physical/4 logical; actual callback offsets must satisfy O14<O1 and O3830first<O3831<O3830duplicate. Numeric offsets are never predicted.

After SOURCE_DRAINED, freeze control sequence and Bronze version. Prove all monthly original integer ordinals1…N exactly, correct original IDs, source-batch content hashes, actual physical partition accounting, event-type/day anomaly totals, no unaccounted attempts and declared duplicate multiplicity. N distinct in-bounds integers proves full coverage; simple min/max/count does not. Aborted/control-marker offset gaps are not automatically missing committed business records.

D11's existing55local tests and approximately10.7k/s short CPU benchmark are prior local evidence only. No tests were run in this architecture phase. They do not qualify managed Kafka, Linux hosting, Spark Python-worker overhead,85.2 h completeness, SLA or cloud cost.

### 10.4 Independent Kafka Silver: silver.rees46_kafka_events

KSG-01 is a project downstream extension, **not a change to D11 producer-to-Bronze**. Keep original key/value/ordered headers, raw inspection/errors, Kafka transaction isolation, physical offsets and native Bronze Delta append/checkpoint. Kafka never passes through SFTP/Copy/ADLS event staging/Auto Loader.

Managed Silver grain: one conformant original source event per **(source_instance,logical_source_generation,source_event_id)**. source_event_id retains D11's original seed/month/hash/ordinal identity; valid identical original payloads at 3830/3831 remain two rows. Same event republished at new physical Kafka offsets/application_attempt is a physical publication duplicate, not a new logical event. Consumer/recovery/run/topic generation is lineage, not permission to invent another business occurrence. A deliberate new simulated source generation is explicit.

Columns: nine exact original fields plus trusted typed projections (UTC event time, exact identifier strings/qualified price), source_file_month/scenario_id/schema ordinal/actual Registry ID+generation, original ordinal/seed reference, first/last physical publication references, physical_publication_count, DQ/disposition, raw Bronze lineage/input UUID/version, decoder/release and recovery lineage. Keep source lexical price; no currency/revenue/order inference. Decoding uses pinned actual writer schemas/cached inspector and complete consumption; unknown IDs/malformed/semantic failures remain in Bronze and existing DQ results. Later approved decoder repair reprocesses retained Bronze, never producer refetch.

**Bounded incremental conformance:** a separate native business query reads the append-only canonical Kafka Bronze Delta stream, trigger30 seconds, native checkpoint separate from capture. Each foreachBatch freezes actual Bronze UUID/version and its affected-ID scope (not a claim that all rows at that version have been processed), extracts affected source IDs, and recomputes their complete physical-publication set/content from Bronze. Identical same-ID payloads yield one Silver row with exact recomputed multiplicity. Conflicting original content is a critical business callback hold: raw capture continues, the prior product stays visible only with HELD/STALE trust/freshness status, and no arbitrary winner is published as newly validated state. No blind count increment, payload-hash dedup or arbitrary latest-wins. Deterministic unique per-event actions MERGE into Silver, then reconcile valid physical=accounted Silver memberships plus classified invalid/conflicting populations. Source-level full/month coverage still uses D11 committed summaries after SOURCE_DRAINED; a live partial window is not a complete source month.

Same owning business task then computes G08 affected old/new groups (§10.5), with an idempotent publication marker. Silver and Gold are separate transactions; business checkpoint advances only after its required callback work succeeds. If Gold fails after Silver, retry returns matching Silver effects and recomputes Gold from its bound input. Actual Delta commit/control evidence repairs uncertain outcomes after quiescence. No new control/offset table.

Event-time policy: authoritative Silver retains every valid original event irrespective of age/arrival order. **No watermark-based dropping** in this occurrence-preserving conformance. Stateful processing is demonstrated in an isolated5 minute event-time window/10 minute watermark experiment with declared on-time/late/beyond-watermark oracles; it is not the authoritative late-correction population. Normal conformance is bounded incremental Delta processing, which the user permits instead of an unbounded state store.

### 10.5 Independent Kafka Gold: gold.rees46_kafka_activity_daily (G08)

| Business contract | Selected definition |
|---|---|
| Product / consumer | REES46 Kafka streaming analytics; event-stream operations/product-activity analyst. Separate from G03 SFTP batch/incremental analytics. |
| Grain / key | Logical Kafka source generation + UTC source event date + exact product_id/category_id/category_code + event_type + original/fixture cohort. Generation is the explicitly chosen source population, not a consumer retry. |
| Measures | logical_event_count; exact distinct nonblank user_id and user_session counts per grain; physical_publication_count and duplicate_publication_count separately labelled; first/last original event time; invalid/missing identity/session coverage via quality views |
| Semantics | Valid repeated original records count independently; physical republish of the same original ID increases physical/duplicate metrics only. Sessions/users are source identifiers, not verified people/orders. Price is not revenue; no currency/paid order/checkout inference. |
| Update | Recompute entire affected old/new date/product/type/category/cohort groups from a frozen conformant Silver version; exact count-distinct recomputation, no summing per-batch distinct counts. MERGE replacement/removal plus publication marker, not additive retries. |
| Late/out-of-order | Any valid late logical event updates its actual historical event-date group, even beyond the isolated watermark experiment. No fabricated new date or discard due to wall-clock age. |
| Readiness | Only eligible Silver/DQ/reconciliation scope; live output visibly PROVISIONAL/PARTIAL with coverage. Full month/source completion only after producer/Bronze/Silver/Gold exact certification. |
| Latency targets | D11 broker→Bronze p95≤30s/p99≤120s retained. Selected broker→Silver p95≤60s/p99≤180s; broker→Gold p95≤90s/p99≤300s; consumer target≤360s with actual client. All are NOT QUALIFIED. |
| Event-time versus processing-time | Historical2019/2020 event time defines business date; actual broker LogAppendTime/native Delta commit/publication evidence defines ingestion latency. Ancient event age is not a streaming SLA miss. Throughput/lag are operational views over physical progress, not invented business revenue. |
| Reconciliation / recovery | Exact Silver logical ID/multiplicity/content and group counts/distinct sets; actual publication UUID/version. New consumer replay produces identical logical business results; physical lineage remains separately observable. |
| Independence | Never default join/UNION with G03 or merge them as distinct total users/events. Comparison view may align exact original scopes, explicitly labelled validation, no third physical combined product. |

**Selected compute/lifecycle:** one approved two-worker Kafka qualification Job cluster, with parallel tasks for the unchanged native Bronze capture and the new Silver/Gold business query where runtime supports safe shared Job compute. Each query keeps its own native checkpoint and owner; D11 capture sink/options are unchanged. No separate always-on cluster per layer. If shared tasks cause contention, serialize bounded downstream drain after source capture or qualify/price a revised window; no unapproved capacity increase.

ADF coordinates Job readiness/start/monitor/finite replay, not30 second business callbacks or10 second capture micro-batches. Finish source publication, reconcile committed summaries/native Bronze, drain business query, certify Silver/Gold, then stop task/query services. Business failure need not refetch/republish source data; Raw Kafka Bronze remains its replay authority.

### 10.6 Temporary broker lifecycle, retention and recovery

1. After separate funding/coding/resource approval, qualify the shared host/disk/private TLS/SASL and install pinned broker/Registry/native topics/ACLs.
2. Verify actual cluster/topic/Registry identities/configuration, transaction abort/commit/fencing/read_committed and D11 writer/reader compatibility. Archive real schema bytes/IDs/generation in the immutable release/evidence.
3. Run the independent60×producer on declared source windows; native Bronze plus independent Kafka Silver/Gold process/reconcile them.
4. Execute authorized bounded late/duplicate/schema/commit/checkpoint/process-restart tests. A single broker can demonstrate process outage/restart; it cannot demonstrate replica failover/HA.
5. Before shutdown, stop new publication, resolve transactions, retain producer progress/native query state, drain/reconcile B/S/G and archive source counts/hashes/real offsets/target versions/control certificates.
6. Deallocate VM outside windows; disks/logs/Registry _schemas survive and continue billing. Restart mounts the same data/cluster state, never reformats the broker.
7. Only after explicit closeout and completed required replay/recovery demonstrations, delete broker services/data/resource scope that is no longer shared/needed. Never delete the shared host/disk while H&M/other source journals/Connect still need it.

D11 behavior14day retention and compacted progress/native offsets stay intact while the DEV source is active. Early project closeout **ends the remaining broker replay obligation under DEV-KAF-01**; it does not pretend14day/400day enterprise availability was delivered. Keep minimal audit/schema/summary evidence; full logs/data may be removed only after authorization. A later restart after deleted broker state is a declared new broker/topic/Registry generation and recovery/republication scope, never a guessed continuation of lost progress.

Checkpoint loss before closeout: preserve damaged state; native D11 Bronze isolated replay/reconciliation/canonical cutover. Business-checkpoint loss: restore coherent compatible state or rebuild isolated Silver/G08 from retained canonical Bronze, exact logical/group reconciliation and sole-reader cutover. No overlapping business UNION or blind new-checkpoint append into current targets. Expired Kafka offsets require D11 seed/recovery generation procedure; post-closeout missing source logs cannot support an unqualified zero-loss/RTO claim.

## 11. Distributed Spark profiles and execution design

### 11.1 Minimal starting compute and evidence-driven scale

The user has accepted **Azure Databricks Premium PAYG as a billing direction**. This is not an account/credit/quota verification or authorization to provision. Use Jobs compute/short active windows, pinned supported DBR16.4LTS patch; no initial4/8 worker historical allocation, idle interactive cluster or always-on SQL warehouse.

| Workload profile | Selected initial configuration / purpose | Scale/lifecycle decision |
|---|---|---|
| Small control / API acquisition and receipts | D4ds_v5 single node for D10 serialized HTTP/ledger and bounded receipt/Bronze; avoid launching separate compute merely for a tiny metadata read | Preserve D10 exact ownership/termination; terminate finite Jobs. Large API Silver/G06 uses distributed profile. |
| Tiny Spark functional smoke | D4ds_v5 driver +1D4ds_v5 worker, optional≤15 minute attended smoke | Does not satisfy distributed qualification. Reuse compatible active Job task compute where supported; no permanent per-source cluster. |
| Distributed ingestion / PostgreSQL-Silver-SCD2-Gold (INCREMENTAL and initial HISTORICAL aliases) | D4ds_v5 driver +**2workers**, fixed initial | Mandatory actual multi-worker demonstration; native file/row-group parallelism, joins/windows/MERGE/AQE. One heavy workflow at a time. |
| Kafka qualification / independent B-S-G | D4ds_v5 driver +**2workers**, fixed | Native Bronze and new business tasks may share one compatible Job cluster with independent query checkpoints; measure contention/lag. No per-layer always-on cluster. |
| Full history / larger optimization | Start same2 worker profile with admitted bounded input | Increase file/task parallelism first; resize memory or add workers only when actual utilization/spill/skew/lag proves the need. A4.1 planned4 worker sessions are revoked as default. |
| Optional later4/8 worker experiment | Not provisioned/scheduled initially | Named performance question, measured bottleneck, approved quote/quota/runtime and short limit required first. More workers do not split one gzip. |
| Recovery/query validation | Reuse appropriate finite1/2 worker profile, isolated checkpoint/target scope | No new permanent recovery cluster or warehouse; scale after actual scope measurement. |

Nominal initial distributed workers provide8 vCPU/32 GiB combined worker RAM, not guaranteed usable heap/throughput. Single simultaneous3 node D4 profile is12Databricks vCPU; two finite API single nodes can add8. Serialize heavy H&M/Kafka/file sessions; do not allocate multiple historical clusters during setup. Region/SKU/quota includes source VM/database separately.

Native Auto Loader performs CSV/JSON/Parquet/text file parsing. No SFTP executor SDK credential propagation, RDD downloader or custom ordinal-reader compute profile remains. Publisher chunks native CSV/JSONL files before SFTP; one compressed part remains sequential, independent native parts run concurrently. Bounded native validation hashing and TOA5 wholeText transformations have explicit memory limits.

Keep measured executor/task placement across at least2workers; repartition/shuffle/AQE/skew, safe broadcast versus sort-merge, exact MERGE/SCD2, aggregates, executor failure/recovery, spill/memory pressure, schema/restart and Kafka multi-partition backpressure/lag evidence. No achieved speedup/latency/cost claim. Photon and autoscale changes require supported-plan/cost evidence; classic continuous stream sizing changes use a controlled compatible restart.

### 11.2 Native source parallelism and stable source identity

| Source | Useful native Spark work / identity |
|---|---|
| REES46 CSV/gzip | Native CSV cloudFiles reads multiple source-published bounded parts; all original fields lexical; delivered ordinal + original seed SHA is stable identity. |
| OFF JSONL/gzip | Native JSON inference/hints parse registered envelopes; VARIANT holds open payload; source original image/ordinal and journal IDs preserved. Multiple source parts parallelize. |
| Reviews JSONL | Native JSON explicit schema/rescue or isolated inference/evolution; publisher original ordinal assigned before filtering; source image/repetition preserved. |
| Amazon Parquet | Original native shards and row groups, actual typed/nested schemas; no conversion. |
| Campbell DAT | Native text/wholeText of≤1 MiB/2000observation parts, stateless ordered-content transformations/membership join; station tasks parallelize. |
| PostgreSQL | Sealed JDBC partitions only where source membership/snapshot is fixed; real native CDC capture/transaction conformance, not CSV bypass. |
| REST | HTTP serialized by D10; downstream accumulated conformance/aggregates distribute independently. |
| Kafka | Six source partitions, native read_committed/capture; independent bounded incremental Silver/Gold; extra repartition is shuffle, not additional broker partitions. |

Original ordinal/source IDs are supplied by the simulated source publication contract, not inferred from unordered Spark output. Never monotonically_increasing_id, partition index, timestamp sorting or content hash as an original ordinal. No receiver intermediate file layer is selected. An optional future structured staging table requires a targeted justified inventory amendment; none is needed here.

### 11.3 Spark execution, joins and maintenance

- Explicit schemas/projections; preserve raw/nested content separately. Avoid Python row UDFs for ordinary typing/joins/DQ; the required Avro inspector is measured separately.
- Initial batch file scan target128 MiB where splittable. Historical shuffle256 partitions initially, incremental64, small control8–16. Batch AQE coalesces/splits skew toward64–128 MiB post-shuffle; revise from actual shuffle sizes/task duration.
- Enable AQE/coalescing/skew joins for supported batch operations. Do not assume AQE changes ordinary streaming state plans; checkpoint-sensitive shuffle/state configurations remain fixed.
- Broadcast only a measured narrow dimension safely under the selected threshold/memory envelope. Do not broadcast all metadata/review text. Large joins default to measured sort-merge; precheck unique dimension keys and expected join cardinality.
- Record input/output partition counts and rows, task p50/p95/max, CPU/memory, spill, shuffle read/write/fetch failures, skew, Python serialization, file pruning and actual physical plans. Compare4/8historical workers only when input parallelism can benefit.
- Resolve demonstrated hot analytical keys with correct preaggregation or scoped salting, followed by unsalted exact aggregation. Never salt native source identities or required Kafka user routing.
- Use deterministic complete MERGE source units with unique target identities; do not flatten all source versions into an arbitrary latest row before SCD2 history apply. Restrict target scans by source/month/affected keys; materialize expensive reused bounded inputs when measurement warrants.
- Keep small tables unpartitioned. Large tables use limited date/month partitioning or supported liquid-clustering keys selected from real access patterns, not user/product cardinality partitions. Start with source event date/month pruning; avoid per-delivery directory explosion.
- Prefer managed-table optimized writes/file tuning; observe128–512 MiB output sizes, not a universal forced value. Compact measured small-file debt outside capture critical paths, not each10 s batch.
- Default no watermark-based dropping in occurrence-preserving Bronze or historical conformance. A future stateful analytical watermark needs a business lateness bound and explicit reconciliation of later corrections.
- Spill is not itself failure. Diagnose oversized partitions, skew, joins and broadcast before increasing memory. Keep source evidence/checkpoints off ephemeral shuffle disks.

No throughput, completion time, cluster adequacy or cost efficiency has been proved by these starting values.

## 12. ADLS storage, immutability and retention

### 12.1 Physical separation and paths

**Supplier SFTP account:**
- `supplier-deliveries/dev/<source>/generation=<g>/delivery=<id>/revision=<r>/staging/attempt=<a>/...`
- Final parts/manifest/READY in the same logical revision's finalized path; no rename across containers.
- `supplier-acks/dev/<source>/generation=<g>/delivery=<id>/revision=<r>/<manifest_sha>.json`.

**Lake account, SFTP disabled:**
- `seeds/dev/<source>/release=<seed_sha>/<original_name>` — restricted source initialization copies.
- `raw/dev/<source>/generation=<g>/delivery=<id>/revision=<r>/attempt=<a>/...` — exact acquired SFTP parts/control objects.
- `evidence/dev/<source>/...` — ordinary SFTP/source validation/reconciliation audit; no SFTP WORM.
- `api-evidence/dev/openprices/work=<w>/traversal=<t>/page=<p>/attempt=<a>/body.bin`, response/validation/page manifests.
- `api-evidence/dev/openprices/windows/<manifest_sha>.json`, `roots/<manifest_sha>.json`.
- `api-evidence/dev/openprices/ready/v1/<acceptance_id>.json` — only accepted leaf receipts; separate prefix/volume from body storage.
- `staging/dev/sftp/<source>/source_generation=<g>/unwatched/<publication_attempt>/...` — byte-identical native files, never watched by Auto Loader.
- `staging/dev/sftp/<source>/source_generation=<g>/accepted/mode=<mode>/schema_family=<registered>/delivery=<id>/revision=<r>/...` — verified original native source files only; Amazon FULL/DELTA have compatible separate scopes.
- `state/dev/checkpoints/sftp/<source>/<mode>/<consumer_generation>/` — seven native query scopes across five SFTP integrations (OFF FULL/DELTA and Amazon FULL/DELTA separated).
- `state/dev/checkpoints/rees46-kafka/capture/generation=<g>/` and `state/dev/checkpoints/rees46-kafka/business/generation=<g>/` — independent native D11 capture and KSG-01 business state.
- `state/dev/checkpoints/<source>/<query>/generation=<g>/` — other native source-specific checkpoints.
- `state/dev/schema-state/sftp/<source>/<mode>/<schema_epoch>/` — the persistent SFTP cloudFiles.schemaLocation; text remains fixed-schema.
- `state/dev/schema-state/<source>/<query>/generation=<g>/` — other native schema state.
- `state/dev/work/<run_id>/` — rebuildable bounded derived artifacts.
- `state/dev/releases/<release_sha>/...` — immutable released config; never a credentials directory.
- `managed/` only if required for the discovered catalog's managed root; never overlap external volumes.
- `tfstate/dev/` mutable locked infrastructure backend, only after later infrastructure approval.

API-only api-evidence and ordinary evidence are separate containers in the same lake account; no SFTP payload lives under the API WORM container. This is a selected future deployment binding, not authorization to move existing accepted evidence or rewrite historical URIs.

Use UC external locations/volumes for needed Raw/evidence/staging/state roots; native accepted-file publication and its reader use the same actual lake account, with non-overlapping locations. No DBFS mounts, fabricated managed-table paths, overlap with managed Delta directories or public seed access.

### 12.2 Two-account storage policy and DEV-RET-01

**Selected by user:** two storage accounts under the same Azure subscription/project/DEV. Account1 is the actual SFTP supplier (deliveries/manifests/READY/ACK); Account2 is the HNS lake (native Raw/accepted, API evidence, checkpoints, configuration releases and managed Delta/UC storage). No one-account re-evaluation, per-source accounts or account-wide WORM.

**SFTP paths: no WORM/365day lock.** Raw and accepted files use unused attempt/final names, guarded logical revisions, least-privilege access and exact source/landed/target SHA/membership validation. Matching existing bytes are idempotent; conflicting overwrite is prohibited. These are logical write-once controls with integrity evidence, not a claim of Azure-enforced immutable SFTP retention.

**DEV-RET-01 proposed material retention overlay for D02–D06R09:** retain SFTP supplier exports, cloud seed copies, Raw, accepted files, SFTP payload-bearing Bronze/Silver/Gold generation data and historical decoders while ingestion/DQ/replay/recovery/demonstrations are active. Default working allowance is active project +7day recovery buffer after last authorized run, subject to explicit closeout. This is not a mandatory year after the project. User-authorized earlier closeout is permissible only after the dependency/test/reconciliation checklist below; unresolved obligations override a timer. Original downloaded user files are never deleted/overwritten by this policy.

The full enterprise contracts still specify their original365day payload/source horizon,≥90day acknowledged supplier exports and400day minimal audit. The temporary DEV execution **does not claim those full horizons are satisfied**. Retain these enterprise designs in source traceability; the overlay is the declared deviation. Lightweight hashes/counts/schema/commit/test certificates may be retained independently; a certificate after payload cleanup is past-execution evidence, not proof that replay remains possible.

**API D10 is separate:** exact HTTP bodies/manifests/accepted receipts and ledger recovery retain D10's365/400day requirements and selected container-level evidence WORM. Only the dedicated API evidence container is considered for physical immutability, never Raw/SFTP/Delta/checkpoints/the whole account. Actual retention lock requires separate explicit authorization and cost/rights verification. Already locked WORM cannot be shortened by calling project closeout.

If the desired final cleanup also includes D10-protected API evidence, it conflicts with D10. The precise **D10-DEV-RET-02 candidate amendment, not selected/activated here**, would replace D10 body/receipt≥365day and evidence≥400day horizons with a declared DEV active+buffer/closeout horizon, archive a final ledger/acceptance/receipt/Bronze certificate and explicitly end offline evidence replay/idempotent recovery after deletion. It requires separate explicit contractual approval before any incompatible WORM lock/deletion; no silent waiver. Current architecture preserves D10, so its residual storage cost remains a funding gate.

### 12.3 Retention and authorized closeout

| Scope | Selected DEV / retained enterprise boundary | Expiry safeguard |
|---|---|---|
| Original downloaded seeds | Existing user originals unchanged; cloud copies follow approved source/DEV scope | No user-original deletion/migration authorized |
| SFTP D02–D06 source/cloud payload generations | DEV-RET-01 active+7day planning buffer or explicitly approved closeout; enterprise365/90day counterpart retained as separate design | No purge with incomplete/held/unACKed/replay-dependent scope; preserve source manifest/schema/identity until closeout |
| SFTP accepted files/native checkpoints/schema locations | Persist across ordinary runs; protect throughout active recovery window | No cleanSource/normal checkpoint reset; delete only after source/query generations formally retired |
| SFTP B/S/G managed Delta | Same approved DEV generation lifecycle; no mandatory year of payload retention in this temporary project | Target identity/version/consumer/replay dependencies certified before data retirement; no deletion of other source scopes in shared tables |
| Minimal shared audit/schema/test evidence | Keep minimal source IDs/hashes/counts/actual resource/schema/target versions and executed/not-executed results, nominal400day audit where rights/funding permit | No large duplicated source payloads; no false claim that deleted source/Delta versions are still replayable |
| PostgreSQL source/WAL/CDC | Original D01 retention/slot/horizon while active; later source closeout is explicit, not inferred from SFTP cleanup | Pause mutation/Connect, certify source state and required recovery first |
| Kafka behavior / progress / Registry / Connect state | D11 14day behavior policy and compacted native progress while active; DEV-KAF-01 ends remaining replay availability only at explicit broker closeout | Do not delete shared broker/disk while H&M or pending stream/recovery still depends on it; archive actual schema/topic/generation evidence |
| API evidence/bodies/receipts/ledger | D10≥365day data/receipts and≥400day evidence; protected container only; candidate amendment separate | Funding/lock/rights gate; no project-closeout override or blanket lake deletion |
| Active Delta logs/data/native checkpoints | Native mutable state, no WORM; actual dependent history retained | No short VACUUM/lifecycle rule undermining active rollback/CDF/replay; post-closeout guarantees explicitly end |

**Closeout algorithm (future only):** stop new source work/triggers; resolve/fence transactions and uncertain writes; drain every required native query/business stage; freeze final source/target/control versions; certify no held/incomplete/unACKed/repair/publication debt or explicitly record accepted unresolved debt; finish required recovery/schema tests or list them NOT_EXECUTED; archive minimal source/schema/actual test/retention-deviation evidence; verify no other source/consumer/control/locked API dependency uses the selected paths/resources; obtain explicit user closeout/deletion approval; then delete only approved DEV scope and disable feature/compute charges. Do not automatically delete at 7 days, project-day4, TTL or first ACK.

Raw/native checkpoints removed at closeout no longer support replay. Reopening the project requires declared source/consumer recovery generations and source availability, not pretending the old checkpoints remain valid. Stopping/deallocating VM/SFTP removes some compute/feature charges; disks/Blob/managed Delta/API WORM can keep billing until their own approved/allowed expiry.

## 13. Unity Catalog, controls and physical table audit

Bind the actual available workspace UC catalog after deployment; reject hive_metastore/system and do not invent “main.” Use schemas bronze, silver, gold, control and files. **Every physical table below is a managed Delta table.** Raw files and volumes are not managed Delta tables. Logical keys are enforced by guarded single writers/validated source uniqueness, not by informational PK declarations.

Preserve the A4.1 baseline 31tables and add exactly the two Kafka-owned downstream targets: **10 Bronze +10 Silver +8 Gold +5 control =33 normal managed Delta tables**. Auto Loader state and accepted original native files are transport/state artifacts, not additional managed Delta tables. Retain A3's off_identity_exceptions view over Bronze rather than a duplicate physical copy, and retain its H&M business fact/catalog-quality products and defined daily aggregates. Isolated recovery tables are temporary declared recovery targets in DEV, not a second environment or a permanent parallel data platform.

Common Bronze columns: source/generation/release/schema, logical record or operation identity, record_kind, original evidence reference/hash/ordinal where applicable, operational arrival/commit clocks, DQ disposition and replay lineage. API/source-specific/Gold publication markers, where applicable, are excluded by business views; SFTP uses source_deliveries, not generic Bronze commit markers. All captured rows—including classified row errors—are accounted for. Full physical corruption can block parsing while its exact Raw attempt remains retained.

### 13.1 Ten Bronze tables

| Table / why | Grain and identity | Capture semantics / progress authority | Contract / retention / dependencies / recovery |
|---|---|---|---|
| bronze.hm_articles | Baseline article or journal member; source generation+article/version/operation; sealed completion marker | Fixed seal capture; one target transaction per bounded complete seal; source journal ACK only afterward | D01;365 d; PostgreSQL journal→capture; exact seal/raw image replay |
| bronze.hm_customers_cdc | Physical Kafka record; cluster/topic UUID+partition+offset | Native append, own checkpoint; retain full source envelope | D01;365 d; Connect→Kafka; native restart/isolated recovery |
| bronze.hm_purchases_cdc | Physical purchase or native transaction metadata/tombstone record, record_kind explicit | Native append, own checkpoint; no false cross-table atomicity | D01;365 d; purchase/transaction topics; membership reconciliation |
| bronze.rees46_sftp | Original native CSV occurrence per delivery revision, SP-01 source ordinal | Native CSV Auto Loader → lexical/provenance projection → native checkpointed Delta append | D02+SP-01; DEV-RET-01; original payload/hash/ordinal; source_deliveries whole-delivery certificate/ACK |
| bronze.off_products | Original FULL occurrence or committed journal operation | Native JSON Auto Loader with inference/hints and native payload/after VARIANT; native append | D03+SP-01; DEV-RET-01; open nested payload/raw image, source ordinal/change IDs and canonical/exception accounting |
| bronze.amazon_metadata | Original native snapshot product or committed source operation | Native Parquet Auto Loader/append; separate compatible FULL/DELTA checkpoints, one source owner | D04; DEV-RET-01; native16fields/nesting/case preserved; no conversion; isolated checkpoint-loss recovery |
| bronze.amazon_reviews | Original review occurrence or declared isolated fixture op | Native JSON Auto Loader/explicit schema/rescue → append; publisher original global ordinal/image retained | D05+SP-01; DEV-RET-01; repeated payloads distinct; exact FULL/day/fixture coverage |
| bronze.campbell | Original native observation/revision with exact header/source lineage | Native text wholeText Auto Loader → bounded stateless TOA5 column transforms/mapping join → native append | D06+SP-01; DEV-RET-01; exact four headers/lexemes/station/logger/epoch/ordinal; expected empty source in controls |
| bronze.openprices_capture | ITEM, WINDOW_COMMIT and WRITER_HEAD | One guarded same-table window MERGE; observation_id from D10; Auto Loader checkpoint afterward | D07+D10;365ddata/400dminimal audit; accepted ledger/receipts→decode; idempotent replay |
| bronze.rees46_kafka | Every committed physical Kafka record, including malformed/unknown-ID rows | Direct native append/checkpoint; original key/value/timestamp/type/ordered headers unchanged | D08+D11;365 d; Kafka only; certified isolated cutover |

All five SFTP targets use native append/checkpoint plus source publication identity and controlled accepted paths. Logical whole-delivery completeness/ACK remains source_deliveries, bound to exact target/native/reconciliation evidence. No SFTP custom commit rows or source-key MERGE callback; partial captured scope is held from Silver.

API ITEM/WINDOW_COMMIT/WRITER_HEAD remains D10-specific. Kafka Bronze remains D11 native physical append; new Kafka logical deduplication/conformance occurs only downstream. Prior operational attempts/clocks do not change source occurrence identity.

### 13.2 Ten Silver tables

| Table / why | Grain / key | Update / readiness | Authority, retention, dependencies and recovery |
|---|---|---|---|
| silver.hm_article_history | Source-simulated article version; generation+article_id+source version/position | SCD2 by actual source versions, ordered intervals; baseline observation clearly labeled | D01; history365 d; hm_articles; rebuild affected key history |
| silver.hm_customer_history | Source-simulated customer version; generation+customer_id+native op/version | SCD2 after complete CDC transaction; deletes close explicit current state | D01;365 d; customer CDC + txn proof; operation-id replay |
| silver.hm_purchase_current | One source purchase occurrence; generation+purchase_id | Ordered insert/update/delete; source price text+exact decimal; original multiplicity | D01; current/tombstones and365dhistory/replay need; purchase CDC + txn proof |
| silver.rees46_events | Conformant occurrence per source revision; month+original identity+revision | Append validated revisions with same-table REVISION_COMMIT; canonical view selects one approved revision/month | D02;DEV-RET-01; rees46_sftp; no 67M-row destructive overwrite of prior evidence |
| silver.off_product_current | One unambiguous exact code/source generation | Version-guarded current state, RETIRED/DELETED status; retain operation/tombstone ordering | D03; active DEV/replay/closeout horizon; off_products; journal replay |
| silver.amazon_product_current | One exact case-sensitive parent_asin/generation | Version-guarded complete after/tombstone; native Parquet types retained | D04; same; amazon_metadata; snapshot+journal rebuild |
| silver.amazon_review_occurrences | Original occurrence ID; separate namespaced fixture lifecycle identity | Insert original once; fixture edits/retracts explicit; no historical edit inference | D05;DEV-RET-01; amazon_reviews; day/full reconciliation |
| silver.campbell_observations | Original station/logger observation/revision | Raw NTZ, original units/schema family and quality flags; approved station-day revision selection | D06;DEV-RET-01; campbell; native header/epoch replay |
| silver.rees46_kafka_events | One Kafka-source logical original event; source_instance+logical_source_generation+source_event_id | Bounded native-Bronze-stream conformance; exact source-ID MERGE, physical republish counts recomputed; no authoritative lateness dropping | D08/D11+KSG-01; independent Kafka business checkpoint; exact source/physical/invalid accounting; isolated replay/cutover |
| silver.openprices_observations | Accepted acquisition observation_id, source price.id and observed representation | Immutable observed history, not complete native update history; latest-observed view is separate | D07+D10;365 d; openprices_capture; same occurrence replay |

Views: off_identity_exceptions from retained Bronze; rees46_active_events; current/deleted master views; current Amazon/OFF products; API latest observed price; quarantine/partial completeness. Derived views do not create an extra checkpoint authority.

### 13.3 Eight physical Gold tables

| Table | Why materialized / grain / key | Update, source contract and recovery |
|---|---|---|
| gold.hm_purchase_fact |31M+ business-enriched occurrence fact; one generation+purchase_id | Exact price/source date/channel, stable dimension references and completeness flags; affected-key MERGE; D01;§15 |
| gold.hm_purchase_daily | Repeated daily/customer/article/channel queries should not rescan all 31M facts | Date+article+channel+scenario cohort; exact count/source-unit totals; recompute affected groups; D01 |
| gold.rees46_activity_daily | Large411M-event daily product/activity workload | UTCdate+product/category/event_type+reporting cohort; SFTP active revision only; replace affected groups; D02 |
| gold.amazon_review_daily |44M-occurrence ratings/activity workload | UTCdate+parent_asin+original/fixture cohort; count/sum/distribution measures; D04/D05 |
| gold.campbell_station_daily | Business environmental summaries/quality at comparable unit grain | Station+logger day+registered measurement+unit/schema context; D06 |
| gold.rees46_kafka_activity_daily | Independent Kafka streaming business aggregate; logical source generation+UTCevent_date+product/category/event_type/cohort | G08; exact logical counts/distinct users/sessions and separate physical duplication measures; late-group recomputation and independent publication |
| gold.openprices_daily | Currency/type/day price-observation distribution | Observation date+type+exact product/category key+currency+price_per context+cohort; D07/D10 |
| gold.catalog_quality_daily | Required OFF and Amazon catalog-quality trend product | Source domain+published catalog epoch/as-of date+metric ID, no cross-domain product join; D03/D04 |

Physical Gold products retain data rows and a distinguishable publication marker in one target transaction where publication gating is required. Consumer views expose business rows only. Enterprise historical product retention remains the original design; SFTP-derived DEV generations follow DEV-RET-01 closeout, while API D10 retention and other source obligations stay separate. New G08 retains active project/recovery obligations and approved closeout. Detailed measures/dependencies are in§15; these are not merely renamed Silver tables.

### 13.4 Exactly five managed Delta runtime controls

All five are Unity Catalog **managed Delta** tables in the discovered catalog's control schema. Static source/schema/DQ/SLA/retention/routing definitions remain in Git (§9.4). Payloads/large member sets remain in native evidence/source systems or source Bronze; controls carry small typed status and URI/hash references.

| Table | Logical key and minimal typed schema | Allowed writer / mutation and recovery |
|---|---|---|
| control.ingestion_runs | Key(execution_intent_id STRING, stage_id STRING, attempt INT). source_id/dataset_id/entity_id/source_generation/config_release STRING; pipeline/task/run_mode STRING; adf_pipeline/adf_run_id/job_id/job_run_id/parent_intent STRING; actual_execution_id STRING; status STRING; started_at/ended_at TIMESTAMP UTC; error_class STRING; input_scope_json STRING; input_versions_json/target_versions_json STRING (table UUID/version pairs); retry_scope/recovery_generation/reconciliation_outcome/publication_outcome/evidence_uri/hash STRING; row_revision BIGINT | Stage's configured runtime principal writes its own deterministic intent/stage/attempt; one actual execution claims a given attempt after admission. ADF carries native run details and invokes status tasks, not direct concurrent SQL writes. Actual target/native evidence repairs uncertain audit. |
| control.source_deliveries | Key(source_id, source_generation, logical_delivery_id, revision). All key types STRING except revision INT. dataset/entity, manifest_uri/hash, expected_membership_uri/hash, predecessor/version/range, winning_raw/accepted_uri, contract/config_release/recovery_generation STRING; expected_files BIGINT; expected_records/actual_accepted/flagged/quarantined BIGINT (NULL if not yet measured, never false0); raw_validation/bronze_status/reconciliation_status/ack_status/state STRING; target_table_uuid STRING/target_version BIGINT; native_query/run/checkpoint_ref STRING; certificate/evidence/ack_uri/hash STRING; owner_execution_id STRING; row_revision BIGINT; updated_at TIMESTAMP | Sole configured source-delivery workflow actor; guarded expected state/revision/owner transitions. Native file history remains outside. No API frontier writes here; API scope status is only a mirror/reference to accepted ledger evidence. |
| control.dq_results | Key(evaluation_id STRING). execution_intent/actual_execution/source/dataset/entity/source_generation STRING; rule_id/rule_version/config_release STRING; scope_uri/hash STRING; input_table_uuid STRING/input_version BIGINT; evaluated_count/failed_count BIGINT; severity/disposition/status STRING; evidence_uri/hash STRING; evaluated_at TIMESTAMP UTC | Evaluation actor writes deterministic evaluation ID over exact rule/version/scope/input/config; matching duplicate is a no-op; different result/evidence for the same immutable evaluation is a hold. Rerun with changed inputs is a new evaluation linked to original. Stores results, never static catalogue. |
| control.reconciliation_results | Key(evaluation_id STRING). execution/source/dataset/generation/recovery_generation STRING; comparison_spec/version/source_scope_uri/hash STRING; expected_count/actual_count/missing_count/extra_count/conflicting_count BIGINT; membership/multiplicity/content_result STRING; target_table_uuid STRING/target_version BIGINT; expected/actual_digest/evidence_uri/hash STRING; status/debt_outcome STRING; evaluated_at TIMESTAMP UTC | Independent comparison task under runtime identity; deterministic ID binds exact source/target UUID/version/spec/release. PASS requires actual full evaluated scope. Repair from frozen evidence; cannot advance native offsets. |
| control.acquisition_ledger | Key(environment STRING,row_id STRING). record_type STRING HEAD/COMMAND/EVENT; revision/writer_epoch BIGINT; command_id/command_hash/work_id/root_id/traversal_id/page_id/attempt_id STRING; page_number/attempt_number INT where recorded; fixed_bounds/accepted_manifest/evidence_uri/hash/payload_json/hash STRING; actual owner_job/run/compute STRING; timestamps TIMESTAMP; HEAD payload contains bootstrap/tail/sweep/cooldown/receipt state (§8.4) | **API acquisition identity only**, exactly D10 guarded command/HEAD/event atomic mutation. No other source, ADF actor or generic adapter may update its frontier. Uncertain command recovery/old-owner termination is §8.4. |

input_scope_json and reference-list JSON are small metadata columns inside Delta, not separate JSON processing tables. Large source populations are never inserted here. NULL/planned/queued/not-executed is distinct from actual measured0/PASS/completed.

**Deterministic identity and concurrency:**
- Execution intent hashes source generation + logical bounded scope + release + phase + attempt ordinal. Actual ADF/Job run IDs are evidence; a lost submission reuses the same execution token (§9.5).
- Evaluations hash exact rule/spec version + scope + frozen input table/file identities + release. Compare hashes/content on retry; no last-writer-wins replacement of a completed contradictory result.
- Pre-register a source-delivery row under the single source owner, guarded against conflicting manifest/revision. Initialize its unique row once, then mutate with expected row_revision/state/owner. Different source keys may operate concurrently; the same key may not have competing owners.
- Use Serializable conflict detection/guarded transactions where applicable. Informational Delta PK declarations do not enforce uniqueness; validate exact key uniqueness before/after mutation and fail closed on duplicates. Never implement a conditional matched update plus unconditional insert that lets a stale contender insert effects.
- An uncertain commit is resolved from deterministic command/evaluation ID and real target/evidence **after old operation quiescence**. Readback absence while an old write may still run is not permission to retry.
- No time-expiring lease takeover. Job/source exclusivity, actual run handles and terminated writers establish ownership. ADF parent timeout does not terminate its child.
- Retain minimal audit400 days plus active/replay obligations; no payload duplication. A saved certificate remains valid only while its exact file/table version/protocol dependencies are retained.

**Execution state machine:** PLANNED → SUBMITTED/QUEUED → RUNNING → SUCCEEDED/FAILED/CANCELLED/HELD. ORCHESTRATION_UNKNOWN means uncertain child outcome and requires reattachment. NO_DUE_WORK/NOT_READY/SKIPPED are explicit terminal admission outcomes, not completed data scopes. Only started_at/actual_execution proves execution; only target/reconciliation evidence proves a completed stage.

**Delivery state machine:** §7.9 is normative. BRONZE_RECONCILED requires a deterministic PASS certificate, exact target UUID/version/native completion references, complete file/member sets and critical-DQ disposition; ACKED requires contractual continuity and verified actual ACK. Predecessor hold/HELD/MISSING/LATE/SUPERSEDED stay separate from native file progress.

**DQ/reconciliation statuses:** PASS/FAIL/ACCEPTED_WARNING/QUARANTINED where source policy permits, NOT_EXECUTED for planned obligations, ERROR for check failure. Every result binds its evaluated population/count/version; accepted warnings do not erase failed-count evidence. No NOT_EXECUTED result satisfies a readiness dependency.

No sixth offsets table, per-source copies, Azure SQL, SCD control DB, new schema service or generic watermark is selected.

### 13.5 Atomicity and business visibility

Native Kafka sink completion is its Delta commit/native epoch, not a manually inserted offset row. SFTP uses native append transactions/checkpoints for all five; whole delivery readiness is a reconciled guarded source_deliveries transition, not package-wide ACID across micro-batches/tables. API ITEM rows+WINDOW_COMMIT retain D10's atomic window unit. Source mutation+journal/action receipt is a source transaction. Kafka behavior+summary+progress is a Kafka transaction.

Silver tables and Gold tables commit independently. A stage receipt references actual completed target UUID/versions and exact source scope. Silver reads only SFTP BRONZE_RECONCILED eligible delivery scopes bound to an actual target version/certificate, verified API WINDOW_COMMIT units or complete native CDC transaction stages. Dependent work reads those frozen versions and verifies all required markers before starting. No stage reports success because one of several required table writes succeeded.

Gold materializations recompute affected groups, including deleted old groups, and update business rows+the product's publication marker in one target transaction. A reader sees one Delta snapshot; audit repair after that commit does not reapply the data. A cross-product “all domains published together” transaction is not offered.

A dimension/fact view using multiple physical sources publishes an explicit bound input-version set. Either use fixed-version views refreshed at the product's validated publication, or expose a governed query contract that binds those versions. Do not join arbitrary latest independent table states and call that a consistent release.

## 14. Schema, DQ, CDC/SCD and reconciliation architecture

### 14.1 Complete schema registration, compatibility and migration

**Discover → compare → classify → compatibility decision → approve/register → ingest → downstream migration → reconcile/recover.** Discovery uses retained profiling and native runtime schema/header/envelope checks, not a new source profile. Git stores approved descriptors/fingerprints/semantic policies; D11 additionally uses the actual Kafka Schema Registry. No schema database.

Backward compatibility means the new reader can read prior writer data; forward means prior supported reader can read new writer data; full means both; transitive means against every retained supported version, not only the last. These definitions do not make positional CSV, SQL DDL or semantic changes automatically Avro-compatible. D11 FULL_TRANSITIVE is enforced by the real Registry plus actual writer/reader tests, not a project-only string.

Decision codes: **R** = exactly registered change and pinned decoder/target migration already approved, accept at its boundary; **H** = preserve native evidence but hold affected trusted typed processing/ACK as required; **Q** = preserve/capture raw observation and quarantine decoded use where source contract permits (especially Kafka/API); **B** = block conflicting identity, unsupported target protocol or unsafe release. “MINOR candidate” is only a semantic-version review outcome; none of these rows authorizes automatic source contract amendment.

| Change / compatibility classification | Admission / contract version | Bronze behavior | Silver / SCD2 impact | Gold / migration / evidence |
|---|---|---|---|---|
| Identical registered descriptor | R, no schema version change | Preserve original schema/fingerprint | Normal processing | Normal readiness; replay equality |
| Nullable top-level field added | R only if registered reader/open-content policy supports; otherwise H/Q; MINOR candidate | Retain raw + approved nullable projection; no silent ignored column | Add target field explicitly; new tracked attr has no invented prior values | Consumer projection/version review; old/new writer-reader E01 |
| Required field added | H until all writers/default semantics and migration approved; generally MAJOR | Retain original; no fabricated required values | Historical absence explicitly represented; non-null target only after true backfill/evidence | Recompute affected metrics, fail old reader test E02 |
| Field removed | H for protected/consumed field; MAJOR | Old decoder and prior values remain | Retire tracked field under new tracked-set policy; no silent loss of past history | Migrate dependents; replay old and new, E02 |
| Rename / case change | H, generally MAJOR; not drop+add inferred by case-insensitive engine | Exact field names preserved; explicit map only after approval | Target column mapping/protocol compatibility checked, migrate tracked hash input | Exact key/case/oracle, six lowercase Amazon keys retained, E03 |
| Field reorder | R only for verified name-based formats/decoder; CSV/DAT positional reorder H | Exact original order/header retained | Explicit reorder mapping; never align by physical Spark ordinal | Semantic equality E03 |
| Duplicate names (including case-fold collisions) | B typed activation; preserve source evidence | No ambiguous projection | No MERGE into conflicting schema | Repair publisher/parser release, E03 |
| Field reintroduced | H until old/new semantic identity is proved; MAJOR if reused meaning | Keep all schema epochs | New policy/version; never backfill old absence with new values | Historical cohorts/migration E04 |
| Safe type widening | R after exact reader/Delta feature qualification; MINOR candidate only with semantics unchanged | Raw value/type remains | Explicit widen target; exact decimal/overflow checks | Reader/protocol rollback compatibility, E05 |
| Narrowing / string↔numeric/date | H, generally MAJOR | No lossy cast/float/date guessing | Validate every historical affected value; failed values remain accounted; explicit new projection | No aggregate before exact reconciliation, E05 |
| Decimal precision/scale change | H until lossless round-trip or explicitly approved different business measure | Preserve H&M lexical + unconstrained NUMERIC/raw API tokens | Exact precision/scale; no rounding hidden in MERGE; tracked comparison policy explicit | Separate original and rounded metrics if ever approved, E05 |
| Nullability / default change | H unless registered nullable relaxation is demonstrably compatible | Presence, explicit null and omitted distinguished | Defaults are business decisions; no manufactured old values or source history | Denominators/quality/migration tested E02/E06 |
| Key/uniqueness/business grain/source identity change | B normal apply; MAJOR/new source generation where required | Retain previous identities; never hash-dedup into new grain | Re-key/rebuild affected state/history; no cross-generation overlap | Fact rebind and source-authority cutover, E07 |
| Nested nullable field addition | R only under registered/open policy; otherwise H/Q; MINOR candidate | Preserve complete nested source | Typed nested migration, no forced flatten; tracked nested paths versioned | Old/new nested readers, E08 |
| Nested removal/rename/type or nested nullability change | H, normally MAJOR for protected paths | Keep complete old/new raw shapes | Explicit path mapping/rebuild; distinguish absent/null | Metrics/projections review, E08 |
| Array/map element/key type, object↔array | H, generally MAJOR | Native shape retained; no universal explode | Define order/duplicate/null semantics; no accidental row multiplication | Exact nested/cardinality comparison, E08 |
| CSV delimiter/quotes/escape/encoding/header change | H unless exact registered dialect; transport revision distinct from semantic schema | Raw exact bytes/header/record boundaries | Pinned native CSV parser with source-published ordinal; drift cannot renumber occurrences | Reconcile original ordinals/multiplicity, E09 |
| JSON presence/null/type/open extra fields | Open OFF extras raw accepted per D03, protected changes H; API/Kafka Q where required | Exact raw bytes/open fields preserved | Presence flags/explicit type rules; no treating string-number/object as identical | Type-specific DQ/metric migration E06/E08 |
| Native Parquet type/nesting/codec/writer | Codec-only R after integrity qualification; schema change class above | Original Parquet remains native, footer+pages/schema verified | Direct native reader/schema migration; no conversion | Struct-of-lists and keyset/multiset E10 |
| TOA5 logger/unit/header/schema family | H station/package only until exact registered binding; semantic unit change MAJOR | Preserve four headers/lexical values | Correct NTZ/unit dictionary; no universal sentinel or timestamp conversion | Comparable-unit Gold scope only, E09 |
| PostgreSQL DDL / Debezium envelope | H affected transaction conformance; raw CDC capture continues if decodable envelope isolation allows | Preserve native schema/LSN/tx/order and raw record | Qualify source DDL, slot/decoder/replica identity, before/after; migrate tracked set | No source transaction partly published, E11 |
| REST envelope/pagination/filter semantics | H acquisition/acceptance for unsupported envelope; retain HTTP attempt | Never mark an incomplete/new envelope accepted | No invented update/delete feed; pinned serializer/decoder | Document/code recheck + lawful read-only conformance, E11 |
| Avro writer/schema ID/version | Registry FULL_TRANSITIVE plus pinned schema approval; unknown/malformed Q raw capture | D11 raw key/value/headers/offset retained | No pretend known-ID decoding; additional consumers separately versioned | Real IDs, v1/v2 readers/nullable sim_device_context, E12 |
| Delta target schema/protocol/table feature | B incompatible writer/reader; explicit migration | Source schema version separate from target protocol | Explicit schema assignments/ALTER/migration; no session-wide autoMerge | Checkpoint/CDF/views/rollback compatibility E12 |
| Currency/unit/category/event/timestamp/CDC meaning change | H protected use; generally MAJOR even unchanged physical type | Original lexeme/meaning version retained | New semantic epoch/decoder; SCD tracked policy and delete/order interpretation reviewed | Recompute affected metrics; never combine incompatible currency/unit, E07 |
| SCD2 tracked-set semantics changed | H release until migration policy/oracle approved; new tracked-set version, often MAJOR | Source operation history intact | Rebuild only evidenced affected histories; unknown historical attr remains unknown | Explicit dimension SK/fact rebind and Gold correction, E04 |

A raw-preservation Q does not mean semantic acceptance. Strong source/package DQ can still block SFTP successful ACK; API window completeness/identity remain D10-controlled; D11 raw Kafka capture must survive unknown schema or semantic DQ failure. Never silently accept every additive field, drop malformed records, infer renames or claim mergeSchema implements semantic compatibility.

Target policy: explicit immutable input/schema/release; autoMerge session default off; exact columns and narrow approved target evolution only. Stage migration under exclusive writer, freeze old table UUID/version and checkpoint compatibility, validate candidate/affected keys, reconcile before view/publication switch. Restore compatible release/state together; no blind code rollback across upgraded protocol or incompatible checkpoints. Structured temporary processing stays DataFrames/managed Delta only when justified; native formats remain evidence.

Essential source-specific rules:
- H&M source NUMERIC + exact lexical text and exponent acceptance; no float conversion.
- REES46 original IDs/price/time/blanks preserved; unknown future nonblank event type is a warning/review case, not silently rewritten as a known event.
- OFF open raw JSON; exact code strings and explicit ambiguous/blank identity population.
- Amazon metadata16 native fields: images/videos are structs of lists; details/price/bought_together are strings, rating_number int64. Six lowercase keys remain case-sensitive.
- Reviews ten original fields, exact text/images and epoch milliseconds; occurrence identity independent of user/asin/time or payload hash.
- Campbell exact four header records and station families; timestamp_ntz, unit strings including the WindDir_D1_WVT conflict; no uniform sentinel→NULL replacement.
- Open Prices native REST numeric/serializer representation is independently pinned; lexical JSON tokens/raw item substrings retained. Export decimal(10,3)/null counts are not universal live REST schema guarantees.
- Kafka actual writer ID/schema bytes, complete payload consumption and full raw transport metadata.

### 14.2 DQ authority and execution placement

| Source | Exact retained identified rule family | Required placements |
|---|---|---|
| H&M |36 total: HM-ART, HM-CUS, HM-TRX, HM-CDC, HM-JNL, HM-ALL fromD01 R07 | Source import/transaction; exact numeric/FK; journal/CDC member checks; Bronze; Silver history/Gold semantics; security/recovery tests |
| REES46 SFTP | REE-001…036 | Original seed/part integrity; schema/occurrence/month; package/revision; Bronze/Silver; complete source and Gold reconciliation |
| OFF |30category statements inD03 R07, no original individually identified executable predicates | Selected project mapping is §14.2.1; implementation/evaluation is required, not a new architecture choice; source catalog/journal/manifest/row/nested/security checks |
| Amazon metadata | AMZ-DQ-001…036 | Full shard/keyset/multiset; native footer+pages; exact keys/types; source transaction/journal; manifest/range; replay/ACK |
| Reviews | REV-001…036 | Full/day original ordinal allocation; raw field types/anomalies; parent key relationship; part/revision; exact capture/replay/security |
| Campbell | DAT-DQ-001…040 | Seed/header/station/row; timezone/epoch; anomaly/unit checks; station-day membership; revision/schema/replay/ACK |
| API | OPR-DQ-001…045 | Pinned export checks only for that offline release; live envelope/window/request; raw bytes/observation; accepted/Bronze/replay/security |
| Kafka | KAF-DQ-001…046 | Seed/identity; serializer/routing; transaction/fencing; raw inspector; native checkpoint; full physical/logical reconciliation; isolated destructive tests |

The source traceability/R07 catalogue is authoritative: H&M36 + REE36 + AMZ-DQ36 + REV36 + DAT-DQ40 + OPR-DQ45 + KAF-DQ46 = **275 identified rules**. Routing now references the same ID sets; no rule was invented/reduced to preserve a total. D03's30prose categories and their30project OFF-A41 mappings remain a separate classification, not additional original rule IDs. All original wording/severity/owner/tolerance/disposition is retained. The30 OFF categories add obligations, not30 claimed successful/executable tests. No new rule catalogue is stored in Delta; Git holds approved definitions, dq_results holds evaluations.

For OFF, the future mapping must explicitly separate seed-release assertions, per-row predicates, global code conflicts, source transaction invariants, package membership and operational faults. Example: “conflicting duplicate-code detection” is an exact grouped identity comparison plus canonical exclusion, not code IS NOT NULL. “Open-content preservation” requires raw/parsed round-trip membership evidence, not an invented closed334,949-column schema. Missing operator/tolerance/fixture status is UNQUALIFIED, not PASSED.

DQ results distinguish NOT_EXECUTED, PASS, FAIL, ACCEPTED_WARNING, QUARANTINED and genuinely NOT_APPLICABLE. Prior profile evidence is not a runtime execution. Export-only API rules retain their offline scope; they are not waived or falsely asserted against current live counts.

Critical identity/member/sequence/part failures hold logical completion. Noncritical historical anomalies use the source's preserve/flag policy. Accepted+quarantined+rejected-accounted populations reconcile without losing original physical occurrences. Gold quality percentages disclose valid/flagged/excluded denominators.

### 14.2.1 Selected executable mapping of OFF's30prose categories

D03R07 supplies categories, not original rule IDs. The following **project OFF-A41-001…030 mapping v1.0.0** supplies concrete future predicates without replacing the source authority. All tests are NOT EXECUTED. Zero tolerance applies to lost committed operations/identity/integrity. Known FULL anomalies match the pinned canonical/exception profile; optional/open fields do not become required. Hold applies to the affected canonical/typed/package scope as shown and source-required ACK conditions, never deletion of immutable raw evidence.

| Project ID / D03 category | Executable comparison / oracle | Severity / disposition |
|---|---|---|
| OFF-A41-001 / Compressed integrity | Stream full compressed bytes; exact source SHA/length and gzip CRC/EOF equal frozen manifest | CRITICAL/hold package |
| OFF-A41-002 / JSONL boundaries | Every declared logical record has one complete valid UTF-8 JSON value; no silent skipped/truncated boundary | CRITICAL/hold package |
| OFF-A41-003 / Original counts | Original FULL raw row count equals pinned profile; DELTA operation count equals exact seal | CRITICAL/hold scope |
| OFF-A41-004 / Code presence/type | Classify missing/blank/nonstring code explicitly; canonical eligibility requires exact nonblank string | ERROR/exception population |
| OFF-A41-005 / Conflicting duplicates | Group exact code; compare full original payloads/multiplicity; never choose an arbitrary conflict winner | ERROR/identity exception |
| OFF-A41-006 / Raw/exception accounting | Original occurrences = canonical members + identified duplicate/conflict/invalid exception occurrences under pinned profile; exact anti-joins | CRITICAL/hold missing accounting |
| OFF-A41-007 / Leading zero | Raw code lexeme equals preserved canonical code byte string; no numeric cast | CRITICAL/hold identity corruption |
| OFF-A41-008 / Optional name | Missing/null registered product-name optional fields do not reject an original record | ACCEPTED_WARNING/optional |
| OFF-A41-009 / Timestamp type | For protected timestamp paths, compare observed presence/type against registered source descriptor and accepted original anomalies | ERROR/hold protected use |
| OFF-A41-010 / Source revision type | Validate source rev presence/type against native descriptor; never overwrite publisher revision with simulation version | ERROR/hold protected use |
| OFF-A41-011 / Publisher schema version | Pin original publisher schema marker separately from source operational envelope schema/version | ERROR/hold decoder mismatch |
| OFF-A41-012 / Nested compatibility | Compare registered protected nested types and array element/object shape; retain open extras | ERROR/hold protected use |
| OFF-A41-013 / Open content | Raw original JSON hash/content equals retained Bronze raw value; no unapproved field/nested loss | CRITICAL/hold data loss |
| OFF-A41-014 / Catalog uniqueness | Canonical catalog has exactly one eligible key per source generation; exceptions excluded by explicit IDs, not silently dropped | CRITICAL/hold catalog admission |
| OFF-A41-015 / Journal atomicity | Every committed source transaction has complete expected member IDs and source receipts, no partial export | CRITICAL/hold whole DELTA |
| OFF-A41-016 / Journal sequence | Sealed sequence/member set has no unexplained gap/extra and matches source journal; never max-sequence acknowledgment | CRITICAL/hold whole DELTA |
| OFF-A41-017 / Version/operation | Each committed op has valid previous→new per-key version and source action ownership; RETIRE distinct from DELETE | CRITICAL/hold whole DELTA |
| OFF-A41-018 / After image | INSERT/UPDATE/RETIRE carry complete valid after object with exact product_code/version; no patch inference | CRITICAL/hold whole DELTA |
| OFF-A41-019 / Tombstone | DELETE has correct key/version/operation ID and registered null after/tombstone; no fabricated image | CRITICAL/hold whole DELTA |
| OFF-A41-020 / Manifest completeness | Required source/generation/range/schema/predecessor/revision/empty/exception fields all present and coherent | CRITICAL/hold package |
| OFF-A41-021 / Parts/hashes | Exact unique expected path set, count, byte length/SHA; no extra/missing accepted parts | CRITICAL/hold package |
| OFF-A41-022 / Delivery/revision | All files/operations/manifest share the declared source delivery/revision and export seal | CRITICAL/hold package |
| OFF-A41-023 / Exact retry | Same source delivery/revision/manifest has identical source content; retry yields matching target identity set and no new logical effects | CRITICAL/hold conflict |
| OFF-A41-024 / Correction/supersession | Supersession names a real prior revision; serialization repair has same business membership; business changes need new source operations | CRITICAL/hold unsafe correction |
| OFF-A41-025 / Empty DELTA | Explicit empty seal has zero ops/exact declared sequence semantics and valid native empty file; not inferred from missing delivery | CRITICAL/hold false empty |
| OFF-A41-026 / Predecessor/range | Contiguous eligible predecessor and exact range/operation set; later delivery cannot close a missing earlier range | CRITICAL/hold contiguous ACK |
| OFF-A41-027 / Safe fixture | Action/target/after changes restricted to approved simulation namespace/key/fields; original seed bytes untouched; source action receipt exact | CRITICAL/hold unauthorized mutation |
| OFF-A41-028 / Drift/decoder | Observed schema/semantic fingerprint classified by registered version/policy; unsupported protected change held; old decoder replay available | CRITICAL/hold trusted activation |
| OFF-A41-029 / Sensitive metadata | Contribution/user/raw payload absent from ordinary logs/default public views; approved restricted evidence/access and attribution | CRITICAL/block unauthorized disclosure |
| OFF-A41-030 / End-to-end receipt | Source→manifest→Raw→accepted→frozen Bronze UUID/version exact identities/multiplicity/content and control certificate match | CRITICAL/hold acceptance/ACK |

These predicates bind exact source/schema/scope/profile references in the future Git rule release; evaluations go only into dq_results/reconciliation_results. Original275 identified rules are preserved with their original IDs/severity/tolerance/owner/disposition; these 30project mappings are not another30 original authority IDs or a claim305tests passed.

**Campbell trusted-measurement policy selected:** dictionary keys are exact (registered station/header-family,field,unit), built from already retained four-header source evidence. Parse finite numeric values only where that registered field permits; keep NAN/missing/suspect flags and original lexemes. Exclude flagged -99/999 from trusted statistics unless that exact source field's approved rule explicitly permits the value. Exclude time/RECORD/components from sensor aggregates. Temperature/humidity/battery/pressure/wind-speed/rain use their own registered native unit, no guessed conversions; rain uses source-defined total/increment semantics, never cumulative-counter summation by assumption. Direction never gets arithmetic mean: use a separately approved circular statistic with valid bearings, or publish coverage/quality only. The known WindDir_D1_WVT unit conflict remains excluded from trusted comparable statistics until source evidence resolves it; original values/quality counts remain visible. This chooses the business policy now; actual dictionary/schema checks and numeric/reconciliation tests are execution gates.

### 14.3 CDC/SCD and idempotency by source

| Source | Logical effect identity and business apply |
|---|---|
| Articles | Fixed source journal operation IDs/member set; source version controls SCD2, not max sequence/time |
| H&M CDC | Snapshot generation+entity+PK for snapshot; actual native transaction/LSN/order identity for operations; wait for complete transaction; deterministic apply |
| REES46 SFTP | Original source instance/gzip hash/ordinal + explicit delivery revision; active month selected after complete revision acceptance |
| OFF/Amazon metadata | Source instance/generation/change ID/sequence and per-key version; repeated UPDATEs retained, explicit tombstones, RETIRE distinct |
| Reviews | Original source hash+ordinal; fixture lifecycle IDs separately namespaced; original repeated observations never collapse |
| Campbell | Original file/ordinal and verified station/logger/program/raw timestamp/RECORD context; explicit revision, no invented epoch |
| API | Original accepted acquisition observation_id; source price.id is entity identity; overlap/sweeps remain observed history, latest entity selected only in business projection |
| REES Kafka | Physical cluster/topic UUID/partition/offset for Bronze; stable original event ID for source accounting; application attempt header distinguishes new publication |

SCD2 histories use actual source operation position/version and deterministic interval construction, not ingestion arrival order. Multiple versions sharing one source commit timestamp use native order/version tie-breaks; intervals must not overlap. Late source versions rebuild affected key intervals and repair dependent facts. Deletes close state with explicit tombstone status; no absence-derived deletion.

Baseline master attributes are observed static seed state. They are not certified retailer attributes “as of” purchases years earlier. Original history and simulated source lifecycle are visibly distinct.

### 14.4 Exact reconciliation levels

1. **Seed-to-source:** hashes/counts/allocations, source PK/FK, OFF canonical/exceptions, original repeated occurrences and exact price round-trip.
2. **Source-to-delivery:** fixed journal membership/version/ranges; source transaction complete; deterministic source FULL/day/month membership.
3. **Delivery-to-Raw:** exact named part set, complete bytes/SHA, schema/header and explicit empty status.
4. **Raw-to-Bronze:** every original occurrence/operation accounted at the target UUID/version; same-revision retransmission not new logical effect.
5. **CDC/Kafka:** native committed member identity and isolation; physical duplicates separate from logical source operations; no assumption all integer offsets correspond to visible records.
6. **Bronze-to-Silver:** operation/ordinal coverage; valid/flagged/quarantined dispositions; SCD intervals/current versions/deletes; no multiplicity loss.
7. **Silver-to-Gold:** fact counts/exact price-unit sums, rating buckets, eligible event/session denominators, sensor quality counts, currency groups and changed/deleted contributions.
8. **Business publication:** target publication marker, bound input versions, expected scope/completeness and actual consumer query/refresh evidence.

Counts alone are not an exact equality proof. Use distributed key anti-joins, per-identity multiplicity, in-bounds exact ordinal coverage and independently recomputed content/membership digests. Retain the contract's exact keyset/multiset/hash algorithms; a different distributed digest needs a named amendment, not the same label.

### 14.5 Article-versus-CDC readiness conflict

Newly activated articles arrive through15 min journal polling, while their purchases can arrive through continuous CDC. Therefore a requirement for fully enriched category data within the CDC15 min consumer SLA would conflict with the approved article path.

**Selected business amendment AM-11:** purchase counts and exact source-unit facts remain timely with original article_id and an explicit PENDING_ARTICLE dimension reference when its trusted attributes have not arrived. This is a technical unknown member, not an invented source article. Never inner-join/drop that purchase. Category/attribute-complete views expose coverage and wait for journal enrichment; their bound is the article60 min consumer objective. Resolve the placeholder and recompute old/new affected groups after journal capture. Consumer contracts distinguish fact freshness from attribute completeness. No article journal interface bypass is added.

### 14.6 Enterprise SCD Type 2 for H&M articles/customers

These are the two selected SCD2 targets, not generic copies. Purchases remain an ordered occurrence/current fact state. All changes after baseline are **simulated future source operations**, never invented historic retailer versions.

| Target | Key / tracked business attributes | Nontracked / source order |
|---|---|---|
| silver.hm_article_history | (source_instance,source_generation,article_id). All original article business columns except article_id are tracked, including exact code lexemes, names/classifications and detail_desc; approved sim_status lifecycle tracked. | Ingestion timestamps/run/transport/schema hashes/operational source_updated_at are audit, not reasons to create a new business version. Journal operation_id/source transaction/record_version/sealed member order govern causal identity. |
| silver.hm_customer_history | (source_instance,source_generation,customer_id). FN, Active, club_member_status, fashion_news_frequency, postal_code, age and approved sim_status tracked; preserve original lexical flag meaning. | Same audit exclusions; native committed transaction/LSN/transaction ordinal plus record_version, not Kafka arrival order or Spark partition, determines source operation order. |

**Model:** source key/generation; dimension_version_id and dimension_sk (domain-separated SHA256 of key/generation + originating baseline/operation identity + tracked-set policy version, deterministic across replay); originating and latest observed source operation/transaction/position/version; schema_version/decoder/tracked_set_version; exact tracked payload and collision-safe null-aware canonical hash; effective_from/effective_to UTC TIMESTAMP; effective_order_from/effective_order_to tie-breakers; is_current; lifecycle ACTIVE/DELETED; is_zero_width; baseline_observation_id/observed_at; complete source/ingestion/recovery lineage. Effective_to=NULL denotes open end. Business timestamp interval is **[effective_from,effective_to)**. PK declarations do not enforce logical uniqueness.

**Source-effective-time policy and material simulation overlay AM-13 (selected for approval):**
- Original CSV masters contain one observed static state. Freeze baseline_observed_at at the actual consistent source snapshot; do not assign validity at an earlier purchase date. Original masters have no proven historical changes.
- Normal future changes use source commit time with native source order; delayed arrival retains that earlier source time. Do not backdate source_updated_at to manufacture business effectiveness.
- To demonstrate genuine **backdated simulated business changes** and timestamped simulated fact relationships, extend the versioned simulated source action receipt contract with explicit sim_business_effective_at, time_basis/timezone and action/operation membership. Existing simulation_action_id/action_id ties the receipt to the exact committed journal/CDC operations. This is a **material source-behavior/operational-metadata amendment R4 overlay/2.0.0**, not a claim that D01 already supplies that field, not a source CSV change and not native historical evidence. No new control database/table is needed; reuse source-owned action receipts/evidence.
- An operation requiring this overlay is ineligible until its immutable matching source action receipt is available/validated. Unsupported/missing/contradictory effective metadata holds conformance; normal declared commit-time operations do not silently borrow a guessed effective time.
- Backdating before the first observed baseline is held as HISTORICAL_STATE_UNAVAILABLE unless a separately approved evidenced baseline exists. No interval reconstruction can recover unobserved prior states.
- Source order proves causality and latest full source state; effective-time order serves the explicitly simulated business history. Backdated actions declare a complete replacement state **for that effective point**; later effective versions remain. The backdated fixture also emits an approved normal-effective reassertion of the intended current state in the same complete source action/transaction group where feasible (otherwise a separately sealed complete action). The source end state and final effective terminal state must reconcile; a past-only after-image must not accidentally replace the current source state. Do not confuse this with patching an earlier full after-image into every later version.

**Deterministic changed-key algorithm (distributed PySpark):**
1. Admit only complete eligible journal seals/native CDC transactions and their required action metadata. Separate snapshot read events, committed INSERT/UPDATE/DELETE and null Kafka tombstones; a tombstone is not a second delete.
2. Extract changed natural keys; read their baseline and all retained eligible source operations from Bronze, with pinned source schema/decoder/policy. Deduplicate **the same operation identity** only when identical; conflicting repeated op/version holds. All legitimate distinct source operations remain in Bronze.
3. Validate per-key native causal order/versions, transaction membership and before/after consistency under the source policy. Late arrival waits for missing required predecessors; do not apply an incomplete source transaction. Snapshot/WAL overlap at the same version/state contributes one source state, not duplicate history.
4. Normalize exact tracked values with presence/null/type semantics. Use distributed key partitioning and window functions ordered by (business effective timestamp, declared effective order/native causal tie-break). No timestamp/ingestion-only last-write-wins and no driver key loops.
5. At one timestamp retain distinct material source states with deterministic order. Intermediate equal-time states have [T,T) scalar intervals and is_zero_width=true; keep them as source version audit, exclude them from ordinary timestamp as-of joins. The last causal state at T governs [T,nextT). If a future consumer needs intratimestamp state, require the composite (timestamp,order) coordinate; do not invent microseconds.
6. Collapse consecutive **unchanged tracked states** in the logical interval projection, while retaining all source operations in Bronze and last-source-position audit. Nontracked-only updates do not open a new SCD version. NULL→value/value→NULL and ACTIVE→DELETED/reactivation are material state transitions.
7. Reconstruct all intervals for each affected key using lead(effective_from/order), derive current/deleted status, and compute stable logical version keys anchored to the actual state-origin operation. Late/backdated changes rebuild affected keys; timestamps of existing intervals may be repaired without inventing earlier values.
8. Compare rebuilt versus existing affected histories. Build one unique action per target dimension_version_id: INSERT/UPDATE or explicit DELETE for obsolete reconstruction rows. Perform **one atomic Delta MERGE per dimension target** with explicit assignments; delete only enumerated affected version keys. Do not use unbounded WHEN NOT MATCHED BY SOURCE. Do not expire in one transaction and insert a replacement in another.
9. Validate invariants and record actual target UUID/version/input-operation set/tracked policy/hash in existing dq_results/reconciliation_results and ingestion_runs. Replay of the same frozen eligible operation set/policy produces identical logical history, independent of transport attempts/processing time.
10. Only after every required customer/article/purchase target stage is complete, publish dependent fact repair against a bound input-version set. Tables commit independently: a partial transaction stage is held from business release and resumed idempotently; no cross-table ACID assertion.

**Delete/reactivation:** DELETE closes the preceding active interval and opens an explicit DELETED interval (key/provenance retained; business payload nullable as registered). Its terminal interval can be is_current=true while the current-active view excludes it. Reinstate opens a new ACTIVE interval at its proven effective point. No deletion inferred from missing snapshot rows or a null transport tombstone. FK/source delete rules remain enforced; fixtures use approved keys/actions, not arbitrary destructive source operations.

**Safety/performance:** one owning conformance workflow per target; fixed changed-key scope; repartition by key, distributed window/sort, measured skew and affected-target pruning. Never broadcast full history or collect keys/operations. Large same-key histories are measured, bounded and held if their oracle cannot fit; do not arbitrarily truncate to the last event. Persist only bounded reusable DataFrames as needed; no permanent new staging table. Materialize action source for deterministic retries; on uncertain MERGE, quiesce old writer, compare actual source version set/target content/commit, then return prior result or recompute against current state. Conflicts re-read/rebuild under the same exclusive owner; no blind replay of stale actions.

**SCD invariants:** unique (generation,key,dimension_version_id); exactly one open terminal interval per observed key (including a deleted terminal state); no overlap among nonempty intervals; no unexplained gap inside the observed lifecycle; zero-width rows flagged; all source effects/unchanged operations accounted; first validity not before evidence; source positions/versions consistent; active/deleted/current views consistent. These are executable future DQ definitions in Git, results in the existing controls.

### 14.7 Historical Gold joins and SCD schema repair

G01 keeps every purchase occurrence with exact price, original customer_id/article_id, source date and dimension completeness/time-basis flags. A dimension reference is never obtained by joining arbitrary latest masters.
- Original t_dat is date-only and purchases precede the newly observed master snapshot. Set HISTORICAL_MASTER_UNAVAILABLE and unresolved historical dimension SKs where no genuine as-of state exists. Optional baseline/current association is explicitly labelled BASELINE_ASSOCIATION and cannot be described as historically accurate.
- For selected future simulation facts with an approved exact business timestamp under AM-13, as-of lookup uses fact time in [from,to), the correct source generation and ACTIVE dimension state. Date-only facts can be assigned only where the declared source calendar and dimension history prove one unambiguous state for the entire relevant date; otherwise DATE_PRECISION_AMBIGUOUS. No invented midnight/UTC conversion.
- Fact-before-dimension remains present with PENDING_ARTICLE/PENDING_CUSTOMER; late dimension/history repair recomputes affected SK references and both old/new daily contribution groups. Historical-unavailable differs from temporary pending.
- Bind fact publication to actual article/customer history and purchase-state UUID/versions. Lookup must return at most one applicable nonempty active interval; anti-joins/duplicate matches/coverage/old-new group totals reconcile. Late repair cannot remove a legitimate purchase through an inner join.
- Stable dimension SKs follow logical state origins. If tracked-set/schema migration changes that key policy, rebuild/rebind the affected fact scope under a reviewed publication release; never silently leave dangling SKs.

New tracked attributes start with UNKNOWN_BEFORE_SCHEMA unless earlier source evidence genuinely contains them; removal/rename/type/null/key changes use §14.1, a new tracked policy and affected-key reconstruction. Business history cannot be backfilled from a new default/current value. Retain old decoders and observed history for replay; if required Bronze/CDF/history fell outside retention, hold unsupported repair and rebuild from retained evidenced baseline, not a guessed current table.

## 15. Eight Gold business data products

### 15.1 Shared business and publication rules

Business domains stay separate. Do not join H&M customer IDs to REES46/Amazon users or unrelated product IDs. Only verified native H&M relationships and exact Amazon parent_asin relationships are approved joins. OFF↔Open Prices barcode enrichment is deferred until code/reference coverage, identity conflicts, licensing and type-specific semantics are verified; it is not needed for a usable prices product.

Every product exposes source generation, publication ID/time, bound input versions, data_origin/scenario cohort, completeness/quality measures and relevant as-of semantics. Default original-source reporting includes original occurrences plus approved corrections/replayed source changes, excludes isolated fixture namespaces and identifies simulated source lifecycle fields. Filtering scenario_id=BASE alone must not accidentally exclude a legitimate correction to an original occurrence.

A successful Bronze load is not Gold publication. Incremental processing identifies affected business keys/groups from old and new Silver values/CDF, rebuilds those groups from eligible source state and atomically replaces their contributions. Empty/deleted groups are removed explicitly. Never add a daily aggregate again on retry. Publication markers and source/target reconciliation use actual target commits.

Views suffice for governed dimensions/current catalog projections and low-frequency detailed access. Materialize only repeated expensive fact/enrichment and daily quality/aggregation workloads. These eight tables and named views constitute a focused catalogue, not dozens of decorative marts.

### G01 H&M purchase occurrence fact — physical hm_purchase_fact

| Definition | Selected business contract |
|---|---|
| Domain / purpose / consumer | Retail purchase analysis; customer/article/channel investigation for simulated merchandising analyst |
| Type / grain / identity | Fact; one current original or approved source purchase occurrence; key(source_generation, purchase_id), not an order |
| Dimension relationships | Exact article_id/customer_id/channel/date; deterministic history-version SKs where §14.7 evidence supports as-of assignment; explicit PENDING/UNAVAILABLE/DATE_PRECISION flags; no invented cross-domain identity |
| Measures | purchase_occurrence_count=1 for an active eligible occurrence; exact source_price_units; source event date; no verified revenue/currency/payment measure |
| Filtering / missing | Default original-source cohort and approved corrections, exclude isolated fixtures/deleted purchases from active views; preserve raw failed projections and show excluded/pending counts |
| Temporal / authority | Original purchase t_dat, separately actual simulated source commit and baseline-as-observed master attributes; hm_purchase_current is transaction authority |
| Refresh / corrections | Process complete source transaction stages; idempotent affected-key MERGE; update old/new group contributions on date/article/channel/amount correction, close/delete active effect on explicit source delete |
| Publication / reconciliation | Fact identity/count/amount exact against eligible Silver; dimension join cannot multiply/drop facts; input Silver versions and fact publication marker in target |
| Freshness / DQ | Purchase consumer p99≤15 min after eligible source commit, including PENDING_ARTICLE if needed; enriched attributes follow article60 min objective. Zero lost/multiplied occurrences; exact decimal fit mandatory |
| Example questions / limits | “How many purchase occurrences by article/channel/date?” “Which purchases await article enrichment?” No unique order count, currency revenue, return rate, consent or authentic historical master-as-of claim |

**Dimension views:** gold.hm_article_dimension and hm_customer_dimension expose deterministic generation+key+state-origin/tracked-policy surrogate keys (§14.6), valid-from/to/current/deleted status and approved descriptive fields from Silver histories at a published bound version. Missing article/customer links use a clearly technical unknown reference without inventing source rows. Pseudonymous customer details are restricted; postal_code is not a default analyst field. Versioned baseline attributes describe the seed observation, not verified2018retailer state. Current-state and version-history views are separate.

### G02 H&M daily purchase activity — physical hm_purchase_daily

| Definition | Selected business contract |
|---|---|
| Domain / use / consumer | Retail daily assortment/channel analysis; merchandising/operations analyst |
| Type / grain / identity | Aggregate; one(t_dat, article_id, sales_channel_id, source_generation, reporting_cohort) |
| Dimensions | Date, exact article and explicitly labelled historical-as-of/baseline/current association policy, channel source code; customer remains in detailed fact; no claim current enrichment is historical |
| Measures / KPIs | Active purchase occurrence count; exact source-price-unit sum; average=sum/count with explicitly displayed numeric precision; pending-dimension and excluded-projection counts |
| Filters / missing | Include every eligible original purchase multiplicity; no payload dedup; null/invalid trusted amounts are separately accounted, not replaced with zero |
| History / authority | G01eligible fact snapshot; date is purchase date. Category/channel labels only from supported attributes, no invented retail channel interpretation |
| Refresh / changes | Recompute all affected old/new date/article/channel groups; deletes remove contributions; late enrichment changes attribute views and pending counts |
| Publication / DQ | Exact aggregate counts/sums versus bound G01; no overflow/rounding in stored totals; publication marker atomic within this table |
| Freshness | Purchase updates Gold p99≤12 min/consumer≤15; article-driven enrichment Gold≤50/consumer≤60 under AM-11 |
| Questions / limits | “Daily activity by article or channel code?” “Where is missing enrichment?” Source price-unit totals are not cash revenue; distinct customers must be computed from fact and cannot be summed across article groups |

G01exists to provide a governed reusable business fact. G02exists because repeated daily queries should not repeatedly perform31M-row enrichment/aggregation. They are not duplicate Silver aliases.

### G03 REES46 product activity and funnel views — physical rees46_activity_daily

| Definition | Selected business contract |
|---|---|
| Domain / use / consumer | E-commerce behavior/product engagement; behavior analyst |
| Type / grain / identity | Aggregate; one(UTCevent_date, product_id, category_id, category_code, event_type, source_generation, reporting_cohort) |
| Source / dimensions | **SFTP active approved monthly revision only**; source product/category/brand context, no invented product master or user crosswalk |
| Measures | Original occurrence count, per-group distinct source users/sessions, blank-session count and DQ/coverage counts. A purchase token counts an event, not an order/payment |
| Filters / missing | Accepted original/approved correction cohort; optional blanks retained in explicit null/blank groups; unknown future event token visible separately |
| Temporal | Event-time UTC date; source publication/ingestion clocks separate; current active revision of each business month |
| Refresh / corrections | Recompute affected month/date/product groups on late month or November r2; do not UNION SFTP and Kafka facts |
| Publication / DQ | Totals reconcile to eligible SFTP active occurrences by event type/month; original repeated rows remain; explicit missing-month coverage status |
| Freshness | Gold≤240 min/consumer≤270 min fromREADY; approved previous-month publication by08:00 UTC, with breach recorded for late/missing source |
| Questions / limits | “Which products have views/carts/purchase events?” Distinct group users/sessions are nonadditive; no causal attribution, verified sales revenue or global cross-file user count claim |

**gold.rees46_session_funnel** is initially a view over eligible Silver events, not another physical fact. Grain is(exact user_id, nonblank user_session, product_id, source_generation, reporting_cohort), spanning the full selected active-period history so midnight/month boundaries do not split a source session token. Count one group with view as the denominator. A completed staged funnel has view then cart then purchase in deterministic(event_time, original month, original ordinal) order. Define view→cart, cart→purchase and full view→cart→purchase numerators separately; a purchase without preceding cart is not a completed three-stage funnel.

Attribute the group to first qualifying view UTCdate for cohort reports. Multiple original events are retained, but a group contributes at most once per specified funnel stage. Blank sessions are excluded from this denominator and reported separately; do not fabricate sessions or use all events as conversion denominator. Equal timestamps use the declared ordinal tie rule, which is deterministic rather than proof of causal order. No24 h timeout is invented. A correction/backfill recomputes affected complete source-session/product groups, not only arrival-day rows.

**gold.rees46_kafka_operations** exposes broker-append→Bronze latency, inspection statuses, physical/logical counts, partition distribution and retained-source coverage from the canonical Kafka target/control evidence. It is operational/comparison reporting, explicitly excluded from G03business totals. A future materialized funnel is deferred until measured query cost justifies it.

### G04 Amazon review activity — physical amazon_review_daily

| Definition | Selected business contract |
|---|---|
| Domain / purpose / consumer | Product feedback activity, rating distributions and review coverage; catalog/research analyst |
| Type / grain / identity | Aggregate; one(UTC review date, parent_asin, source_generation, reporting_cohort) |
| Relationships | Exact case-sensitive review.parent_asin→metadata.parent_asin. Never substitute child asin. Product attributes are a separately published current snapshot unless a true historical alignment is later verified |
| Measures | Original occurrence count; valid rating1…5count/sum and five distribution buckets; zero/outside-range count; verified_purchase true/false counts; nonnegative helpful-vote sum and negative anomaly count |
| KPIs / denominators | Average valid rating=sum(valid1…5ratings)/valid_rating_count, NULL when zero; zero-rated originals remain in total occurrence count. Product review coverage=distinct eligible metadata keys with≥1 original review / eligible current catalog product keys |
| Filters / missing | Preserve477,968 repeated raw-hash excess occurrences, two rating0 records/four negative helpful votes and permitted blank text; exclude isolated fixtures by default, not original anomalies |
| Temporal / truth | Original epoch-millisecond review event; source commit clock not invented. Silver original occurrence identity is authority |
| Refresh / late changes | Recompute affected old/new day/product groups for original late packages or approved revisions; explicit fixture edit/retract affects fixture cohort only |
| Publication / DQ / freshness | Exact occurrence and rating-bucket reconciliation; no duplicate/drop from product join. Increment Gold p95/p99≤35/60 min, consumer≤45/75 min, business99%≤90 min fromREADY |
| Questions / limits | “How does review activity/rating distribution vary by product/day?” “Which current catalog products have source reviews?” No guaranteed unique real-world review count, fraud/causality/orders or contemporaneous catalog assertion |

**gold.amazon_product_dimension** is a governed current product view, with exact parent_asin, native optional/nested fields and opaque source price string. It does not need another physical copy of1.61Mcurrent Silver rows.

**gold.amazon_review_fact** is an occurrence view exposing review ID-as-occurrence, event time, parent/child ASIN, rating/verification/quality, and explicitly `metadata_alignment=CURRENT_REFERENCE_NOT_HISTORICAL`. Restricted text/user/image fields require grants; default aggregates do not expose them. Original unmatched-parent count was0in the pinned evidence, not a permanent future FK guarantee.

### G05 Campbell station/day environmental observations — physical campbell_station_daily

| Definition | Selected business contract |
|---|---|
| Domain / use / consumer | Station-day environmental observations, completeness and sensor-quality; environmental analyst |
| Type / grain / identity | Long-form aggregate; one(station_id, logger context, raw logger date, registered measurement field, unit, header/schema context, reporting_cohort) |
| Dimensions / truth | Published station/logger/header dictionary and approved active station-day revision from Silver; timestamp_ntz date, not converted UTC |
| Measures | Observed record count; numeric finite count; NAN/missing-like and field-specific suspect-token counts; observed min/max/mean of registered comparable readings; appropriate Tot fields sum available reported totals |
| Interpretation | Mean of recorded hourly averages is labeled mean_of_reported_values, not a complete24 h environmental average. Rainfall total is observed_available_total, not imputed daily rainfall |
| Missing/invalid | No generated hourly rows. Unknown unit conflicts get quality counts and no trusted comparable aggregate. -99/+999 stay raw/typed with flags, not universal NULLs; RECORD999 is never a sensor sentinel |
| DQ gate | Separate all_finite statistics from trusted statistics. Trusted eligibility requires an approved field/unit/plausibility dictionary; where R1 supplies no numeric plausibility bound, trusted value remains UNQUALIFIED rather than inventing one |
| Refresh / corrections | Recompute affected station/date/field groups for late/corrected files; preserve independent station progress and source schema versions |
| Publication / reconciliation | Observation/source-token counts exactly match approved station/day; no incompatible F/C or unit-family mixing; original header and target versions retained |
| Freshness | Initial Gold≤60 min/consumer≤75 min and onboarding≤120; daily Gold p95/p99≤40/60 min, consumer≤50/75 min; business99%≤90 min and04:00 UTC operating deadline |
| Questions / limits | “What values and quality did each station report?” “Which hours are absent in the source grid?” Grid gaps are observations of absence, not proven failed deliveries; no unverified timezone, DST or temperature/unit conversion |

GLBA_BARTLETT27 fields, GLBA_NUNATAK_QUEEN21 fields, KLGO_T10720 and KLGO_T10920remain distinct registered families. WindDir_D1_WVT's station-specific “miles/hour” versus “Deg” metadata discrepancy is retained. Direction values are not averaged as ordinary linear temperatures.

**AM-12 qualification gap:** original DAT-DQ-027plausibility and related cross-field review obligations retain their source definitions, but a numeric field dictionary/test oracle must be approved before those trusted Gold statistics are advertised. This is not a claim that all 40 identified rules were already executable or executed.

### G06 Open Prices observed-price distribution — physical openprices_daily

| Definition | Selected business contract |
|---|---|
| Domain / use / consumer | Observed market prices and observation coverage; price research analyst |
| Type / grain / identity | Aggregate; one(observation_date nullable, PRODUCT/CATEGORY type, exact product_code or category_tag, currency nullable, price_per context, source generation, reporting_cohort) |
| Entity/source truth | Latest **observed** representation per provider price.id chosen by actual ordered response observation clock and deterministic tie; complete captured representations remain in Silver |
| Dimensions | Provider type/product/category/currency/observation date and supported location/source context; provider product/location/proof IDs are nullable with coverage flags |
| Measures | Distinct latest observed price IDs; declared duplicate_of count; eligible non-duplicate-declared count; valid exact observed amount min/max/mean/median by homogeneous context; null currency/date/reference counts |
| Filtering / duplicate policy | Retain every price.id and duplicate_of relationship. Default comparable-price metrics exclude rows with nonnull duplicate_of, disclose that excluded count, and do not invent a canonical replacement for missing/cyclic relations |
| Currency / units | No FX or cross-currency total/comparison. Null currency is a quality group without a trusted price comparison. Zero price is allowed. Do not derive unit price from receipt_quantity without verified source semantics |
| Temporal / limitations | Observation date differs from created/updated/acquisition clocks; no fabricated date for null; latest observed does not mean provider current state is continuously complete |
| Refresh / revisions/delete | Another observation of same ID does not increment business ID count; replace previous contribution and repair old/new date/product/currency groups on observed changes. Absence/404 is evidence, not a source DELETE |
| Publication / DQ | Exact latest-entity population, duplicate/exclusion counts, lossless amount projection and currency partitions reconcile to bound Silver; partial bootstrap/history coverage explicitly labeled |
| Freshness | Eligible new tail records: B≤45 min, selected S≤60, G≤75, consumer≤90 under runtime budgets. Older-record revisions only weekly-sweep observation coverage, no native mutation SLA |
| Questions | “What observed prices exist for this product/currency/date?” “How much duplicate/reference/missing-context evidence is present?” Prices are not purchases, sales or realized revenue |

**gold.openprices_latest_observed** is the detailed governed entity view. Repeated overlap/sweep acquisitions remain original observation history but do not inflate price entity counts. The content fingerprint includes related-object payloads, not just amount. Historical export335,600and9,422duplicate_of counts are never hard-coded as the live product denominator.

### G07 Catalog completeness and quality — physical catalog_quality_daily

| Definition | Selected business contract |
|---|---|
| Domain / use / consumer | OFF/Amazon catalog stewardship; source-specific product coverage and quality trend |
| Type / grain / identity | Snapshot aggregate; one(source_domain, source_generation, published_catalog_epoch, as_of_date, metric_id, reporting_cohort) |
| Dimensions/source | OFF canonical/exception and Amazon current product views at explicit version set; domains are separate labels, not matched products |
| Measures | Original/source/canonical/exception counts, active/retired/deleted/fixture counts, code-identity conflicts, missing product_name/title/category and registered optional-field completeness |
| KPIs | Completeness numerator/denominator explicitly per metric. OFF name completeness uses eligible canonical active products; full-source exception rate uses original captured occurrences. Amazon review coverage uses exact parent keys with current-reference alignment |
| Filters / missing | OFF ambiguous/blank codes excluded from unambiguous catalog, retained in exception metrics; missing optional names do not delete a product. Amazon94blank titles and106,334null main_category originals remain quality observations |
| Semantics | As-observed source catalog history; no authoritative nutrition/business scoring for unverified OFF paths, no live Amazon inventory/pricing |
| Refresh / correction | After a reconciled FULL/contiguous DELTA, publish new catalog epoch, update affected metrics; fixed epoch retry is idempotent |
| Publication / DQ | Canonical+exceptions exactly account for source populations; no key case/zero loss, no fixture pollution; source-specific DQ gates and visible unresolved metrics |
| Freshness | FULL Gold≤36 h/consumer40 h target,48hbusiness deadline; incremental source-commit G/consumer targets as in§17 |
| Questions / limits | “What share of each catalog has approved identity/name/category?” “Which quality dimensions changed after the last DELTA?” No cross-source identity equivalence or unverified nested nutrition interpretation |

**gold.off_approved_products** is a view of current unambiguous eligible product codes with original open nested content and approved shallow projections. **gold.off_identity_exceptions** exposes the retained ambiguous/blank population to authorized stewards. No arbitrary latest-record winner resolves61conflicting codes.

### G08 Independent REES46 Kafka streaming activity — physical rees46_kafka_activity_daily

The complete grain/measure/late-data/quality/latency/replay contract is §10.5. Materialize this independently from G03: source-generation-scoped event-date/product/category/event-type/cohort groups, exact logical occurrences and nonblank source user/session distincts, separately labelled physical publication duplicates. It answers “What activity has this Kafka source observed?” and “Which stream groups changed after late arrivals?” It does not infer revenue/orders or combine the SFTP population.

Sole writer is the Kafka business task; inputs are frozen eligible silver.rees46_kafka_events versions. Changed old/new groups are replaced/reconciled with a same-table publication marker. Default consumer view is gold.rees46_kafka_streaming_activity; gold.rees46_sftp_batch_activity labels G03 independently. Native capture/consumer/recovery generations and completeness remain visible. Operational throughput/lag/state views are secondary outputs, not extra physical products.

### 15.2 Lineage and business publication inventory

| Product | Required committed dependencies | Publication scope / correction route |
|---|---|---|
| G01 | H&M purchase/current + complete transaction proof + bound customer/article versions | Purchase keys; article-pending explicit; source correction repairs key/history |
| G02 | G01published fact snapshot | Affected date/article/channel groups; reconcile count/amount |
| G03 SFTP + funnel | Approved SFTP month revisions + exact source ordinal coverage | Affected date/product and complete session/product groups; Kafka excluded |
| G04 + Amazon views | Reviews original occurrence Silver + published metadata version | Day/parent groups; current-reference join alignment disclosed |
| G05 | Approved Campbell station-day revisions + field/header dictionary | Station/date/measurement/unit; no incompatible mixing |
| G06 | API accepted observations + latest-observed selection | Source price.id's old/new business groups; no absence delete |
| G08 Kafka streaming | Independent Kafka Silver/source generation and exact producer/Bronze scope | Changed event-date/product/category/type groups; physical duplicates separate; no SFTP UNION |
| G07 + OFF views | Published source catalog epoch + original exception/current key state | Domain/epoch metric set; source totals and eligible denominators |

Consumer refresh/caching is separately measured. No dashboard has been implemented or proven available. Publication/query fixtures later validate the stated consumer questions and KPIs, including a late correction after a product was already published.

### 15.3 Gold refresh ownership, modes and consumer handoff

The business catalogue and eight physical products remain unchanged. The exclusive writer/trigger matrix in§9.7 is authoritative; a source workflow must not also write an ADF domain-owned product.

For G04 and G07, ADF checks eligible publication debt at one-minute active intervals and may request an immediate dependency dispatch through the same owner. The Job first freezes a valid input-version set or returns NOT_READY/NO_DUE_WORK without a full transformation. Unrelated domains do not wait for each other.

**FULL refresh:** first business publication, approved transformation/schema migration or a bounded rebuild after source CDF/version history is unavailable. Validate a candidate or bounded replacement scope before atomic product publication. Do not silently erase the old published product.

**Incremental refresh:** use committed changed-key/old/new input evidence to recompute affected groups. Inserts, updates, deletes and late corrections affect both former and new groups; empty groups are removed. A retry verifies the product's actual publication marker and returns its existing effect. Product input-version/publication metadata does not become another source acquisition watermark or offsets table.

Only publish when required Silver versions, exact source scope, DQ disposition and business reconciliation pass. If a product is stale despite fresh Bronze, record publication debt/incident independently of ingestion success. Missing Amazon metadata may defer enriched/coverage products while original review counts remain explicitly unenriched.

When a real reporting consumer is configured, ADF can invoke its allowlisted refresh endpoint after a verified product publication and record the actual refresh/query outcome. Until that consumer and credentials/licensing are present, keep the step NOT_CONFIGURED; neither a successful Gold Job nor a proposed cache interval proves dashboard freshness. No dashboard service is provisioned by this architecture correction.

## 16. Production success/failure scenario catalogue and safe demonstrations

**Every future test below is NOT EXECUTED in this phase.** Existing source calendars and source-specific test IDs in D01–D11 remain additional obligations; this risk catalogue does not replace them. It covers realistic selected risks, not every theoretically possible failure.

Each row specifies the twelve requested fields. “Hold” means no successful acceptance/business checkpoint for the affected scope; already durable evidence is retained. Operational alerts identify source/generation/scope/release/run and sanitized class, not raw personal data.

### 16.1 Source acquisition and publication

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A01 | All eight/FULL | Pinned release, systems ready | Independent seed/source/target count/hash/member checks | Complete genuine interface path | FULL marker only after exact scope | Stable original identities | Resume verified unit | FULL report, target UUID/version | Bounded FULL then scheduled full population | Exact expected accounted members; no bypass | P0 |
| A02 | All eight/increment | Prior baseline eligible | New source journal/day/window/offset work | Capture/apply exact new unit | Only actual completed work advances | Native operation/occurrence IDs | Resume same source unit | Per-source change report | One normal increment each integration | Expected new effects exactly once | P0 |
| A03 | OFF/AMZ/reviews/DAT/API | Contract-valid empty unit | Explicit empty flag, schema/header/envelope | Accept valid emptiness, not outage | Zero data+real completion marker | Same empty unit stable | Replay exact empty | Empty count/range receipt | Parquet0 row, DAT4headers, API0items | Valid empty passes; missing/incomplete fails | P0 |
| A04 | SFTP/revision | New correction to accepted scope | Supersedes/member/hash comparison | Validate fully before activation | Old raw/revision retained | New revision, same logical members | Reconcile then select revision | Revision lineage | November108/full replacement and station correction | Only corrected effect active; old evidence intact | P0 |
| A05 | Source/interfaces | Source unavailable | Connect/error/deadline and no readiness | Bounded retry/defer, incident | No fake empty/completion | Reuse logical scope | Catch up retained history | Outage/backlog age | Block endpoint during isolated window | No lost source members/false success | P1 |
| A06 | Five SFTP/manifest | READY references missing part | Exact named-part anti-join | Hold package, retain evidence | No RAW_VERIFIED/ACK | Same revision not accepted partially | Publish missing correct part/valid revision | Missing names/count | Remove one fixture part before READY | All-part receipt impossible until complete | P0 |
| A07 | SFTP/readiness | Premature/partial READY | Invalid control bytes/hash or .part/unclosed member | Reject readiness, never parse as complete | No capture marker | Fixed source revision guards | Finalize valid package | Readiness incident | Upload READY before closing fixture | No accepted partial package | P0 |
| A08 | SFTP/Raw | Byte flip/truncation after copy | Independent length/SHA/CRC | Reject attempt | Raw failure retained, no ACK | New transport attempt only | Recopy exact source | Expected/actual digest report | Corrupt isolated landed object | Mismatch blocks acceptance | P0 |
| A09 | SFTP/retry | Same delivery/revision repeated | Existing manifest/target marker | Return matching receipt | No duplicate business rows | Same immutable identity | Repair only missing audit/ACK | Duplicate attempt metric | Retransmit identical package | Physical attempts tracked; logical count unchanged | P0 |
| A10 | SFTP/continuity | Late/out-of-order/missing predecessor | Calendar/predecessor/range gap | Retain later evidence; hold contiguous publish | Affected chain pending | Identity not arrival order | Acquire missing predecessor | Late/missing/gap incident | D02March gap, AMZout-of-order, oneDATstation | No false contiguous high; other stations proceed | P0 |
| A11 | OFF/AMZ/publisher | Crash/rollback mid source transaction | Action receipt/journal/head consistency | Roll back or return committed receipt | State+journal+receipt all-or-none | Same action_id once | Resolve source receipt before retry | Source transaction proof | UPDATEtwice, INSERT→DELETE, rollback | Every committed op retained; rollback absent | P0 |
| A12 | Publisher/SFTP host | Upload interrupted/host or disk fails | Missing remote completion; disk budget | Stop admission, noREADY | Journal/seal durable; incomplete attempt | Same sealed membership | Restart host/read-back/reupload unused attempt | Disk/host/ACK debt | Kill publisher after part close/before READY | Correct source history and resumable publish | P1 |
| A13 | CSV/JSONL parser | Embedded quoting/newline/large record | Quote-aware boundary/ordinal oracle | Preserve logical record intact | No counter shift/partial commit | Original file+logical ordinal | Fix decoder, replay Raw | Parser allocation comparison | Quoted CSV multiline +105328Breview fixture | Exact ordinals/bytes/counts | P0 |

### 16.2 PostgreSQL and CDC

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| D01 | Articles/journal | Lower allocated seq commits after seal | Visible fixed-member comparison | Later commit stays eligible | ACK only sealed members | Source operation ID | Next seal captures late member | Seal/member report | Two transactions with reversed commit order | No skipped low sequence | P0 |
| D02 | H&M/import/amount | Scientific/high-scale value | NUMERIC/text/exact round-trip | Accept valid exponent, hold lossy cast | Lexeme/raw retained | Original purchase ID | Correct exact projection | Decimal mismatch evidence | All6exponents and>18scale fixtures | No rounding; exact value equality | P0 |
| D03 | H&M/snapshot | Import complete, snapshot→WAL | Connector snapshot state + baseline/LSN checks | Reconcile handoff before replay | Native snapshot/offset state | Snapshot generation+PK | Resume consistent snapshot or declared reseed | Snapshot report/JMX | Crash near snapshot completion | No lost/double original baseline | P0 |
| D04 | H&M/transaction | Rollback/repeated action/version conflict | Source action/FK/version guards | Reject/rollback or return receipt | No committed rollback effect | Same action once | Resolve source commit | Source+CDC absence proof | Rollback insert and repeated action | No duplicate/rolled-back business effect | P0 |
| D05 | H&M/CDC apply | Transaction split across topics/batches | END expected member counts/order | Wait for all members | Bronze advances independently; business pending | Native op identity | Re-evaluate retained Bronze | Pending txn/count report | Delay customer or purchase member | No partial transaction Gold publication | P0 |
| D06 | H&M/WAL | Connector outage/backlog near budget | Slot bytes/horizon50/75/85% | Alert; pause new mutation before pressure | Slot retained; no fake ACK | Native offsets | Resume/catch up; grow approved capacity | WAL/slot graphs | Bounded connector pause | Recoverable members exact; no silent slot drop | P0 |
| D07 | Connect/offset loss | Isolated internal offset unavailable | Connector/slot mismatch | Stop; declare recovery generation | No claim continuous CDC | Generation-scoped source IDs | Restore proven offsets or reconciled resnapshot | Connector incident | Fault isolated connector state only | Explicit continuity/gap accounting | P0 |
| D08 | PostgreSQL/restart | Source restart or managed restore | Slot identity/native source checks | Distinguish restart from slot-loss restore | Preserve baseline/history | Same generation only if continuity proven | Restore/validate; otherwise reseed | Slot/DB restore evidence | Restart then separate PITRdrill | No silent new slot/history reset | P0 |
| D09 | H&M/delete/tombstone | Approved fixture DELETE | Before/key/op/END validation | Apply explicit deletion; retain evidence | Raw tombstone not dropped; source checkpoint normal | Native delete op | Repair dependent fact/aggregate | Delete reconciliation | Delete fixture after Gold publication | Correct removed contribution/history | P0 |

### 16.3 API

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P01 | API/bootstrap | Old export available, new run | Planner input/source audit | REST floor/H0 only | Separate history/tail frontier | Deterministic work IDs | Same REST planner | Discovery/floor manifest | Mock stale export plus newer live rows | No export read as baseline; full bounded coverage | P0 |
| P02 | API/429/5xx | Legitimate mock response fault | Status/header/attempt counters | HonorRetry-After, bounded retry/breaker | Attempt consumed; frontier unchanged | Persisted attempt IDs | Deferred same work | Retrydeadline/breaker evidence | Seconds/date429 +5xx mocks | One request at a time, no early retry | P0 |
| P03 | API/deadline | Slow body, hung worker, lost result |70 s watchdog/incomplete status | Kill/join before new send; cooldown unknown | Raw partial not accepted | Reserved attempt remains consumed | New safe traversal/worker | Deadline/unknown outcome | Contract-faithful slow-body mock | No overlapping requests/false completed page | P0 |
| P04 | API/mutable pages | Total/order/ID movement | Envelope/order/ID check | Abandon/restart or split | No accepted inconsistent traversal | Fixed bounds/traversal IDs | Restartpage1within budget | Mutation validation report | Move/delete/add ID between pages | Inconsistency held; no snapshot claim | P0 |
| P05 | API/incomplete/overflow | Missing page/too-dense bucket | Membership/byte/time caps | Hold/split orCAPACITY_BLOCKED | No root frontier leap | Work/child membership fixed | Complete valid bounded children | Coverage/tree report | Omit page; overflow minbucket | No acceptance with unexamined records | P0 |
| P06 | API/acceptance gaps | Crash after acceptance/before receipt or after target commit | A/R/Banti-join | Republish fixed receipt or return target marker | Acquisition not rolled back | Observation IDs stable | Repair downstream only | Receipt/target debt | Named crashpoints at each boundary | One acceptance/effect; debt closes | P0 |
| P07 | API/poison receipt | Forged/unknown acceptance/hash | Strict receipt/ledger comparison | Fail batch, reviewed exact-path disposition | Native file checkpoint not falsely complete | Exact accepted identity | Stop/investigate/resume legitimate work | Integrity alert | Extra receipt with no ledger acceptance | No unauthorized item admitted | P0 |
| P08 | API/ledger contention | Two guarded writers or stale epoch | HEADrevision/hash/CAS | One complete command, loser none | No orphan acceptance events | Deterministic command ID/hash | Terminal-owner proof/new epoch | Delta conflict audit | Actual Delta concurrent-contender drill | Exactly1command;0loser domain inserts | P0 |

### 16.4 Kafka

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| K01 | REES/transaction | Abort after data before commit | read_committed and control snapshot | Abort all publication effects | No visible data/progress | Same committed source cursor | Retry source batch | Broker/control proof | Isolated atomic abort drill | Behavior+summary+progress all-or-none | P0 |
| K02 | REES/fencing | Two lane owners | Fenced error/native epoch | Old producer stops | Only current owner commits | Stable transactional.id | Recover committed control | Fatal code/owner evidence | Old owner sends thennew init | Old owner cannot commit | P0 |
| K03 | REES/late/duplicate | Verified1/14/3830/3831 | Actual callbacks+Bronze assertions | Preserve5 physical/4 logical | Native checkpoint normal | StableeventID/newattempt | Reconcile exactfixture | Offset/key/byte evidence | D11delay30and600isolated pairs | Expected order, delay, multiplicity | P0 |
| K04 | REES/Avro/DQ | UnknownID/badframe/payload/semantic | Inspectorstatus | Preserve full raw, classify | Nativecapture can complete with quarantine | Physicaloffset identity | Register approved decoder/replay isolated | Raw/status report | Fault bytes/actual test registry catalog | No raw record dropped | P0 |
| K05 | Kafka/broker outage | Connection/commit interrupted | Client retriable/fatal/uncertain | Retry same op or stop | No guessed progress | Same transaction/command resolution | Reconnect/fence/read_committed control | Brokerlag/commit metrics | Bounded outage during commit | No lost source ordinal/false commit | P0 |
| K06 | Kafka/partition skew | Hot valid user key | Partitionrecords/bytes/lag | Measure; bound backlog, preserve routing | No repartition/offset reset | Originalkey identity | Tune measured consumers/approved change | Per partition/skew metrics | Isolated skewed keyed fixture | Exactrouting/rows; visible lag and catchup | P1 |
| K07 | Kafka/retention | Required offsets expire | Earliest/required offset proof | failOnDataLoss, declare gap | Stop completedprogress claim | New physicalgeneration explicit | §18isolated recovery | Missing-range certificate | Isolated short-retention topics | No primary retention change; no silent skip | P0 |

### 16.5 Schema and business quality

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | Applicable/schema | Approved nullable addition | Exactversion/fingerprint | Use qualifiedold/newreader | Priorraw/decoder retained | Versionedidentity unchanged | Activate approved boundary | Schema compatibility report | REESApril, AMZv2, DATGNLA2, Kafka v2 | Old/newpayloads preserve fields | P0 |
| Q02 | JSON/Parquet/Avro | Breaking nested/type/schema99 | Structuraldiff/decodefailure | Rawpreserve; typedhold | No trusted completion | Same raw/operation identity | New approved serialization revision | Fieldpath incident | Typechange/nestedarray fault | No silentcast/flatten/drop | P0 |
| Q03 | All/DQ anomalies | Repeated rows/blanks/zero/sentinels | Rule-specificwarning counts | Preserve accepted historical anomaly | Captureaccounted, qualityvisible | Occurrenceidentity | Review only if contract requires | DQdenominators | Original anomaly fixtures | No multiplicity/lexeme loss | P0 |
| Q04 | OFF/Amazon/identity | Conflictcode/lowercase/zeros | Exactkey/case/exception anti-join | OFF excludes ambiguous; Amazonpreservecase | Raw all; canonical only eligible | Exact string identity | Resolve only by approved amendment | Key/exceptionreport |61OFFgroups, blank; lowercaseASIN |123exceptionsretained; no arbitrarywinner | P0 |
| Q05 | Silver/Gold join | Duplicate dimension/wrong join key | Outputcount/keymultiplicity | Blockpublication | Targetunchanged | Same input version scope | Fix join/recompute | Cardinality/unmatchedreport | DuplicateASINdim or asin-vs-parent_asin | No multiplied/dropped facts | P0 |
| Q06 | Gold/KPI | Wrongdenominator/currency/units | Independent metric oracle | Blockproduct | No successmarker | Fixed publication intent | Recompute affected groups | Metricdiffreport | Funnel, reviewrating, weather/FXfault | Businessformulas match§15 | P0 |

### 16.6 Actual Spark execution and performance failures

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| S01 | Spark/join skew | Isolated 90% hot-key fixture | Max/p50 task time, partition bytes | Demonstrate baseline; AQE/scoped fix | Source unchanged; no invalid publication | Stable input/result | Rerun optimized exact logic | Plans/task metrics | Bounded skew join | Equal result; measured skew change | P1 |
| S02 | Spark/shuffle spill | Sort/join beyond execution memory | Actual spill bytes/events | Allow spill; measure cost | No premature commit | Same deterministic data | Adjust partitions/compute if needed | Spill/task durations | Bounded large wide fixture | Spill>0and exact result; no OOM claim | P1 |
| S03 | Spark/executor loss | Remove one isolated worker mid-stage | Executor lost/task retry event | Native retry | Only complete Delta commit visible | Deterministic outputs | Retry/recompute | Event log+target version | Supported worker-loss drill | Exact final count, no duplicated effect | P0 |
| S04 | Spark/driver failure | Stop isolated driver at named boundary | Job failed/terminal compute | Preserve checkpoint/target evidence | Committed target may exist | Native epoch/target marker | Same checkpoint/commit inspection | Job/Delta/checkpoint logs | Crash before/after commit | No false checkpoint/no double commit | P0 |
| S05 | Spark/bounded OOM | Isolated low-memory/capped fixture | Actual OOM/error/heap metrics | Fail bounded job, stop pressure | No accepted partial unit | Stable target scope | Fix partition/memory, replay | OOM class+budget stop | Safe MemoryError then optional real OOM | Label simulation versus actual; correct recovery | P1 |
| S06 | Spark/shuffle fetch | Worker loss after map outputs | FetchFailed/stage retry | Native recomputation | Target atomic | Same logical result | Rerun lost stage | Fetch/retry events | Isolated shuffle loss | Recomputed exact result or clean failure | P1 |
| S07 | Spark/broadcast | Forced oversized broadcast in fixture | Broadcast error/driver pressure | Fail/fallback after diagnosis | No partial publish | Fixed join input | Sort-merge/narrow projection | Join plan/memory | Bounded broadcast fault | Equality after fix; no source damage | P1 |
| S08 | Spark/input imbalance | One gzip versus workers/sparse parts | Read tasks/utilization | Use bounded derived parts | Raw unchanged | Original ordinals | Rebuild work plan | Input partitions/executor tasks | Unsplittable versus derived comparison | Actual multiple worker tasks; exact members | P1 |
| S09 | Delta/small files | Many tiny bounded writes | File count/size/scan metrics | Bounded maintenance outside SLA | Business rows unchanged | Maintenance is not business change | Compact approved range | Delta/history metrics | Tiny file fixture/compaction | Same row multiset, better file profile | P2 |
| S10 | Delta/MERGE conflict | Two isolated writers same keys | ConcurrentModification/error | Reject/replan; no silent overwrite | One valid commit, loser uncertainty resolved | Expected version/operation IDs | Serialized retry from current target | Commit/conflictreport | Actual concurrent MERGE | Exactly expected versions/effects | P0 |
| S11 | Stream/state growth | Isolated unbounded-key operator | numRowsTotal/memory/lag | Stop bounded test; design correct state | Primary Bronze has no stateful dropping | Fixture identities | New compatible checkpoint/test scope | Streaming progress/store metrics | Bounded state-growth demo | Growth observed; source unchanged | P1 |
| S12 | Stream/watermark | Late historical record past wrong bound | Oracle late count/output difference | Reject wrong logic | No primary drop/approval | Original event ID | Batch repair from Bronze | Watermark/exclusion report | Isolated incorrect-watermark case | Loss detected; correct product restored | P0 |
| S13 | Delta/CDF retention | Consumer behind retained files | Version/file/CDF error | Stop incremental claim | No guessed CDF advance | Bounded snapshot/recovery generation | Reconcile/rebuild affected target | Retention incident | Isolated CDF expiry fixture | No missing updates/deletes hidden | P0 |

### 16.7 Orchestration, deployment and recovery

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| O01 | ADF/duplicate | Same event twice/overlap | Same intent/Job token/scope | Attach/return existing run | No second writer/effect | Deterministic token+target ID | Reconcile existing result | ADF/Job run links | Simultaneous duplicate pipeline | One logical capture | P0 |
| O02 | Schedulers/missed | Disabled/skipped tick | Due calendar/oldest debt | Next tick coalesces outstanding work | No time-based advance | Durable work identity | Catch up within bounds | Missed slot/backlog metric | Skip two ticks then resume | Exact coverage, visible SLA miss | P1 |
| O03 | ADF/uncertain child | Activity times out while Job active | Runs-get actual state | Reattach; no new writer | ORCHESTRATION_UNKNOWN | Same execution token | Await/cancel and prove terminal | ADF timeout+live Job | Response-loss mock/real run | One active Job; correct completion | P0 |
| O04 | Routing/release | Wrong source/path/target metadata | Allowlist/release hash | Fail before apply | Targets unchanged | Fixed intent/release | Correct metadata with approved release | Routing incident | Swap source parameter fixture | No cross-source write | P0 |
| O05 | Retry exhaustion | Repeated source/Job failure | Persisted attempt budget | Hold+incident | No successful completion | Same work/attempt lineage | Operator recovery after cause fixed | Final failed state | Exhaust bounded budget | No infinite loop/false success | P1 |
| O06 | Security/secret | Expired Kafka/SSH/DB/storage credential | Auth failure class | Stop retry abuse; rotate via approved secret | No frontier leap | Same work after rotation | Reconnect/resume safely | Sanitized auth alert | Isolated test credential rotation | No secret log; exact continuation | P0 |
| O07 | UC/ADLS permission | Required grant denied | Authorization exception | Fail visibly | No ACK/acceptance of unreadable data | Target marker check | Restore grant, rerun | Access/Job audit | Revoke isolated target permission | Correct failed state/recovery | P0 |
| O08 | Dependency/partial layer | Silver fails or Gold failsafter commit | Actual marker/version mismatch | Stop dependent stage; retain debt | Upstream committed, not rolled back | Stage-specific IDs | Repair failed stage only | Debt/DQ report | Fail taskafter Bronze/Silver | No early Gold/source duplication | P0 |
| O09 | CI/CD/rollback | Incompatible release/checkpoint | Release/schema/state gate | Keep Jobs paused; refuse unsafe rollback | Old state preserved | Immutable artifact version | Compatible rollback/forward migration | Deployment record | Rejected release fixture | No mixed release/invalid checkpoint reuse | P0 |
| O10 | Cost/capacity | Autoscale/spend/storage unexpected | Budget/node hours/disk metrics | Pause admission/controlled drain | Retain recovery evidence | No deletion for budget | Approve bounded cost or stop window | Cost alert/stop record | Lower isolated budget threshold | Alert+drain; retention protected | P1 |
| R01 | Gold/late correction | Product already published | Old/new group reconciliation | Rebuild both groups, atomic product update | New product marker after pass | Same correction once | Recompute from bound Silver | Before/after KPI report | Move fixture date/category/amount | Old contribution removed, new correct | P0 |
| R02 | Business/delete/SCD | Explicit delete/late version | Native op/order/history check | Close/rebuild history; repair facts | No absence-derived delete | Source version/op identity | Key-scoped history rebuild | Interval/aggregate proof | Late SCD version+fixture delete | No overlap/double count/stale Gold | P0 |
| R03 | Backfill | Bounded historical request | Scope/release/source coverage | Exclusive approved scope | Old evidence kept | Same original identities | Replay Raw/source interface as required | Backfill report | Repeat bounded historical range | Same business result twice | P0 |
| R04 | Delta/ACK loss | Commit completed, response/audit lost | Target marker/version lookup | Repair audit/ACK only | Completed target not reapplied | Deterministic completion ID | Resolved writer/receipt | Commit/audit comparison | Kill after commit before ACK | One effect, correct ACK | P0 |
| R05 | Checkpoint loss | Native checkpoint damaged | Missing/inconsistent files | Pause; restore or isolated replay | No earliest append into old target | Original IDs/new physical generation | §18certificate cutover | Recovery certificate | Isolated checkpoint damage | Exact candidate; old target retained | P0 |
| R06 | Retention/history loss | Required Kafka/WAL/source expired | Exact missing ranges/member proof | Declare gap; no hidden reset | Completeness not claimed | New generation boundary | Source reseed/rebuild if possible | Gap/RPO/RTO report | Isolated expiry/slot case | Missing history explicit, not fabricated | P0 |
| R07 | Recovery cutover | Verified replacement candidate | Actual paused Jobs/terminated compute, frozen UUID/version | Exclusive atomic view replacement | One canonical target | No UNION duplicates | Rebind readers/checkpoints | Cutover certificate | Auto-restart/manual race fixture | Stale target/refused race; safe view | P0 |
| R08 | Source journal durability | Host/disk restore/seal/ACK uncertain | Receipt/head/backup compare | Fence owner, resolve history | No second journal authority | Source action receipt | Restore consistent journal/seals | Source backup/ACK debt | Restart from durable journal | Same committed operation sequence | P0 |
| R09 | Clock/DNS/network | Clock skew or endpoint misresolution | UTC clock check/TLS/DNS/source binding | Hold bad clock/endpoint | No false freshness/foreign source | Pinned source identities | Correct clock/routes, then resume | Clock/network incident | Bounded time/DNS fault | No fabricated UTC/SLA or wrong source | P0 |

### 16.8 Safe fault-injection architecture

Future demonstrations use an explicit fixture/recovery namespace **inside the same DEV environment**, new temporary target/checkpoint scopes and cost/time caps. They read copied fixtures or immutable source samples. No original local file, authoritative seed, primary Kafka retention, source journal history or live checkpoint is deleted/overwritten to demonstrate failure.

- **Skew/spill:** bounded synthetic DataFrames with declared key/cardinality distribution; capture actual Spark plans/task metrics. Skew fixture cannot accidentally create an unbounded many-to-many result.
- **Executor/shuffle loss:** isolated cluster only; use a supported worker-loss/restart mechanism during a known stage. Require actual executor-loss/FetchFailed evidence, not merely a raised Python exception labeled executor loss.
- **Driver/native checkpoint:** named before/after-commit crashpoints; prove actual Delta epochNcommit versus missing completed checkpointN. If that boundary cannot be observed/injected, mark NOT_PROVED.
- **OOM:** first a bounded injected MemoryError to prove error paths; label it simulated. A real OOM is optional on isolated low-memory compute with capped input, two-node/resource policy, short timeout and operator stop. Never run it on the source host or normal stream cluster.
- **Wrong join/MERGE:** small explicit expected-result fixture; duplicate a dimension key or race two isolated target writers. Exact multiset/operation-version comparison is the oracle.
- **State/watermark:** separate synthetic stateful query, bounded duration/key count/checkpoint. Primary Bronze remains occurrence-preserving and stateless.
- **Kafka lag/retention:** isolated .test. behavior/control pair. Pausing a consumer demonstrates backlog; actual segment expiry must be observed for a retention test. DeleteRecords truncation is a different test and cannot substitute for proven TTL expiry.
- **API failures:** provider-contract-faithful mock, durable ledger/storage modules later, named crashpoints. Legitimate provider conformance is read-only, at most10requests per approved execution through the same one-request budget; never provoke429/load-test the provider.
- **ADF uncertainty:** drop/delay the orchestration response while keeping the child Job active; verify same run token and target effect.
- **Gold correction:** publish a known baseline, deliver one late changed/deleted source unit, independently compute expected old/new contributions and query the corrected product.

Each run records fixture seed/release, fault name/location/time, expected outcome, scope, actual Spark/ADF/connector evidence, target UUID/versions, recovery result, measured cost and cleanup decision. Cleanup is reviewed and respects retained incident evidence; no blanket recursive deletion.

### 16.9 A4 Auto Loader and orchestration qualification additions

These ten scenarios extend the retained 75-scenario risk catalogue. They are **NOT EXECUTED**. Use the same isolated DEV fixture/target rules and twelve-field evidence contract.

| ID | Source/stage | Trigger/precondition | Detection | Expected handling | Data/checkpoint state | Idempotency guarantee | Recovery | Alert/evidence | Future test | Pass/fail acceptance | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| F01 | All five SFTP/native landing | Incomplete original part or accepted publication | Manifest/size/SHA/ACCEPTED_READY check | Hold query/ACK; repair winning native set | No eligible source scope/checkpoint | Fixed source revision/paths | Resume after owner quiescence | Source/Raw/native-path report | Partial accepted copy plus unauthorized accepted-path fixture | No incomplete input enters Bronze; no ACK | P0 |
| F02 | SFTP native batches | First native append batch commits; later batch fails | Native failure/target scope versus frozen source manifest | Retain partial Bronze/same checkpoint | No complete-delivery/Silver/ACK | Native Delta sink/query identity | Same source query/finalizer | Native query/Delta/source certificate | Crash after one native batch | Exact final source multiplicity once; package held until complete | P0 |
| F03 | SFTP target/checkpoint | Delta append durable, acknowledgment lost | Actual native query/target commit evidence | Resolve old query quiescence; native restart | No guessed native file advancement | Native sink/checkpoint semantics | Same C/S; isolate if state lost | Delta/native log/source comparison | Commit-before-checkpoint crash | No manual append/MERGE retry; no duplicate logical effects after recovery | P0 |
| F04 | SFTP finalizer | Query succeeds, reconciliation/control commit fails | Native completion; delivery certificate/state missing | Rerun finalizer even with zero new files | No complete delivery/ACK/business readiness | Deterministic certificate/scope | Repair from exact frozen target | Native/file/target/source evidence | Fail after native query completion | No recopy/reapply; exact control/ACK repair | P0 |
| F05 | SFTP empty source | Valid zero-row native Parquet/JSONL/CSV or4header DAT | Expected-zero source scope + EOF/native evidence or explicit zero-byte-not-ingested disposition | Complete logical delivery without marker rows | Zero source DATA; real native/control completion | Same empty source identity/revision | Same owner/finalizer | Header/footer/part/zero-scope report | One applicable empty file each adapter | No fabricated row; empty and missing distinguished; ACK correct | P0 |
| F06 | SFTP checkpoint loss | Missing/corrupt native checkpoint/schema state | Native error and frozen source/target audit | Stop owner; coherent restore/isolated candidate | No blind append/reset/offset repair | Original source identity/path/revision | Exact isolated replay/canonical cutover | Checkpoint/target/certificate generations | Lose C/S after committed files | No missing/duplicate business effects or unsafe same-target replay | P0 |
| F07 | SFTP schema/identity | Wrong SP-01ordinal/version/header/native shape | Source identity + native schema/rescue/corrupt comparison | Hold affected source delivery, preserve Raw | No false complete/ACK/trusted use | Publisher seed+ordinal or source operation identity | Fix registered source revision/native parser and replay | Source/native schema/mapping certificate | Repeated review/CSV ordinal fault, TOA5family change | Native cloudFiles only; no SDK fallback or invented Spark ordinal | P0 |
| F08 | H&M onboarding/cutover | Customer/purchase baseline missing members or snapshot/WAL handoff uncertain | Source totals, slot/connector snapshot state and target transaction proof | Keep source replay paused; hold cutover | No claim of complete initial CDC | Native baseline generation/PK and operation IDs | Resume native snapshot or declared recovery | ADF/source/Connect/Bronze handoff record | Bounded baseline interruption at handoff | No second uncoordinated Copy baseline, lost or double source occurrence | P0 |
| F09 | API scheduling | Both old native cron and ADF schedule enabled, or dispatch response lost | Schedule binding/running Job/token checks | Reject deployment or attach existing run; no overlapping requester | Ledger remains sole frontier; receipt path independent | Same execution token/command identity | Disable competing schedule; resolve actual owner | ADF/Jobs/ledger correlation | Dual-scheduler configuration fixture and lost run-now response | One scheduling owner/active acquisition; accepted receipts still drain during failure | P0 |
| F10 | Shared Gold/ADF | Metadata and reviews complete in different order; next tick/backfill overlaps | Product owner, frozen dependencies and actual publication marker | Admit only ready scope; defer/attach competing request | Old Gold retained or clearly pending; no false freshness | One writer/product intent; affected groups recomputed | Existing product Job repair; late-input dispatch | Input-version/debt/consumer evidence | Stale G04/G07, duplicate dispatch and late correction | No double write/count, unrelated domains proceed, product SLA miss visible | P0 |

### 16.10 SCD2 and historical fact qualification

All C01–C20 are NOT EXECUTED. Future isolated fixture chooses the first two valid sorted article/customer keys from the **verified selected source-generation baseline**, freezes their exact IDs/images in the fixture manifest and applies approved source operations through real PostgreSQL/journal/Debezium. T0 is actual observed baseline; T1/T2/T3 are explicit future simulation instants, and backdating uses AM-13 only. Oracle is an independently enumerated expected key/state/interval/fact set, not a copy of the MERGE implementation.

| ID | Source/stage | Trigger / fixture | Detection | Expected handling | Data/checkpoint state | Idempotency | Recovery | Evidence | Future test | Exact pass criteria | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| C01 | SCD2baseline | Initial consistent source snapshot | Freeze observed T0 and baseline keyset | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Baseline starts atT0; no earlier invented versions | P0 |
| C02 | Tracked update | Article detail_desc/customer age changed atT1 | Expected complete source before/after and new source version | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Old interval endsT1; new stableSK/current state; unchanged keys identical | P0 |
| C03 | Unchanged update | Identical tracked payload, distinct operation | Compare tracked/null-aware state and operation accounting | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | No new business interval; source operation retained/audited | P0 |
| C04 | Nontracked update | Operational/audit field only changed | Registered tracked-set excludes it | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Logical history unchanged; source position/audit advances | P0 |
| C05 | Multiple same-key/equal-time | Two material updates at sameT2/native ordered versions | Compare all operations/order, zero-width flag and final scalar interval | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Both source states retained; last atT2 serves as-of; no invented microseconds | P0 |
| C06 | Multiple keys/batch | Interleaved material changes for two keys in one transaction | Exact member set and per-key reconstruction | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | One complete eligible transaction effect; no arbitrary last-row loss | P0 |
| C07 | NULLtransitions | Nullable tracked age/detail_desc NULL→value→NULL | Canonical null/presence hash and expected intervals | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Two correct material transitions; no NULL/empty-string collision | P0 |
| C08 | Late arrival | Earlier native committed version arrives after later capture | Missing predecessor/complete transaction gating | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Conformance waits/rebuilds deterministic source order, never arrival order | P0 |
| C09 | Backdated business state | Approved AM-13 receipt places complete state betweenT1/T3 | Validate receipt/action membership and bounded effective point | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Affected interval rebuilt; later effective state preserved; prebaseline backdate held | P0 |
| C10 | Delete/reinstate | Approved source key DELETE then reactivation | CDC d versus tombstone and journal/source images | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | DELETED interval explicit, active view excludes it; reinstatement opens correct interval | P0 |
| C11 | Duplicate CDC | Same operation identity twice physically | Compare identity and full payload; conflict branch too | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Identical op contributes once; conflicting same identity holds | P0 |
| C12 | Snapshot/WALoverlap | Same source state/version observed in baseline/read+CDC | Native handoff/position proof and before/after | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | One logical baseline transition; no lost/double source member | P0 |
| C13 | Failed MERGE | Fail before atomic target commit | Actual target version/content versus expected action set | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Target old snapshot intact; same-owner retry yields complete reconstructed history | P0 |
| C14 | Uncertain acknowledgment | Commit succeeds, response lost | Quiesce writer then exact UUID/version/source-version/content comparison | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Return matching existing effect or safe repair; never expire+insert twice | P0 |
| C15 | Concurrent writer | Second execution attempts same history scope | Job owner/revision/conflict/actual run evidence | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Reject/defer contender; no duplicate current row or stale action apply | P0 |
| C16 | Interval integrity fault | Isolated candidate has overlap/gap/two terminal rows | Distributed key/window/invariant oracle | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | FAIL and no product release; repair matches expected observed lifecycle | P0 |
| C17 | Historical as-of | Simulated factT2 between trusted effective boundaries; original date-only/prebaseline facts too | Bound dimension versions/time-basis/uniqueness | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | One correct simulated SK; unsupported history stays explicit unresolved; no current-as-historic claim | P0 |
| C18 | Late fact/dimension/Gold | Fact first, dimension late then interval correction | Pending/old-new SK and aggregate dependency comparisons | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Fact never dropped; SK and both old/new groups repaired exactly | P0 |
| C19 | Schema/tracked-set migration | New nullable tracked field/rename/type; historic value unavailable | Pinned old/new decoder/policy and affected-key comparison | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Unknown old value retained; explicit migration/rebind; breaking release blocked until approved | P0 |
| C20 | Replay/performance | Same frozen changed-key operation set twice on 2workers; optional4 worker comparison only after measured bottleneck/approval | Logical-history digest plus exact keys/content/invariants and Spark plans/tasks/spill | Freeze eligible operation scope; enforce §14.6/14.7 | Bronze preserved; affected Silver/Gold held until reconciled | Stable source op/version and tracked policy | Same owning Job; changed-key rebuild after quiescence | Frozen source receipt/target UUID/version, DQ and exact reconciliation | Isolated real source fixture; bound affected keys | Identical logical history/Gold on replay; actual multi-worker measurements reported without invented speedup | P0 |

### 16.11 Evolution, financial and release qualification

All E01–E12/B01 are NOT EXECUTED. Use contract-faithful isolated native source fixtures for faults; legitimate provider read-only checks establish REST conformance, never provider fault/load injection.

| ID | Source/stage | Trigger / fixture | Detection | Expected handling | Data/checkpoint state | Idempotency | Recovery | Evidence | Future test | Exact pass criteria | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E01 | Nullable addition | Registered compatible nullable field plus unregistered variant | Old/new writer-reader + physical schema/presence | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Approved field survives; unregistered protected addition held; no fabricated historic value | P0 |
| E02 | Required/removal/null/default | Required field absent, removed consumed field or new default | Complete affected native data and target constraints | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Hold incompatible contract; no silent fill/drop; source records accounted | P0 |
| E03 | Rename/reorder/case/duplicate | CSVheader reorder; Parquetcase rename; duplicate names | Exact native schema/header/case-sensitive key comparison | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Registered name-based reorder only; positional/ambiguous change held | P0 |
| E04 | Reintroduction/tracked-set | Name reused with changed meaning or new tracked attr | Old/new semantic epoch/history oracle | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Explicit version/migration; old absence not overwritten | P0 |
| E05 | Types/decimal | Safe widen and narrowing/string-numeric/precision-loss variants | Full lossless round-trip/overflow/lexeme comparison | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Only approved exact widen; H&M precision preserved; lossy target held | P0 |
| E06 | JSONnull/presence | Absent/null/string/object variants for protected field | Strict raw parse/presence/type flags | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | OFFopen extras retained; protected drift held/quarantined without raw loss | P0 |
| E07 | Semantic/key/grain | Currency/unit/timestamp/event/identity meaning change | Semantic contract and source-identity/fact oracle | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | New epoch/MAJOR review; no mixed-unit aggregate or re-key dedup | P0 |
| E08 | Nested structures | Array/map/object/null element addition/removal/type/rename | Native nested descriptor and exact cardinality/content | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | No flatten/list-to-struct corruption, multiplied occurrences or ignored drift | P0 |
| E09 | CSV/DATdialect | Quote/encoding/header/unit/logger family altered | Quote-aware original ordinal + exact4headers/schema family | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Correct registered v2 only; station/source hold otherwise | P0 |
| E10 | Parquetnative | FULL/DELTA native files with changed nesting/codec/footer | Actual native Auto Loader physical schema/page/hash checks | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | No conversion; compatible codec accepted; breaking native schema held | P0 |
| E11 | PG/RESTprotocol | Database DDL/CDC envelope or REST envelope/paging change | Native transaction/response membership and documented request contract | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Raw preserved; affected apply/acceptance held; no invented CDC/update filter | P0 |
| E12 | Avro/Delta/protocol | Actual Registry v1/v2/unknown ID and incompatible target feature/checkpoint | FULL_TRANSITIVE/old-reader tests and real Delta/client/runtime compatibility | Apply §14.1 classify/register/hold/migrate rules | Original source evidence/native progress preserved; typed scope held as required | Pinned source/schema/decoder/target release | Approved migration + exact replay/cutover | Native descriptor/fingerprint, target version and independent comparison | Successful and breaking isolated variants | Unknown Kafka bytes retained; compatible release works; unsafe writer/rollback blocked | P0 |
| B01 | Funding/quota/cost | Expired/insufficient credit, Premium/quota denial, unexpected meter or unfunded source VM/disk/API residual obligations | Read-only account/quote plus actual usage/retained-liability comparison | Block paid activation/new work; preserve evidence and stop authorized compute safely | No source/native state deletion to save cost | One approved budget/session/release | Smaller PlanB or separately approved funding; no subscription upgrade | Offer/balance/expiry/eligibility/quotes/actual usage | Planning fixture first; real read-only verification before paid run | No paid service starts without full funding/permissions; actual spend+retained liability within approved envelope | P0 |

A4.1 subtotal: the retained85 A4 scenarios plus20SCD2 +12evolution +1financial =118future cases. A4.2 retains/reconciles them and adds32cases below, for the current **150future architecture scenarios, all NOT EXECUTED**. Source-contract fixture IDs and all 275 identified source rules remain additional obligations, not replaced by this risk catalogue.

### 16.12 A4.2 native Auto Loader qualification (AL01–AL20)

All are **NOT EXECUTED**. Future fixtures use the real source publisher/SFTP/ADF/native cloudFiles path under an isolated demonstration source generation where needed. Temporary managed Delta targets/checkpoints/schema roots do not change the normal33table inventory; authoritative inputs/C/S are never destructively switched/reset. Native vendor behavior is §7.8; business compatibility remains §14. SCHEMA-LAB-01/v1.0.0 registers the explicitly simulated sim_counter/sim_review_context fixture additions only in isolated source generations; these are not observed original fields or unapproved changes to the original Parquet shards.

| ID | Source/stage | Trigger / fixture | Detection | Expected handling | Data/checkpoint state | Idempotency | Recovery | Evidence | Future test | Exact pass criteria | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| AL01 | Native initial inference | Publisher initial registered CSV/JSON/Parquet delivery via actual SFTP/ADF | Inspect inferred S/_schemas and native read schema/target | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Only expected source fields/types/hints appear; original identities/counts preserved | P0 |
| AL02 | Nullable addition | REES payloadv2 sim_export_quality or registered JSON envelope field | UnknownField failure, persisted schema update, compatible target change | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Restart same checkpoint/schema location; old rows null, new values retained, no duplicates | P0 |
| AL03 | Required addition | Declared required source field absent in old/new records | Source DQ/explicit contract versus nullable physical schema | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Physical Bronze capture cannot falsely pass required-field Silver/ACK/business gate | P0 |
| AL04 | Nested JSON addition | Publisher emits registered sim_review_context.note nested nullable field | Inferred nested schema and old/new raw/rescued evidence | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Supported typed nested evolution preserved; OFF VARIANT interior not falsely counted as schema mutation | P0 |
| AL05 | Nested incompatible type | Nested registered text field becomes object/array | Rescue/corrupt distinctions and compatibility classifier | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Raw/identity accounted; protected Silver use held, no silent cast/drop | P0 |
| AL06 | Parquet widening | Isolated native DELTA fixture sim_counter INT32→INT64; original shards untouched | Widening mode/S update/failure/runtime/Delta feature and exact values | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | DBR16.4+compatible target; restart same C/S, value2147483648 exact | P0 |
| AL07 | Parquet incompatible change | Fixture integer→string or incompatible nested shape | Unsupported conversion/rescue or qualified native failure | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | No forced schema/rounding; affected business contract rejected/held | P0 |
| AL08 | CSV reorder | Same registered field names reordered in a native source fixture | Native header-name parsing versus source positional/header contract | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Values align correctly where native support applies; unapproved header change still held by contract | P0 |
| AL09 | Unexpected field | Publisher unregistered envelope/CSV column to isolated scope | Compare rescue/fail/addNew modes and exact Raw field | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | No silent loss masquerading as success; main protected release fails closed | P0 |
| AL10 | Rename/drop/case | Rename/drop/case-only field and duplicate-name variants | Persisted S, source descriptor, rescued fields and target consumers | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | No inferred rename or fabricated old values; explicit migration/hold | P0 |
| AL11 | Malformed/missing | Truncated CSV/JSON, bad typed token, missing required identity | _corrupt_record versus _rescued_data plus Raw/expected membership | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Every source observation accounted; malformed body/identity blocks successful scope; no DROPMALFORMED | P0 |
| AL12 | Registered v1→v2 | Approved SP-01transport/payload schema combination versus wrong declared version | Manifest/schema/release and actual native parse comparison | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Only the registered version routes; original v1/v2 payload semantics remain separate | P0 |
| AL13 | Schema-stop | Crash/failure after S update before successful target write | Actual schema directory/checkpoint/native target evidence | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | No data checkpoint invented; same query safely resumes after registered migration | P0 |
| AL14 | Persistent restart | Stop/restart with all prior C/S state and newly eligible files | Native completed-file/query evidence plus exact target | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Old files not duplicated; new rows admitted; no checkpoint deletion/reset | P0 |
| AL15 | Bronze Delta evolution | Approved nullable target field or isolated widening feature | Actual Delta schema/protocol/reader compatibility | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Only scoped mergeSchema/feature enabled; target/readers and values reconcile | P0 |
| AL16 | Bronze accepted/Silver rejected | New physical field has unapproved business unit/required meaning | Native Bronze success versus source/Silver DQ/readiness | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Preserve Bronze; Silver/Gold publication remains held with explicit debt | P0 |
| AL17 | Gold consumer | Changed Silver field/meaning invalidates G03/G04/G07 projection | Bound schema/version/KPI denominator/reconciliation | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Old published product retained/labelled; no unsupported silent refresh | P0 |
| AL18 | Migration recovery | Checkpoint/schema loss or incompatible reader after target change | Coherent restore/candidate certificate and old/new protocol compatibility | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Isolated replay/canonical cutover or compatible restore; no blind append into old target | P0 |
| AL19 | Mode matrix | Same publisher fixtures on isolated addNewColumns/widen/rescue/fail/none targets | Native S updates/errors/rescued/ignored fields and Delta outcomes | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | All supported modes match §7.8; explicit-schema restrictions and none-with/without-rescue proven | P0 |
| AL20 | TOA5 fixed text | GNLA2v2/new logger field/unit in bounded native DAT parts | Exact4headers, wholeText ordered positions, source map/family parser | Apply native mode and registered source/business gate | Raw retained; native C/S/target and source_deliveries outcomes explicit | Source identity/path/revision + native sink | Same C/S compatible restart or isolated replay | Source manifest/native schema/checkpoint/Delta/certificate | Authorized native file/scope experiment | Text value schema fixed; proper registered field/NTZ/unit handling; no SDK file reading or fake Auto Loader field evolution | P0 |

### 16.13 Kafka downstream/temporary-resource and closeout qualification

All KS01–KS10/CL01–CL02 are **NOT EXECUTED**. Existing RF3/replica-failover/enterprise availability cases remain in the catalogue as enterprise-only, NOT_APPLICABLE_DEV on a single broker; they are not silently waived or marked passed.

| ID | Source/stage | Trigger / fixture | Detection | Expected handling | Data/checkpoint state | Idempotency | Recovery | Evidence | Future test | Exact pass criteria | Priority |
|---|---|---|---|---|---|---|---|---|---|---|---|
| KS01 | Independent B/S/G | Real private broker/producer/native capture and business tasks | Exact frozen source/Raw Kafka/B/S/G identities/lineage | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | D11 append unchanged; Kafka S/G independent; no SFTP/Auto Loader event detour | P0 |
| KS02 | Duplicate versus repeat | D11 verified1/14/3830/3831 plus one 3830republish | Actual physical offsets plus source-ID/payload/multiplicity comparison | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | 5 physical Bronze/4 logical Silver; G08sumlogical4/physical5/duplicate1;3830/3831remain distinct | P0 |
| KS03 | Late/out-of-order | D11 late30s/600s publication fixture and reversed arrival | Actual source/broker/event clocks and historical group oracle | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | All valid logical events eventually in Silver/G08correct date; no arrival-order loss | P0 |
| KS04 | Watermark isolated | 5 minute event windows,10 minute watermark with before/after-boundary records | Native state/watermark/output and authoritative no-drop population | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Demonstrate permitted window drop separately; G08late correction still complete from Silver | P0 |
| KS05 | Avro/version/DQ | Actual registered v1/v2, unknown ID, invalid bytes/semantic record | Raw inspector/registered decoder and source-quality checks | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Bronze always preserves physical raw record; eligible decoded rows/accounting correct; no invented ID | P0 |
| KS06 | Stage uncertainty | Silver MERGE succeeds, Gold fails or response lost | Actual UUID/version/source scope/native business checkpoint | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Same-owner retry/repair no duplicate logical or additive group effect; checkpoint only after required stages | P0 |
| KS07 | Business checkpoint loss | Lose isolated business state after durable S/G | Restore versus candidate replay from retained canonical Bronze | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Exact same logical events/groups/distinct sets; safe sole-view cutover, no UNION overlap | P0 |
| KS08 | Broker/producer process restart | Abort/commit/fence/restart on same persistent single broker | read_committed, actual source progress/cluster/topics/Registry | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Atomic/fencing semantics and exact resume within qualified DEV fault model; not a replica-HA pass | P0 |
| KS09 | Native backlog/cost | Two-worker query with six partitions and bounded representative load | Actual executor tasks/lag/backpressure/commit/CPU/RSS/disk/session meter | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Record actual results; no one-worker-only or unmeasured throughput/cost claim; preserve60×schedule semantics/debt | P0 |
| KS10 | Product independence/replay | Aligned seed subset ingested via both SFTP and Kafka, then Kafka replay | Separate source generations/lineage/publication/oracles | Preserve D11 + KSG-01/native/query boundaries | Physical Bronze/source progress/native checkpoints retained | Stable source ID and deterministic stage/group action | Same owner or certified isolated recovery | Actual broker/Registry/offset/B-S-G versions and exact comparisons | Bounded real source/stream fixture | Two separate correct products; no combined distinct population/double additive replay | P0 |
| CL01 | Retention/closeout | Attempt cleanup with pending SFTP debt, active checkpoint/H&M broker use or protected API evidence | Dependency/retention/control/lock inventory | Refuse premature deletion; no automatic TTL | Active data/state retained | Declared exact resource/generation scope | Finish debt/tests and obtain explicit closeout approval | Final evidence/retention-deviation/approval record | Planning negative fixture then separately authorized real closeout | No required data removed; API D10separate; no false365day or replay guarantee after cleanup | P0 |
| CL02 | Shared host/resource limit | Broker co-runs with heavy publisher or disk/RSS/CPU pressure | Actual process/disk/lag/admission metrics | Stop competing work/new admission; preserve state | No unsafe log truncation/VM reformat | Stable cluster/Registry/source identity | Drain/restart same disk or separately approved resize | Active and residual cost/configuration report | Bounded safe contention/process/VMrestart fixture | Shared host adequate at measured rate or holds explicitly; no HA/peak claim; cleanup avoids another source's disk | P0 |

A4.2 retains/reconciles all 150prior future scenarios and adds20 native Auto Loader +10Kafka downstream +2closeout/resource cases = **150 future architecture scenarios, all NOT EXECUTED**. D01–D11 source rules/fixtures remain their own obligations; this catalogue does not invent extra original source DQ IDs.

## 17. Freshness, business SLAs and schedule feasibility

All times below are **design objectives**, not achieved results. R/B/S/G mean verified Raw, Bronze, Silver and Gold. Consumer time includes publication and cache/refresh. Operational clocks use actual source COMMIT acknowledgment, READY publication/verified source clock, provider created time or Kafka append time as specified—not historical event dates.

### 17.1 Complete source/consumer matrix

| Integration / clock | Source publication / discovery | R | B | S | G | Consumer / business objective | Monitor owner / feasibility |
|---|---|---|---|---|---|---|---|
| H&M articles, source COMMIT ACK | Journal oncommit; ADF15 min | No separate fileRaw | p99≤30 min, p95≤20 | p99≤40 min | p99≤50 min | p99≤60 min;99%within 60; cache≤5 min | DE+source owner; at risk if startup/queues consume remaining15 min |
| H&M customers/purchases, COMMIT ACK | Debeziumcontinuous; native 10 s capture /60 s conformance | Raw Kafka retained inBronze | p95≤60 s, p99≤5 min | p99≤8 min | p99≤12 min | p99≤15 min;99%within 15; cache≤2 min | Connector/DE; steady-state only after baseline; article enrichment split underAM-11 |
| REES46 SFTP, READY | Monthly next1st02:00 UTC; ADF10 min |≤60 min|≤120 min|≤180 min|≤240 min|≤270 min; previous-month analytics08:00 UTC;100%scheduled periods measured; cache≤15 min | Supplier+DE; large bytes/parse at risk; intentional late/missing still recorded |
| OFF FULL, READY | One FULL; activeADF1 min |≤12 h|≤24 h|≤30 h|≤36 h|≤40hinternal;≤48hbusiness | DE+catalog steward; serial gzip phase and83.8 GB work qualify first |
| OFF DELTA, source commit exceptRaw | Hourly; publish≤10 min afterclose; ADF1 min | READYp95≤5 min | p95/p99≤110/150 min |≤135/180 min|≤160/210 min|≤175/225 min;99%≤240 min; cache≤10 min | Supplier+DE; source sealing/ready clocks included; warm small-file path required |
| Amazon metadata FULL, READY | One FULL10shards; ADF1minactive |≤12 h|≤24 h|≤30 h|≤36 h|≤40hinternal;≤48hbusiness | DE; native row-group parallelism, not proven duration |
| Amazon metadata DELTA, source commit exceptRaw | HourlysealHH:30→followingHH:00; ADF1 min | READYp95≤5 min |≤110/150 min|≤135/175 min|≤160/205 min|≤180/225 min;99%≤240 min; cache≤10 min | Supplier+DE; worst normal source seal/publish wait can approach90 min |
| Reviews FULL, READY | OnehistoricalFULL; ADF1minactive |≤12 h|≤24 h|≤30 h|≤36 h|≤40hinternal;≤48hbusiness | DE;43.88Msource occurrences require measured full execution |
| Reviews daily, READY |1 source day/10elapsedmin; ADF1minactive/15minrecoveryonly | p95≤5 min | p95/p99≤15/30 min |≤25/45 min|≤35/60 min|≤45/75 min;99%≤90 min; cache≤10 min | Supplier+DE; stop original expectations after 13Sep2023 |
| Campbell FULL, READY | Seven historical packages; ADF1minactive |≤15 min|≤30 min|≤45 min|≤60 min|≤75 min; onboarding≤120 min | DE/station gateway; small files feasible by design, runtime pending |
| Campbell daily, READY | Perstationdaily02:00 UTC; ADF1minactive | p95≤5 min |≤15/30 min|≤25/45 min|≤40/60 min|≤50/75 min;99%≤90 min;04:00UTCdeadline; cache≤10 min | Station/source+DE; no raw-logger-time→UTC SLA |
| Open Prices tail, providercreated eligiblevisibility | Provider independent; ADF acquisition15min/H5min and independent ADF receipt dispatch5min | Accepted acquisition≤30 min |≤45minend-to-end; receipt→B≤15 min|Selected≤60 min|Selected≤75 min|Selected99%≤90 min; cache≤5 min | APIowner+DE; conditional runtime gate; no provider visibility/uptime guarantee |
| Open Prices bootstrap/older revisions | Boundedhistory; weeklyRESTsweep | Peracceptedwork | Perreceipt≤15minobjective | After eligible capture | After eligible Silver | No invented complete-history deadline or native mutation90minSLA | APIowner; coverage/sweepage reported, densebuckets canblock |
| REES46 Kafka, publish/brokerappend |60×bounded independent producer; native Bronze10s/business30s | No ADLS event Raw | p95≤30s/p99≤120s | Selected p95≤60s/p99≤180s | Independent G08 p95≤90s/p99≤300s | Selected≤360s with actual consumer; source historical event time is not latency clock | Producer/broker/DE; actual2 worker B/S/G and shared host qualification required |

Selected API and independent Kafka S/G/consumer objectives are new simulated business policies, not R1 provider promises. Consumer SLA cannot be measured until an actual query/refresh client exists. An API record exposed later than its own created timestamp/overlap is reported as late visibility/re-observation, not hidden by redefining provider guarantees.

### 17.2 Scheduling arithmetic and target classification

**OFF/metadata/Campbell/active-review SFTP (excluding monthly REES46):**5 min polling has a nearly5 min worst phase and approximately4.75min95th percentile phase under uniform arrival; only15 s remain before a5 min target, before preflight/copy/hash. That schedule does not establish the objective. Selected1 min active polling provides a Raw budget: discovery≤1 min, admission/preflight≤1 min, copy+full landed verification≤3 min. Warm compute and small bounded DELTAs must meet those actual bounds. Native accepted-file publication/ordinal-header parsing, direct ADF Auto Loader dispatch, all micro-batches and finalization are additional Bronze latency components, not hidden inside successful Copy. Their combined actual runtime must fit each source's Bronze15/30-minute or FULL/monthly objective; no extra periodic Auto Loader clock is inserted. End-to-end p95 must be measured directly; adding component p95 s is not a proof of an end-to-end percentile.

**REES46 SFTP specifically:** monthly READY is discovered by10 minute ADF polling. Worst nominal schedule phase≤10 minutes, leaving≤50 minutes of the original READY→Raw60 minute objective for admission/transport/verification. Bronze/Silver/Gold/consumer120/180/240/270 minute targets are measured end-to-end;10 minute polling is not a guarantee of any processing deadline. Missed ticks resume from actual source readiness/manifest state. No5 minute REES46 discovery variant remains. OFF/metadata/Campbell and review active clocks remain1 minute under retained amendments, not10 minutes universally.

**API:** creation→acceptance latency is15 min ADF schedule phase+5 min horizon+Qa(ADF queue/submission and compute startup/admission)+Ta(acquisition/seal). Receipt publication addsP. Receipt→Bronze adds the independent5 min ADF schedule phase+Qb(ADF queue/submission/startup)+Tb. Combined bound:
**25 min+Qa+Ta+P+Qb+Tb≤45 min.**
Select Qa+Ta≤9.5 min, P≤0.5 min andQb+Tb≤10 min, so receipt publication is not unbudgeted. Persist/read back receipts in the30 s finalization slice; a missed bound is an incident, not permission to advance an incomplete frontier. Queue-disabled/overlong skipped runs can break freshness while preserving eventual correctness. Those misses remain in metrics.

**H&M:** source snapshots/imports have separate onboarding milestones; the5 min CDC target applies to eligible steady-state simulated commits after the baseline handoff. Article15minpolling cannot guarantee all article attributes within the purchase15 min consumer target; AM-11 explicitly separates these.

**Kafka:**10 s is a trigger interval, not processing time.150000/10 s=15000offsets/s is a nominal cap, not measured sustainable rows/s; offsets can include nondata/aborted ranges.6 partitions, actual record sizes, Python/Delta overhead, commit duration and skew determine throughput. At8,415 source-derived peak/s, about 84,150events arrive per10 s, but this estimate does not qualify the path.

| Classification | Selected finding |
|---|---|
| Feasible structurally | Continuous native capture; small station/day increments; correct asynchronous API receipt recovery; source-backed missed-tick catchup |
| At risk pending measurement | All exact p95/p99 targets; largeFULL/monthly processing; warmSFTP5minRaw; APIcold-start/ledger overhead;85.2hstream |
| Inconsistent without amendment |5minpolling asserted as5minend-to-end guarantee; fully enriched new article within fasterCDCdeadline; receipt delay omitted from API45 min |
| Controlled amendments selected | AM-04polling, AM-11 fact/enrichment split, explicit API combined budget; no silent relaxation of advertised metrics |

### 17.3 Detection, escalation and evidence

Monitor source-specific missed-delivery thresholds:
- OFF/Amazon metadata/reviews warning10 min after due, late incident30 min, missing60 min.
- Campbell warning15 min, incident60 min, missing expectation120 min; each station independent.
- Articles oldest unACKed member>20 min warning,>30 min or≥2missed cycles incident.
- CDC lag>60 s for 5 min warning,>300 s or missing committed transaction incident.
- Kafka age>30 s for 5 min warning,>120 s incident; producer-progress/member gap immediate integrity hold.
- API tail success age>30 min incident; accepted-without-receipt>5 min warning/repair; receipt-without-Bronze>15 min incident.
- REES46 monthly lateness versus scheduled02:00and08:00business deadline stays visible; no other month substitutes for missing March.
- Critical SHA/identity/sequence/unauthorized access failure alerts immediately, regardless of freshness percentile.

Use actual source/receiver clocks, source_due, READY/write acknowledgment, ADF queue/start/submission, Copy end, Raw verification, native accepted-file publication/ACCEPTED_READY, native file/batch completion, whole-delivery finalization, Gold publication and actual consumer query times. Store minimized outcome rows in the four controls, immutable detailed reports in evidence and operational alerts in Monitor/ADF/Jobs/broker logs. Track clock error; do not compare unknown Campbell logger time with UTCarrival for latency.

P1triage≤15 min, containment decision≤60 min, conditional recovery objective4 h; P2triage≤60 min/next-business-day where source specifies. These are simulated operating objectives, not staffed-service or30-day99.9%availability evidence.

## 18. Recovery, replay and canonical cutover

### 18.1 Ordered decision process

1. Classify scope: source transaction, physical transport, target commit, native checkpoint, business conformance or publication.
2. Stop new conflicting admission and establish actual owner termination/fencing; retain source/target/checkpoint evidence.
3. Resolve uncertain source action/Kafka commit/Delta command by durable identity before any replacement.
4. Prefer normal restart with the same native state and compatible release.
5. If native state is lost, select a declared recovery generation and isolated target/checkpoint.
6. Reconcile exact retained source/operation/occurrence coverage against a frozen candidate version.
7. Publish/cut over only after actual readiness/certification; retain old target evidence and rebind dependent readers.
8. Record actual RPO/RTO, missing ranges/identities and any unrecoverable physical history.

### 18.2 Source-specific algorithms

| Failure | Selected recovery |
|---|---|
| SFTP Copy/parse/Auto Loader failure | Reuse verified Raw and winning native accepted path set. Same native checkpoint and append semantics repair partial batches; finalization still runs when discovery is empty. Recopy only when original Raw is invalid/unavailable; no new source action. |
| SFTP Auto Loader checkpoint loss | Preserve damaged state and stop/fence owner; restore a coherent checkpoint or use a declared new consumer/recovery generation. Reconcile actual file/delivery markers and original source identities; never invent file offsets or blindly append all discovered files. |
| Supplier journal crash | Restore consistent catalog/journal/action receipts/sealed definitions; fence old publisher; source generation unchanged only if identity/history proof holds. |
| Article journal ACK loss | Look up exact sealed members and actual target marker; repair ACK. Late committed members remain unACKed for next seal. |
| Connect restart | Same slot/publication/native internal offsets; repeated source events harmless through native op identity. |
| WAL/slot or Connect state loss | Stop CDC completeness claim; restore only with proven continuity; otherwise declared gap/new source generation/reconciled consistent snapshot. |
| API request owner loss | Terminate old job compute,90smargin, new guarded epoch; unknown attempt cooldown; incomplete live traversal restartspage1, complete registered pages can seal offline. |
| API acceptance/receipt/consumer loss | Repair A−R andR−B using retained exact evidence; native checkpoint reset never reconstructs acquisition frontier. |
| Silver/Gold job loss | Read actual source unit/target marker; repair partial targets/stage receipt, then affected business groups. No rollback of successful upstream acquisition. |
| CDF/version history expired | Stop blind incremental resume; bound a current/snapshot rebuild and independent source comparison. Do not assume400dlogs retain all data files. |

### 18.3 Kafka checkpoint loss and retention expiry

Pause the owning continuous Job and prevent automatic/manual restart races. Confirm run termination and actual compute termination. Preserve damaged checkpoint/target before recovery. Restore a coherent compatible checkpoint if possible.

Otherwise:
- New isolated target and new native checkpoint; consume required retained committed Kafka history.
- If required Kafka records expired, keep failOnDataLoss=true, identify required missing ranges and stop false-completeness claims.
- If existing Bronze is complete, rebuild downstream from it; do not unnecessarily republish411Mrecords.
- If source reseed is needed, stop former producer or revoke its old-generation publish access before creating a new topic generation. A different transactional.id does not automatically fence an old generation.
- Republish exact pinned source occurrences through the same independent producer. Original logical event IDs/simulation identity remain; new physical offsets/append times are new evidence, not recovered old transport history.
- Reconcile a complete finite source candidate; for partial/incomplete history, publish a declared limitation rather than a complete certificate.

A4hRTO is not achievable by a full60×85.2hreseed. If that procedure is necessary, record the objective breach explicitly. Same-source-data reconstruction can restore logical business occurrences but cannot recreate lost original Kafka bytes/offset/timestamp history.

### 18.4 Safe Bronze view cutover

Use an exclusive operator/deployment window. Both old and candidate owning schedules must be PAUSED, no active/queued/manual runs, and actual runs/compute TERMINATED. A completed previous run alone is insufficient because a continuous Job can auto-restart.

Certificate includes source/simulation/topic generation, coverage mode ORIGINAL_KAFKA_HISTORY versus SOURCE_OCCURRENCES_RECONSTRUCTED, target table UUID and Delta version, release/schema, complete exact expected/actual membership report, quality disposition and retained-files proof. Refuse certification if the candidate changes during validation. Recheck actual UUID/version/schema and termination immediately before an atomic CREATE OR REPLACE of the canonical view.

Only one complete target is canonical; never UNION overlapping targets. Keep old physical evidence. Running streams do not automatically change source because a view definition changed: stop/rebind each dependent reader, then choose a compatible/new checkpoint and validate its target effects.

Avoid duplicate business effects through stable source identities, one canonical target and deterministic downstream rebuild/publication. This is an explicit application/recovery protocol, not a claimed distributed exactly-once transaction across Kafka/Delta/Silver/Gold.

## 19. Monitoring, identities and cost controls

### 19.1 Minimal identity inventory

| Identity | Needed boundary; no per-folder proliferation |
|---|---|
| Federated GitHub deployment principal | Approved infrastructure/Jobs/ADF/UC grants; no runtime secret embedded in Git |
| Platform runtime principal | Named Bronze/Silver/Gold/control privileges and UC volumes for native file/Delta access; no SFTP executor SDK credentials or per-source identity |
| API acquisition principal | Sole acquisition ledger writer; its own narrowly scoped Azure SDK evidence creation credential; also Job run-as—no additional API-SDK principal |
| ADF system managed identity | Raw writes/config reads, required Key Vault reads and Databricks run/observe permission |
| One Access Connector managed identity | UC external locations/volumes, reusable storage credential |
| Shared execution-host managed identity | Seed reads, necessary source audit writes and Key Vault reads; cannot advance receiver ledger/checkpoints |
| Two SFTP local users | Shared publisher and shared receiver, ordinary container permissions as§7 |
| PostgreSQL roles | Import/source application, replication and read-only journal extraction privileges; separate because database duties differ |
| Kafka/Registry service credentials | Native SASL/TLS/Registry producer/consumer/Connect/admin scopes; minimum topic/transactional-ID ACLs on selected private broker |

Do not treat a UC Access Connector as automatically usable inside a Python Blob SDK. API acquisition uses its explicitly supported SDK token flow. Kafka/Registry credentials, SFTP private keys, DB credentials and any SDK credential remain in Key Vault/Databricks secret scope or protected runtime files. Read-only public REST does not imply authenticated Azure storage.

One region initially, TLS/certificate verification for HTTPS/database/Kafka, SSH host-key verification for SFTP. No anonymous containers or unrestricted DB access. Restrict raw customer/user/postal identifiers, review text/images and API owner/comments. Gold default views minimize sensitive payloads. Original data use/licensing and immutable retention compatibility remain source governance gates; engineering quality is not commercial redistribution permission.

### 19.2 Operational monitoring

Use native ADF monitoring/diagnostics, Databricks Job notifications/Spark event and stream metrics, PostgreSQL native slot/transaction metrics, Confluent metrics and Azure Monitor/action group. Minimal diagnostics can reuse existing log destinations; no custom telemetry service or new control database.

Track: source availability, expected-delivery status, oldest unACKed/sealed work, exact gaps, SFTP Raw/unwatched/published/ACCEPTED_READY counts, native cloudFiles backlog, partial delivery/source scope, delivery-finalization debt, source commit/export/capture/conformance/publication lag, APIattempt/breaker/frontier/receipt debt, Kafka per-partition lag/retention horizon, WAL growth, CDFhorizon, Spark failures/spill/skew, schema/DQexceptions, target version, current release and actual billable node hours/storage/traffic. Alerting health itself is checked—missing telemetry is not healthy ingestion.

Use400dminimal audit evidence; keep detailed sensitive/debug logs≤7 d and aggregate cost/performance measures without raw source values.

### 19.3 Dated public pricing evidence and formulas

No actual subscription region/credit balance/discount or billable resource activation is verified/authorized by this phase. Premium PAYG is the user's accepted billing direction. The following **illustrative East US USD retail meters** were read from Microsoft's public Retail Prices API during the retained A3 audit on 30 October 2026; they are not an account quotation. Product/SKU filters exclude Windows/Spot and the similarly named Cloud Services product.

| Public meter / selected SKU | Returned retail unit price |
|---|---:|
| Virtual Machines Ddsv5 Series, Standard_D4ds_v5 |$0.226pernode-hour |
| Virtual Machines Ddsv5 Series, Standard_D8ds_v5 |$0.452pernode-hour |
| Virtual Machines Dasv5 Series, Standard_D4as_v5 |$0.172pernode-hour |
| PostgreSQL Flexible Server General Purpose Ddsv5,2vCore/Standard_D2ds_v5 |$0.178perhour for the returned2vCoreSKU meter; do not multiply again by2 |
| Azure Databricks Premium Jobs Compute DBU |$0.30perDBU-hour |
| Azure Databricks Premium All-purpose Compute DBU |$0.55perDBU-hour |

Endpoint: [Azure Retail Prices API](https://prices.azure.com/api/retail/prices); filters serviceName, armRegionName=eastus, priceType=Consumption and exact armSkuName/product/sku. The API labels DBU meters “1Hour”; their quantity is **DBUs**, not VMcount. Actual per-SKU/runtime/Photon DBUs per node-hour are still quote/configuration inputs. No region/account-specific DBU quantity is invented.

For the unselected managed comparison only, Confluent's published Standard rate is$0.75/eCKU-hour, plus traffic/storage/governance. The provider states its99.99%SLA option requires at least2 eCKU. This architecture does not purchase/claim that option or call one eCKU an all-inclusive service cost. [Confluent pricing](https://www.confluent.io/pricing/) [Cluster/SLA options](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)

SFTP rate r_sftp must come from the selected region's current Blob price quote. The current service documentation establishes hourly enablement billing, not a universal account-specific dollar rate. Do not reuse an old$0.30Q&A quote as verified2026pricing.

### 19.4 Premium PAYG, temporary Kafka and active/residual cost worksheet

The user confirms Premium PAYG is an acceptable billing direction and is willing to fund necessary temporary infrastructure. **No paid activation is authorized by this architecture task.** The$200promotional amount remains a budget objective, not verified available credit. Actual balance/expiry/offer, regional Premium DBU/VM eligibility/quota/prices and authorized out-of-credit limit remain account-specific gates; do not upgrade/change spending protection or incur out-of-pocket charges by inference.

Selected Kafka now runs on the shared Azure VM; there is **no default Confluent subscription/eCKU/traffic invoice or third-party trial funding gate**. Broker/Registry consume real paid host/disk resources and can increase measured capacity needs; their cost is included, not called free. Managed alternatives stay unselected quote comparisons. User-selected two storage accounts remain in the same subscription.

Use §19.3's dated illustrative East-US VM/Jobs DBU rates only for arithmetic, not this account's quotation. Let u4 be the actually quoted DBUs per D4ds_v5 node-hour for selected runtime/workload (Photon disabled initially). Node-hour rate=0.226+0.30u4 for Jobs; all-purpose would instead use its actual DBU meter, never silently Jobs pricing. Startup/shutdown/retries count.

| Workload / admitted native input | Driver/workers / allowance | Planned runs and node-hours | Per-run / total illustrative VM+DBU cost |
|---|---|---|---|
| Tiny functional smoke only | D4driver+1 worker,≤15 minutes |1run;≤0.5 node-hours |≤$0.113+0.15u4; not distributed qualification |
| Native SFTP/Silver/SCD2/API-business/Gold/optimization | D4driver+2workers,2hour session allowance |4sessions;24 node-hours |Per session$1.356+1.80u4; total$5.424+7.20u4 |
| H&M actual snapshot/WAL/SCD2 slice | D4driver+2workers,2 hours |1session;6 node-hours |$1.356+1.80u4 |
| Actual Kafka independent B/S/G, fixtures/schema/restart | D4driver+2workers,2 hours; compatible shared Job task compute |1session;6 node-hours |$1.356+1.80u4; no extra always-on Silver/Gold cluster |
| D10 serialized API acquisition | D4single node,≤12 minutes billed/run incl startup |16slots;≤3.2 node-hours |Per run≤$0.0452+0.06u4; total≤$0.7232+0.96u4 |
| Independent D10 receipt/Bronze | D4single node,≤6 minutes billed/run incl startup |48slots;≤4.8 node-hours |Per run≤$0.0226+0.03u4; total≤$1.0848+1.44u4 |
| Optional later scale-up | Not initially provisioned/scheduled |0planned4/8 worker runs |Quote only after actual bottleneck/performance question and approval |

Plan A Databricks≤44.5 node-hours: **VM$10.057 +13.35u4 DBU cost**, before allowance overruns/all-purpose changes. This is a bounded **planning allowance**, not measured runtime or a completion guarantee. Require exact approved quote/session meter estimates before launch.

| Source/service/residual meter | Active-window formula and selected decision | Residual/cleanup liability |
|---|---|---|
| Shared source/KRaft/Registry/Connect/producer VM |4 vCPU/16 GiB D4as_v5;12active hours initially × illustrative$0.172=$2.064 | VM compute stops only on deallocation; data/OS disks keep billing. Kafka service adds no second VM by default. |
| Real managed PostgreSQL | GP D2ds_v5;12compute hours × illustrative$0.178=$2.136 | Provisioned storage/backups and actual restart/stop policy still bill; retention/source closeout independent of SFTP. |
| Source VM OS/data disks |64 GiB OS +128 GiB data starting tier; (q_os+q_data)×retained_hours/730 | Include active days plus actual hold/closeout buffer; Kafka logs/KRaft/Registry/Connect share measured capacity. Deletion only after no other source dependency. |
| Kafka log footprint | Actual encoded bytes + log/index/internal/Registry overhead, RF1DEV;≤32 GiB initial admitted log allowance within disk plan | No paid broker cluster remains after authorized teardown; deleting state ends remaining broker replay rights. Disk/broker generation proof archived first. |
| SFTP feature | Actual region hourly rate × enabled transfer-window hours, initially≤12 hours | Disable after transfer/ACK drain; supplier Blob bytes/operations still bill until approved cleanup. |
| ADF | Actual activities/status polling/CopyDIU-hours; source-specific active clocks, REES10 minutes, others per contracts | No Mapping Data Flows/extra metadata DB. Stop triggers outside service windows; not proof skipped data was captured. |
| SFTP Raw/accepted/managed Delta/source images | Time-weighted native/envelope/accepted/Delta bytes × rate + operations | DEV-RET-01 active+buffer/authorized cleanup; **no365day SFTP WORM liability**. Include source JSON image/identity overhead; no receiver Parquet copy. |
| API D10 protected evidence/receipts | Actual stored bytes × tier/replication/365–400day horizon + operations | Separate funded long-retention/lock obligation; D10-DEV-RET-02 not active. This residual cannot be erased by SFTP/broker closeout. |
| Other source/target/audit storage | Actual source-specific retained bytes/protocol/history + backup/operations | Original non-SFTP obligations unchanged unless explicitly amended; closeout manifests define actual removed/retained scope. |
| NAT/IP/DNS/Key Vault/Monitor | Actual provisioned gateway/IP hours+data+DNS/secret/log meters | Include all running/idle hours until actual authorized resource deletion; VM shutdown is not network shutdown. Private Kafka adds no public endpoint/managed egress service. |

Illustrative source VM+database compute adds$4.20; disk/storage/ADF/SFTP/network/audit/API retention quantities remain quote inputs. Exact projected Azure cost plus residual contractual liabilities must fit **verified available credit C and separately authorized funding**, not a fabricated$200total. Legacy96hour all-source compute alone exceeded$200; it is not the selected plan.

Retain the planning envelope at C=$200: Databricks$85, source host/database/disks$25, storage/retention$20, SFTP/ADF$25, networking/KeyVault/monitor$15, contingency$30. These are caps, **not priced estimates or actual remaining balances**. Remaining buffer=C−all projected active charges−residual retention/resource liability. If an actual quote exceeds its cap, reduce representative input/window or obtain explicit funding; never silently buy resources. No4/8 worker reservation.

**Plan A:** meaningful distributed-first four-day representative populations from§21.1, real native SFTP/actual database/REST/Kafka, one heavy2 worker Spark session at a time. Keep source clients/broker only during required windows; no unattended-service SLA claim outside the declared calendar. Actual reference fixture/event identities are preserved; full411Mreplay/complete histories are later qualification.

**Plan B:** same33tables/four real interfaces,2×1hour two-worker transformation sessions,1hour H&M stream,1hour Kafka B/S/G,8acquisition/24receipt ticks; omit smoke/scale-up if unnecessary.≤16D4 node-hours gives VM≤$3.616 +4.80u4, plus source/services/residuals. Smaller5 GiB admitted retained footprint versus PlanA20 GiB does not change native interfaces, schema/identity/DQ or source contract scope declarations. Physical source/managed Delta sizing is measured; these caps are not source results.

Both plans remain financially unqualified until actual account/quote/permission evidence is available. Funding failure blocks paid activation, not another routine architecture decision. Do not silently weaken D10 retention to meet the budget or retain an unnecessary managed Kafka invoice.

### 19.5 Spend and shutdown controls

Tag resources DEV/source/owner/expiry and release. Alert at 50/75/90%of the subsequently approved available-credit envelope; close new discretionary admission at 90%and reconcile active/retained obligations. Budgets notify rather than hard-stop. Four-day bounds are the Plan A/B session/run/population limits above, one heavy workflow, fixed stream workers, one source publisher lane and explicit SFTP/compute windows. The retained 8 worker full-history option is not an approved DEV budget allocation.

At experiment shutdown, stop new source mutation/admission, resolve or fence transactions, record outstanding accepted debt, pause ADF/native schedules, terminate Databricks compute, stop Connect/producer, deallocate the execution VM, stop PostgreSQL where appropriate and disable SFTP after transfers/ACKs drain.

Do not delete retained topics, checkpoints, Raw or managed disks to reduce cost without a separate retention/recovery decision. Managed Kafka alternatives, storage, NAT and disks can continue billing after compute stops. Selected local KRaft has VM/disk costs, not an eCKU subscription; authorized closeout removes the right resources only after state/evidence dependencies clear.

## 20. DEV GitHub CI/CD and eventual deployment

This phase creates no workflow, bundle, ADF JSON, SQL, Terraform, notebook, config or implementation file. The following is the planned later delivery process.

### 20.1 Repository and release process

Use harshitarav/azure-databricks-project and default main. Inspect and reuse relevant original workspace implementations before any approved migration. Development branches use codex/ by default; reviewed changes merge to main. One DEV environment is retained.

Planned PR CI validates contract/artifact fingerprints, metadata/path allowlists, schema compatibility, DQ mappings, secrets, pinned packaging and appropriate component tests. It validates future bundles, ADF templates and infrastructure definitions against pinned tools. Mock tests never certify cloud semantics or full populations.

A release binds package hashes, Git commit, source contracts/decoders, metadata release, runtime/tool locks and actual resource bindings. GitHub OIDC/workload federation supplies deployment tokens; no source credentials or long-lived deployment PAT in Git. The DEV environment approval/concurrency guard serializes deployments.

CI does not automatically run simulators or terraform apply. A separately authorized gate applies approved billable infrastructure, deploys Jobs PAUSED/ADF triggers STOPPED, then runs authorized migrations/connectivity/qualification. Architecture approval does not authorize that gate.

Rollback redeploys a compatible release. A checkpoint/schema-incompatible change needs a reviewed migration or recovery generation; never blindly restore old code over incompatible state. Keep native source/target commit identities for forward repair.

### 20.2 Planned deployment order after separate approvals

1. Confirm source rights, subscription/tenant/region, budget, quotas and architecture/coding/resource authorizations.
2. Reuse or create DEV foundation, network/egress/DNS, two compatible accounts, Key Vault, ADF, Databricks/Access Connector and selected PostgreSQL.
3. Create actual Kafka/Registry topics and ACLs, and the shared execution host without a SFTP server, public database or exposed Connect REST port.
4. Discover UC catalog; create approved schemas, non-overlapping volumes, managed tables and grants through reviewed migrations.
5. Release inspected reusable code/contracts/metadata; initialize genuine source schemas, journals and import receipts only after execution authorization.
6. Resolve actual Registry IDs/runtime bindings, two SFTP local users/container permissions, host keys and native UC/ADF accepted-path permissions; no SFTP executor SDK credential.
7. Deploy paused Jobs/stopped ADF triggers; disable native cron for ADF-owned article/API/SFTP/domain-product Jobs; validate bindings, network, secrets and native UC access; API SDK scope is separate.
8. Qualify control guards, native transaction/isolation and snapshot handoff, original ordinals/decimals, accepted-file publication, partial Auto Loader recovery/finalization, product ownership and an actual slice per interface.
9. Activate only approved finite schedules/windows and collect actual freshness, performance and cost evidence.
10. Implement and qualify source-specific Silver/Gold and late-correction/recovery publication as authorized.

This is a deployment plan. No resource or pipeline in this list was provisioned or executed by this audit.

## 21. Four-day targeted implementation sequence after separate approval

This is a prioritized execution plan, not work performed or a promise that full source histories/150 future cases fit four days. Preserve AM-14 representative source scopes and original identities. Use minimal initial Jobs compute and short service windows (§11/19); no large cluster during setup.

| Day | Prioritized slice | Required representative exit evidence |
|---|---|---|
| 1 | Actual account/quote/permissions approval, inspect/reuse work; same-subscription supplier/lake/KeyVault/ADF/Databricks/private network; shared host KRaft/Registry/native state readiness before genuine H&M database snapshot/journal/WAL slice | Actual source count/decimal/FK/import/seal/native snapshot-to-WAL proof; real private broker transactions/Registry IDs; no CSV-to-Bronze bypass or HA claim. |
| 2 | Publisher SP-01identity/native part release; all five actual SFTP/ADF/native cloudFiles/Bronze paths; source-specific Silver/Gold slices | Native CSV/JSON/Parquet/text read plans/options, persistent C/S, exact publisher IDs/raw/member sets/empty/retry/reconciliation/ACK. No executor SDK or receiver Parquet layer. |
| 3 | Actual D10 REST/ledger/receipt/Bronze and distributed API business; independent REES producer/native Kafka Bronze plus physical Kafka Silver/G08 | D11 physical/source identity fixture, original repetitions versus publications, late/out-of-order correction and independently published Kafka/SFTP products. Two-worker native tasks and real Registry/offset evidence. |
| 4 | H&M SCD2/as-of/late repair, native schema-mode and consumer-compatibility experiments, representative Spark/recovery/CI-CD/monitoring/cost/closeout review | Named PASS/FAIL/NOT_EXECUTED evidence and actual plans/executors/spill/lag/cost; no claim all 150tests/full411Mhistory passed. Teardown only after separate explicit closeout authorization. |

Continuous source clients/query checkpoints remain native. Source service windows freeze mutation outside them, drain transactions/queries and preserve required state. Kafka host can deallocate between windows after H&M/REES dependencies are safe; persistent disks still bill. No scheduled restart per event/micro-batch.

Architecture complete, implementation complete, representative runtime passed, full-source historical passed and production-style recovery passed remain distinct. This phase establishes only the documented architecture deliverable. Full historical processing/14day retention/replica-HA/destructive qualification needs later actual scope, permission and funding; those tests are not credited to a four-day representative run.

### 21.1 Deterministic representative population and source-contract scope

**AM-14 representative DEV scope overlay/1.0.0 (proposed):** source generation IDs explicitly distinguish REPRESENTATIVE from the original FULL source generation. Original global hashes/ordinals, payloads and schema contracts remain, while exact selected membership is frozen before source initialization/publication. Full-source count/DQ tests remain NOT_EXECUTED/FULL_SCOPE_NOT_RUN until their whole original population is evaluated; a selected subset comparison cannot pass those full-population predicates. This overlay is authorized as a proposed architecture choice by the user's representative four-day requirement; it does not weaken full enterprise obligations.

| Interface/source | Selected Plan A initial admission population / selection | Plan B reduction and exact evidence |
|---|---|---|
| PostgreSQL H&M | First≤250000 original purchase ordinals in the approved initial region, plus exact referenced article/customer FK closure; include required approved source fixture keys. Load into real DB, preserve original purchase_id/price text. |≤50000purchases + FK closure. Manifest records all original IDs/counts/SHA/membership; no fabricated seed/changed ordinal. Initial subset count is not the31M+full-source count. |
| REES SFTP/Kafka |≤1000000 original occurrences in one declared contiguous source-time window for each bounded run, with D11's verified fixture ordinals included in an eligible isolated run. SFTP and Kafka retain separate source-authority/reconciliation. |≤100000occurrences.60× remains within each selected window; any skipped interval or separately started fixture run is declared, not presented as complete continuous history. Source duplicate payloads retained. |
| OFF | First≤100000 original JSONL ordinals for an attended real supplier generation, with exact eligible canonical/exception classification from retained evidence, plus declared journal fixtures. |≤10000ordinals. Stream source to choose them after approval; no reprofile now. Exact byte/identity/exception manifest; full catalog profile remains authority for later full run. |
| Amazon metadata | Prefer all 10 original native Parquet shards (verified1,610,012 rows), only if the total retained-footprint/session admission caps fit; otherwise first registered shard(s), no conversion. |One original native shard plus referenced review/fixture-key closure through declared source exports if needed. Exact selected keyset/multiset and native schemas; no claim full10shard coverage when reduced. |
| Amazon reviews | First≤250000eligible original pre-cutover ordinals plus declared reserved-day/fixture membership; original global ordinal map kept before selection. |≤25000eligible occurrences plus fixtures. No natural-key/content dedup; missing unselected metadata remains explicit, never invent a join. |
| Campbell | Prefer all 7small historical station files plus representative original station-day/empty/revision fixtures under the input/time cap. |One declared station/family plus required v2/empty examples in isolated runs. Station/time/header/RECORD and selected original ordinal manifest exact. |
| Open Prices | Genuine D10 bootstrap/tail roots with bounded leaves under normal request/time/rate budgets; exercise recent accepted windows plus an eligible historical leaf if budget allows. |Fewer scheduled ticks/leaves, same actual endpoint/ledger/receipt semantics; remaining bootstrap debt explicit. Never replace history with export or declare whole live source captured. |

Representative publications are explicitly DEV_ONLY/source-generation-scoped. A later full source generation replaces the representative generation only after exact source/target/product certification and bound-view cutover (§18); consumers select one active generation per source/domain, never UNION representative and full populations into duplicate business totals. Original repeated source occurrences within that chosen generation remain distinct.

All population limits are **selected maxima**, not newly measured source results or promised completion times. Exact runtime selection may stop earlier at the20 GiB Plan A/5 GiB Plan B retained-footprint or session/funding cap; manifest actual selected membership and count, never advertise the cap as delivered. Source initialization freezes this declared membership before mutation; later fixtures are separate approved source operations. The33table logical platform and all original source populations remain designed for subsequent full-scale qualification.

## 22. Remaining qualification gates and review boundary

| Gate | Required evidence before its corresponding activation claim |
|---|---|
| GATE01 architecture approval | User explicitly approves A4.2/SP-01/receiverR4/KSG-01/DEV-RET-01/DEV-KAF-01 and retained OA-API/AM-13/AM-14 policies; recording the document is not approval. |
| GATE02 coding/resources | Explicit implementation authorization; separate authorization for billable provisioning and budget. |
| GATE03 infrastructure | Actual region, subscription, tenant, identity permissions, SKU/quota, network/DNS and discovered UC catalog. |
| GATE04 storage compatibility | Actual HNS/SFTP authentication, rename, read-back and ADF connector behavior; API-only evidence WORM/retention compatibility, no SFTP Raw WORM. |
| GATE05 source initialization | Exact hashes, allocations, original ordinals, canonical/exception populations, FKs and NUMERIC round-trip against actual PostgreSQL/source catalogs. |
| GATE06 native CDC | Supported connector/runtime, slot/publication, consistent baseline, snapshot-to-WAL handoff, END membership/order identity, restart and WAL budget. |
| GATE07 Kafka protocol | Actual private KRaft/topic/Registry IDs/settings, explicit RF1DEV internal-topic configuration, abort/fencing/read_committed/process restart/compaction/TLS/ACL compatibility; enterprise RF3/failover separately unqualified. |
| GATE08 API provider | Legitimate read-only documented filter/envelope/schema conformance; no provider load/fault tests or export bootstrap. |
| GATE09 guarded Delta | Actual CAS loser inserts zero events; deterministic commands; one HEAD; uncertain commit resolution and actual owner termination. |
| GATE10 native file capture | Exact original source/Raw/accepted membership/schema/ordinal/header; native CSV/JSON/Parquet/text append for all five; logical whole-delivery certificate, empty/partial/uncertain commit and lost ACK recovery. |
| GATE11 native Auto Loader | Actual csv/json/parquet/text parsing/options; source-published identity/native TOA5 transforms; bounded validation; schema-mode/Delta compatibility and multi-worker plans; no executor SDK reader/fallback. |
| GATE12 DQ definitions | All 275 identified rules retained at their source scope; implement/evaluate selected OFF-A41 predicates and Campbell registered-field trusted-statistics policy (§14.2.1), preserving original source dispositions. |
| GATE13 business correctness | Existing G01–G07 plus independent Kafka Silver/G08 grain/duplicate/user/session/late-event oracles, source-generation isolation, SCD2/as-of/deletes and publication versions; no combined SFTP/Kafka population. |
| GATE14 latency | Actual stage/consumer metrics, including ADF queues, preflight, transfer/hash, native accepted publication/ordinal-header parsing, native file batches/finalizer, API receipt publication and Gold dependency waits. |
| GATE15 full-source coverage | Complete source identity, multiplicity and operation reconciliation, including all 411,709,736 REES46 occurrences; no count-only/sample extrapolation. |
| GATE16 recovery | Actual checkpoint, retention, slot, offset and cutover drills; frozen UUID/version proof; reader rebind; no duplicate effects; measured RPO/RTO. |
| GATE17 security/rights | Real TLS/SSH/ACL checks and secret rotation; restricted logs; source-use, attribution, privacy and retention permissions. |
| GATE18 actual funding/release | Active credit/expiry/spending protection, Premium/VM/region/quota eligibility, actual DBU quantities/meters and full source-host/disk/API residual funding; PlanA/B envelope, compatible paused release and shutdown. No upgrade/out-of-pocket authorization. |
| GATE19 orchestration ownership | Actual source/Job schedule bindings, no dual API/article/source clocks, independent API receipts, single product writers, duplicate/queued/uncertain ADF run recovery and cross-source readiness. |
| GATE20 SFTP native semantics | All five native formats/seven compatible C/S scopes; source-published identities and TOA5 mapping; schema inference/hints/rescue/modes/restart and append/finalizer/empty recovery; no SDK ingestion or custom marker protocol. |

### 22.1 Final eight-integration source-specific verification

These are architecture/document checks, **not runtime tests**. D01–D11 and original profiling remain source authorities; D05 hash formatting is outside this correction. §3 retains all 80R01–R10 slots and §9.4 all 10 operational entities/eleven Git categories. The common required metadata record plus source-specific routing supplies schema/DQ/identity/connection/credentials/paths/targets/dependencies/retries/release/ACK/replay fields for each entity.

| Integration | Source/seed/FULL and incremental behavior | Actual acquisition/landing/Bronze | Silver/Gold and independence | Schema/DQ/frequency/owner/progress | Recovery/cost dependencies |
|---|---|---|---|---|---|
| PostgreSQL H&M | Original articles/customers/transactions CSV initializes a real DB; exact sealed article baseline/journal, native customer/purchase snapshot then WAL; future simulation distinct from historic truth | ADF onboarding/articles; sealed JDBC and native Debezium→private Kafka→native Bronze hm_articles/hm_customers_cdc/hm_purchases_cdc; no CSV-to-Bronze bypass | hm_article_history/customer_history SCD2, purchase_current → G01/G02; exact decimals and historical-unknown/pending dimension rules retained | D01R3/HM36; article15 minuteADF, continuous connector/native checkpoints; source journal/Connect state owns progress; DEV-KAF-01 transport availability explicit | Snapshot/WAL/member/slot/state recovery; private DB/Kafka/shared host/retained source state, no ordinary CDC polling |
| REES46 SFTP | Seven original monthly files; OctoberFULL/subsequent months, Novemberr2/Aprilpayloadv2; SP-01publisher original ordinal and bounded native CSV/gzip parts | Actual Azure SFTP/ADF Binary Copy/native accepted CSV cloudFiles→bronze.rees46_sftp append | silver.rees46_events/active revision → G03 SFTP batch/incremental analytics only | REE-001…036;10 minuteADF discovery; inferred/hinted native schema/addNewColumns; persistent C/S and source_deliveries certificate/ACK | Late/missing/partial/revision/replay/native checkpoint isolation; DEV-RET-01, source/feature-window/Blob/Jobs costs |
| OFF SFTP | Original full canonical/exception evidence; SQLite source catalog/journal; FULL/DELTA operation identity and SP-01 native JSON wrapper | Actual SFTP/ADF/native JSON with payload/after VARIANT hints→bronze.off_products append; no receiver reader | off_product_current + explicit exceptions → G07OFF domain; no invented cross-source product key |30D03categories mapped OFF-A41-001…030;1minuteactiveADF; registered native envelope/open payload, source journal + native C/S + logical delivery state | Complete source range/multiplicity, conflict preservation, held continuity and native replay; DEV-RET-01/catalog/shared host/Blob footprint |
| Amazon metadata SFTP | Original10 native Parquet shards/full keyset; committed native source journal DELTAs, explicit lifecycle/tombstones | Actual SFTP/ADF/native Parquet cloudFiles FULL/DELTA scopes→bronze.amazon_metadata append; no conversion | amazon_product_current → G04enrichment/G07Amazon; exact case/types/nested structures | AMZ-DQ-001…036;1minuteactiveADF; native inferred schema/hints/registered changes; persistent C/S and exact source operations/ACK | Native state/partial copy/schema/target reconciliation and isolated replay; source catalog/validation memory/DEV-RET-01 |
| Amazon reviews SFTP | Original FULL/day allocation/global ordinal, repeated original reviews retained; isolated fixture edits/retracts; SP-01identity/image | Actual SFTP/ADF/native JSON explicit schema/rescue→bronze.amazon_reviews append | amazon_review_occurrences → G04; metadata association labelled/pending, no inner-join loss | REV-001…036;1minuteactive/15 minute recovery-only; source ordinal/native C/S and whole-delivery proof | Same source identities and day/full disjointness on replay; native schema/corrupt/target repair; DEV-RET-01/envelope storage costs |
| Campbell SFTP | Seven original station files/four header families; original station-day/empty/revision/v2 packages, SP-01bounded native parts and joinable ordinal map | Actual SFTP/ADF/native text wholeText cloudFiles→minimal TOA5 column transforms→bronze.campbell append | campbell_observations → G05; native NTZ/header/unit/sentinel semantics retained | DAT-DQ-001…040;1minuteactiveADF; text fixed ingestion schema, registered family transformations; station/epoch/native C/S + source_deliveries | Exact header/zero-file/row-map/native restart/isolated replay; no SDK reader; DEV-RET-01/bounded parts/Jobs costs |
| Open Prices REST | Pinned export profiling only; actual REST fresh discovery/bootstrap/tail/weekly re-observation, no complete snapshot/CDC claim | ADF→D10serialized Python→API-only immutable evidence/ledger/receipts→receipt Auto Loader/guarded bronze.openprices_capture | openprices_observations → G06; distinct observed history/latest observed, qualified currencies/coverage | OPR-DQ-001…045 correct scopes; independent ADF15/5 minutes; one acquisition_ledger frontier plus separate receipt checkpoint; no API credential fabricated | D10attempt/cooldown/CAS/receipt/window/uncertain-commit repair retained; actual public conformance/runtime and protected365/400day funding/rights gates separate |
| Independent REES46 Kafka | Same seed, separate source identity/transactional producer60×; two source topics/real Registry; DEV-KAF-01 broker availability deviation | Real private KRaft/Registry→read_committed native10s Structured Streaming→bronze.rees46_kafka append; no SFTP/Copy/Auto Loader | **silver.rees46_kafka_events → G08gold.rees46_kafka_activity_daily**; original repetitions versus physical republish, late groups, exact distinct user/session measures; no default SFTP UNION | KAF-DQ-001…046; D11producer/Bronze unchanged; separate native capture/business C, cached actual writer schemas, native30s business query; ADF lifecycle only | D11 source/physical reconciliation, native/isolated B/S/G replay/cutover, process restart and explicit no-HA; shared host/disk/closeout costs, no managed eCKU default |

### 22.2 Final completeness and consistency audit

| Audited requirement | Concrete result |
|---|---|
| Native SFTP | Five actual native Auto Loader adapters: CSV/JSON/Parquet/JSON/text; seven compatible C/S scopes, persistent across runs. No executor Blob SDK/default/fallback/file-discovery framework. Native append chosen for all five; no generic SFTP commit markers. |
| Identity/fidelity | SP-01 source-side ordinal/image or ordered-content metadata joins; original seeds/chronology/repeats/old payload schemas retained; delivery fingerprint changes explicit, never fabricated ordinals. |
| Schema evolution | All five modes, inference/explicit/hints/rescue/corrupt/Delta/reader/consumer boundaries and20real publisher/native-format experiments; fixed text/VARIANT limits explicit; no automatic business approval. |
| Physical tables | Preserve all original31 and add only silver.rees46_kafka_events/gold.rees46_kafka_activity_daily: **10Bronze+10Silver+8Gold+5 control=33 normal managed Delta tables**. Temporary isolated test/recovery targets are declared, not extra normal layers. |
| Git/runtime metadata | Eleven Git static categories and ten entity routes; exact existing five managed Delta controls/keys/writers/state guards; no generic offsets DB/service or new DQ catalogue table. |
| DQ | Exact authoritative ID families: HM36/REE36/AMZ36/REV36/DAT40/OPR45/KAF46 =275 identified; OFF30categories/project mappings separate. Routing/traceability/summary agree; no invented or reduced rules. |
| Frequencies | REES SFTP10 minutes; OFF/metadata/Campbell1minuteactive; reviews1minuteactive/15 minute recovery; PGarticles15 minutes; API15/5 minutes; Kafka native Bronze10s/business30s. Other clock/AM-04 source policies retained. |
| Kafka independence | Real producer/broker/Registry/native D11append plus independent physical Silver/G08; separate source-generation/history/publication/readiness and logical/physical duplicates, no combined distinct population. |
| Hosting/storage/retention | Two accounts/same subscription; shared temporary private KRaft RF1DEV explicit versus enterprise RF3/minISR2. No SFTP365day WORM; active/recovery/explicit-closeout safeguards; API D10protected evidence separate. |
| Spark/finance | Minimal control/smoke; two-worker distributed qualification; zero default4/8 worker reservations; active/residual VM/DBU/source/SFTP/ADF/disk/network/API-retention formulas; actual credit/quote/permissions unverified. |
| Preserved processing/reliability | H&M genuine database/journal/WAL/SCD2/as-of, D10RESTledger/receipt protocol, D11producer-to-Bronze, original G01–G07, independent G08 and all native progress authorities remain. |
| Failure evidence | All118prior future risks retained/reconciled;20AL+10KS+2CL additions =**150 future scenarios, all NOT EXECUTED**. Prior D11local evidence is not rerun/cloud qualification; RF3failover is not a single-broker pass. |
| Strict scope | Only architecture.md changes. No implementation/config/contract/Job/test/resource execution, provisioning or cleanup occurs. Original work/files remain in place; actual closeout/coding/billable activation require separate authorization. |

**Approval recommendation:** approve the complete targeted A4.2 architecture and explicit SP-01/receiverR4/KSG-01/DEV-RET-01/DEV-KAF-01 overlays for subsequent implementation. No routine design choice is returned to the user. Account-specific funding/actual identities/permissions/licensing, approval of material source/DEV retention/availability amendments, genuinely unverified native runtime/protocol compatibility and actual performance/recovery evidence remain external or execution gates.

**Readiness:** specification ready for architecture approval; implementation/tests/deployment are not performed. Resource activation still needs actual quotes/balance/expiry/quota, permitted billing/rights/credentials and separate authorization. API full cleanup before its retained D10horizon would additionally require explicit D10-DEV-RET-02 approval; it is not silently selected.

**Architecture decision:** preserve A4.1 and apply only these targeted native Auto Loader, source identity, independent Kafka B/S/G,33table, corrected DQ/frequency, DEV hosting/retention and minimal-compute changes. The four source patterns remain technically distinct. A source profile, prior local test, service feature statement or successful document commit does not establish runtime qualification.

**Approval boundary:** submit this complete architecture for review. Implementation, tests, simulators, pipelines, provisioning and deployment remain stopped. Only the authorized architecture.md recording is performed.

## 23. Official technical references

These references support service behavior; project policies, source facts and all actual-runtime status are identified separately above.

- [Azure Blob SFTP support/authentication/billing](https://learn.microsoft.com/en-us/azure/storage/blobs/secure-file-transfer-protocol-support)
- [Azure Blob SFTP limitations/rename/fsync](https://learn.microsoft.com/en-us/azure/storage/blobs/secure-file-transfer-protocol-known-issues)
- [Storage feature matrix including HNS/SFTP/WORM](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts)
- [Immutable Blob storage](https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-storage-overview)
- [ADF SFTP connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-sftp)
- [ADF Web activity/authentication](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-web-activity)
- [ADF Databricks Job activity—evaluate selected compute support](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-job)
- [Databricks Jobs run-now API](https://docs.databricks.com/api/workspace/jobs/runnow)
- [Databricks VNet injection](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/vnet-inject)
- [Databricks Structured Streaming production guidance](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/production)
- [Databricks foreachBatch semantics](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/foreach)
- [Databricks native Kafka connector](https://learn.microsoft.com/en-us/azure/databricks/connect/streaming/kafka/)
- [Auto Loader native data discovery](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Auto Loader production/availableNow and monitoring](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/production)
- [Auto Loader options and soft file limits](https://learn.microsoft.com/en-us/azure/databricks/spark/api-options)
- [Auto Loader explicit schema/evolution behavior](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema)
- [ADLS hierarchical namespace and rename](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
- [Databricks checkpoint constraints](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/checkpoints)
- [Databricks AQE](https://learn.microsoft.com/en-us/azure/databricks/optimizations/aqe)
- [Flexible Server logical decoding/slots](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical)
- [Flexible Server pricing model](https://azure.microsoft.com/en-us/pricing/details/postgresql/flexible-server/)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Confluent Cloud cluster/SLA choices](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)
- [Confluent Cloud topic settings](https://docs.confluent.io/cloud/current/topics/manage.html)
- [Confluent Kafka Python transactions/client API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [Event Hubs Kafka transactions public-preview status](https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-transactions)
- [Confluent public pricing](https://www.confluent.io/pricing/)
- [Microsoft Azure Retail Prices API](https://prices.azure.com/api/retail/prices)
- [Open Prices official prices view/filter/pagination source](https://github.com/openfoodfacts/open-prices/tree/main/open_prices/api)
- [Delta MERGE semantics](https://learn.microsoft.com/en-us/azure/databricks/delta/merge)
- [Delta schema evolution](https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema)
- [Azure promotional credit/spending guidance](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/avoid-charges-free-account)


- [Auto Loader native type-widening behavior/runtime](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/type-widening)
- [Native JSON/VARIANT ingestion and limits](https://learn.microsoft.com/en-us/azure/databricks/ingestion/variant)
- [Native text reader](https://learn.microsoft.com/en-us/azure/databricks/query/formats/text)
- [Apache Kafka broker properties](https://kafka.apache.org/39/configuration/broker-configs/)
- [Schema Registry durable native state](https://docs.confluent.io/platform/current/schema-registry/installation/deployment.html)

## Final decision summary

**A4.2** retains one DEV/eight independent integrations/four real interfaces. All five SFTP sources use actual native Auto Loader and persistent C/S after source publication/ADF acquisition; no executor SDK ingestion remains. Original31 managed Delta targets are preserved, with only Kafka Silver/G08 added: **10Bronze/10Silver/8Gold/5 control=33**. SFTP and Kafka products are independent.

Two storage accounts stay under the same subscription. Temporary SFTP data has no365day WORM; active/recovery evidence and explicit closeout replace it under a declared DEV contract deviation. API D10retention remains separate. Real temporary single-broker KRaft/Registry reuses the shared source host under explicit RF1DEV/no-HA limits. Start minimal, use2workers for distributed evidence and scale only after measurements/approval.

The architecture is ready for review, not automatically approved or runtime/financially certified. All150 future scenarios remain NOT EXECUTED. **Stop after recording architecture.md; implementation, paid provisioning and actual cleanup require separate explicit approval.**
