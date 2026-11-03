# Project conventions

- Prioritize native SQLx queries, exact Doris values and trustworthy application
  ingestion. Checked macros and migrations are development tools; identity,
  drain, recovery, managed Routine Load and Kafka observations are specialist
  administration. Do not expand this driver into an ORM, scheduler, general
  Kafka client, CDC connector or full Doris control-plane SDK.
- Keep five packages. Expose basic CLI through `cli`, Doris administration
  through `cli-admin`, and independent read-only Kafka observations through
  `cli-admin-kafka`. Advanced commands live under `admin`; do not keep old
  command aliases. Basic CLI must not enable maintenance HTTP or Kafka wire.
  Doris-only diagnosis must compile independently of Kafka. Share read-only
  migration history, configuration and result semantics across these features.
- Teach existing-table ingestion, stable application batch identity, rejected
  rows, and accepted/committed/visible/unknown outcomes before recovery tooling.
  Keep source inputs immutable during an upload; redirects must not silently
  switch a file generation. Never interpret missing history as proof that a
  write did not happen. Examples preserve their inspection data on failure.
- Keep user guides in paired English `.md` and Simplified Chinese `.zh-CN.md` files.
  English is the default and both languages are maintained in the same change;
  never ship a shortened translation or retain old `.en.md` compatibility copies.
  Every page has reciprocal language-name links. Use matching stable `doc-`
  anchors, section order, tables and identical fenced code/commands/data.
  Keep document links in the current language; code/API/external references are shared.
  Rustdoc and CLI messages remain canonical English; do not translate identifiers,
  flags, SQL, JSON keys, reason codes or sample data. Check facts and scope manually:
  structural parity cannot prove translation accuracy.
- Root README is the checkout quickstart; docs/README indexes task-specific guides.
  Keep package READMEs concise and self-contained inside each archive, with both
  languages included and no relative links escaping its package. Keep installation
  explicit about the published registry version and the source checkout used for
  development. Compile copyable Rust examples and validate documentation
  pairs/anchors through scripts/check-docs and test-docs.
- Maintain the paired architecture/task atlas as user documentation. Distinguish
  build-time metadata, runtime SQL, HTTP payloads, tool control and package
  dependencies with labelled directions and separate legends. A dependency graph
  is not an execution trace. Keep feature/topology facts aligned with Cargo, local
  guide links in the current language, and static/text alternatives to interaction.
  Final SVG/HTML/CSS/JS assets belong to the guides; screenshots, browser profiles,
  visual review notes and temporary rendering outputs stay on the external volume.

- Expose one public Doris driver entry point, `sqlx-doris`, backed by five aligned
  packages: the facade, core, proc-macros, operations and CLI. Internal packages
  depend on core, never on the facade. Keep versions exact and aligned.
- Keep SQLx database identity, native types, binding and migration trait implementations
  in core. Put schema/MV/HTTP helpers and maintenance workflows in ops; put clap,
  prepare/Cargo integration and human/JSON rendering in CLI. Keep one thin binary
  in the public package. Never add a second query facade or compatibility wrappers.
- Gate Doris description serialization and compiler dependencies behind
  macro-support/query. SQLx 0.9's offline feature controls the required
  Executor::describe method, so retain it to keep the trait surface consistent
  with upstream macros. Verify isolated resolved features, including macros-only,
  macros+migrate and upstream migrate! without Doris query macros. An inactive
  query macro package must have no normal/build compiler dependencies.
- Keep shared ops migration history read-only. SQLx runner preparation belongs to
  CLI integration, while native migration and UUID lock primitives stay in core.
  Reject invalid helper IDs before acquiring a connection; timeouts and polling
  intervals must not overflow clocks or start another poll after the deadline.
- Write for human readers: four-space Rust indentation, normal spacing, one meaningful
  step per line, and expanded macro control flow and JSON fields. Use rustfmt and
  Black, but manually format macro bodies, TOML and generated source templates.
  Keep formatting-only work separate from behavior changes; do not alter SQL literals
  while reformatting. Include standalone Rust fixtures in the formatting gate.
- Maintain one standalone SQLx Doris database identity: `sqlx-doris`, `Doris`, `DorisConnection`,
  `DorisPool` and `DorisConnectOptions`. Follow SQLx traits and query conventions.
- Do not vendor, patch, rename-publish, or depend on sqlx-mysql. Do not maintain a
  MySQL backend, backend selector, or dual-database compatibility layer.
- Disable generic SQLx default features; never enable its database driver
  features. Audit the active normal/build graph, since Cargo.lock may retain
  upstream optional driver entries without compiling them.
- Implement public Doris protocols and operations-library read-only Kafka APIs used
  by the CLI; never modify/build/deploy server source.
- Preserve typed/tokenized safe binding, exact values and Doris semantics. Never
  replay writes or claim an uncertain commit failed safely.
- Require SQLX_DORIS_EXTERNAL_VOLUME to name a separately mounted external volume.
  Never embed machine-specific paths in source or documentation.
- Run build/test/package tooling via ./scripts/dev. All target directories, caches,
  downloads, logs, temporary files, Docker VM disks and Doris data must remain on
  the external volume. Stop if it is unavailable; verify VM/data storage before
  starting or pulling database fixtures. Keep .cargo/ ignored. Hosted CI has no
  external volume, so keep it to documentation, link and formatting checks; run
  builds, tests, packaging and live contracts only on external-storage runners.
- Keep only user-facing documentation (installation, usage, API, compatibility,
  testing, performance and release notes). Store iteration notes, research,
  intermediate plans and review reports outside this repository.
- Validate performance with reproducible workloads and state measured limits.
  Missing live-test prerequisites must fail; ignored tests are not passing tests.
- Record resource refusals as failures. Repeated regressions and performance runs
  may use separately restarted, exclusively owned test fixtures with data retained;
  verify unchanged effective configuration, quotas and mounts. Preserve original
  failures and uncertain work; never restart shared services or raise quotas to pass.
- Scope MV task assertions to the fixture's exact parent IDs, since task metadata
  includes other databases. BUILD DEFERRED does not disable periodic scheduling;
  use an explicit future STARTS from the server clock when testing an active
  scheduler without tasks. Never hide tasks belonging to the tested parents.
- Query macros must reuse the Doris driver and SQLx macro infrastructure, retain
  honest metadata/type checking, support renamed dependencies and offline builds,
  and never execute business SQL during compilation.
- Migrations must persist dirty state before DDL, wait for asynchronous schema
  changes, use real exclusion or explicitly configured serial execution, and
  never automatically take over stale locks or replay uncertain statements.
- Use a fresh temporary registry URL for each workspace package verification;
  unchanged unreleased versions must not resolve to cached internal archives.
- Keep CARGO_BUILD_BUILD_DIR external as well as CARGO_TARGET_DIR. Verify custom
  MSRV/package build paths before launching Cargo.
- Migration unlock/recovery must serialize owner-check plus DROP through the
  transition gate. Never auto-take-over an interrupted gate. A ping/flush that
  releases a pending migration lock must retire its connection.
- Both main and transition locks use persistent parent tables and the same
  validated anchor/exclusive-range UUID partition protocol. Acquisition adds
  a fresh owner partition; release/recovery drops only that owner's partition.
  Never DROP either reusable parent, restore a retired owner, convert an old
  comment-only lock, or retry an uncertain removal. A failed main DROP must
  retain the transition owner and retire the connection for explicit review.
- Preserve caller expressions and record paths when adapting SQLx macro output;
  only framework paths may be rewritten. Use native Doris column descriptions
  without copying base-table NOT NULL flags into outer-join result inference.
- Recovery of an interrupted transition is an explicit maintenance action, never a timeout
  takeover. Require exact gate/main owners, reject nonterminal schema jobs,
  preserve the main lock and dirty/applied history, and retire the connection
  once removal starts. Use a persistent gate with an anchor partition and a
  fresh UUID-named owner partition in a fixed exclusive range. Atomically ADD
  that range for exclusion; DROP only the exact owner's partition, never the
  reusable parent table. Never reuse an owner name or automatically convert an
  old gate format. Validate partition metadata and fail closed on unknown layouts.
  This fences delayed main and gate cleanup after the final owner check. Old business SQL
  still requires externally serialized maintenance with
  all old migration/unlock/recovery actors and commands stopped.
- Applications use upstream `sqlx` for generic APIs, FromRow and migrate!;
  `sqlx-doris` supplies Doris driver types, checked query macros and the CLI.
  Do not re-export the SQLx namespace, generic query APIs, FromRow or migrate!.
  Doris query macros require a direct upstream dependency named `sqlx`, matching
  upstream generated-code conventions. Do not promise upstream query-macro or
  CLI registration for Doris.
- Keep internal runtime/operations/CLI code on exact-version sqlx-core; applications
  use upstream sqlx directly. The facade exposes an explicit Doris API list and
  optional ops/CLI dependencies, without generic SQLx or pool-lifecycle forwarding.
  CLI preparation invokes the application's Cargo build and must not enable the
  driver's query macros. Operations take explicit requests and per-call state;
  never use clap, process exits, printing or global command output from ops.
  Audit default, migrate-only, CLI-only and minimal upstream SQLx consumers in
  isolated manifests so workspace dev features cannot hide dependency leaks.
- Recovery diagnostics must distinguish no DROP sent, uncertain DROP outcome,
  and acknowledged DROP with failed state verification. Fault tests must permit
  an already submitted DROP to commit after future/process cancellation and
  verify no replay, preserved history and retired connections.
- Check ownership before migration dirty markers, business statements, async
  job polling and completion markers. Lost or unverifiable ownership retires
  the connection; preserve history and successor locks. These are cooperative
  checkpoints, never an atomic fence between a metadata read and business SQL.
- Do not automatically KILL sessions from persisted numeric connection IDs:
  IDs are FE-local and restart-reusable, with no owner-conditioned KILL API.
  Query cancellation does not stop future queries; killing a socket does not
  prevent reconnects or undo commits/submitted jobs. Never infer safe takeover
  from KILL success, processlist absence or client-side ownership checks. Keep
  explicit exclusive maintenance and external runner/credential isolation.
- Dedicated identity management is an ops workflow exposed by the CLI: generated never-reused names,
  private external credential files, database-scoped direct grants and separate
  administrator credentials. Do not grant ADMIN/GRANT to runners or retire
  arbitrary/shared users. Validate managed provenance, exact host identities,
  permission layout and supported topology/authentication before mutation.
- Automated identity cutoff is currently limited to verified Doris 4.1.5 on
  one live master FE with built-in authentication/authorization; fail closed
  for multi-FE, external auth and unknown metadata. Retirement is preview-first,
  explicit maintenance only, never automatically recovers locks/history or
  replays ambiguous account DDL. Persist credentials before provisioning and
  preserve the breadcrumb on failure. Never log passwords or overwrite secrets.
- Account deletion blocks future authentication/privilege checks but admitted
  transactions can still COMMIT and submitted schema jobs can complete. Require
  serialized account administration, no identity recreation/regrant, explicit
  old-runner correlation and transaction/job/data reconciliation before lock
  recovery. Do not claim account-to-lock ownership from a preview alone.
- Drain checks are ops observations exposed by the CLI on the verified identity topology:
  retired-name sessions across databases, all current-database transactions
  (COMMITTED is nonterminal), and COLUMN/ROLLUP jobs. Track observed work until
  exact terminal states; missing history, unknown metadata, errors and deadlines
  must never imply completion. No auto recovery, reconnect, KILL or transaction abort.
  The CLI may explicitly abort only a verified active BROKER LOAD's exact positive
  registered TransactionId with matching JobId/ListenerId, label, creation and
  BATCH_LOAD_JOB source; never arbitrary SQL, unbound, Routine Load or name fallback.
  Require stopped actors/admission, serialized administration and reconciliation
  of other async work before guarded recovery; an empty snapshot is not an atomic
  server fence. Never offer COLUMN cancellation by JobId on Doris 4.1.5: its handler
  ignores that ID and may cancel a successor. Explicit ROLLUP cancellation must
  preview first, use only the exact observed JobId, never retry uncertain CANCEL,
  and verify a terminal state without changing locks/history.
- Every CLI main/transition recovery must freshly check database transactions and
  the documented async scope, even without --retired-identity. That option also
  verifies retired-name absence/sessions; no name means no session/credential
  attestation. Keep core recovery APIs explicit maintenance primitives.
- Async drain supports verified shared-nothing 4.1.5 only (deploy_mode empty/local,
  cloud_unique_id empty). Inspect SHOW LOAD plus legacy pending/loading counters,
  SHOW ALL ROUTINE LOAD, and paired mv_infos/jobs/tasks metadata by database and
  exact parent IDs. PAUSED Routine Load is nonterminal; active ON COMMIT/scheduled
  MVs block even with no tasks. Read transactions again after producer checks.
  Paused MV schedulers are only a current disabled-scheduler observation, not a
  durable fence against manual refresh, resume/reconfiguration or restart.
- Do not wrap name-only LOAD cancellation, Routine Load stop or MV scheduler
  pause as exact-ID operations: labels/names can resolve successor generations.
  MV task cancellation uses the verified generated inner_mtmv_<MvId> parent name
  plus exact TaskId through public CANCEL TASK, preview-first and maintenance-only.
  Never reuse/spoof generated job names or retry uncertain cancellation. Validate
  parent/view/task identity, preserve dirty/history and successor generations,
  and report terminal status without claiming rollback or complete backend drain.
  Removed/orphaned work, history retention, hidden server metadata, other job
  classes and admitted/unregistered commands remain explicit limits.
- LOAD abort is preview-first and requires exact --job-id/--transaction-id plus
  --maintenance --force. Use the public transaction-ID-only HTTP 2PC abort on the
  verified shared-nothing 4.1.5 one-master-FE/one-live-BE topology. Attest stable
  cluster/endpoints and no metadata restore/rebinding/reuse; verify SQL/HTTP FE
  identity and node epochs, validate the registered BE redirect/path, discard
  redirect credentials and independently authenticate. No BE override, redirect
  following, name-based CANCEL or retries. Recheck binding before submission and
  require exact transaction ABORTED/job CANCELLED after ACK. Distinguish unconfirmed
  submission and ACK with failed verification, retain locks/history on all paths.
  Committed transactions cannot be undone; Routine Load transaction abort does not
  stop its producer. Public quotas/configuration are not generation fences.
  reqwest and percent-encoding belong to the optional ops maintenance feature; default
  and migrate-only consumers must not acquire HTTP dependencies from this feature.
- Managed Routine Load is a conditional non-reuse protocol, never an exact-ID STOP
  API. Only the CLI creates fresh canonical v4 sqlx_rl_ UUID names; reserve each
  atomically with a permanent _sqlx_rl_ ordinary view before one CREATE, without
  IF NOT EXISTS/OR REPLACE, adoption, caller-selected names, replay or cleanup.
  Persist a private external receipt first. Failed/uncertain creation consumes
  its name forever; inspect the original receipt, never repeat CREATE.
  Create through a verified dedicated runner credential file and a separate
  administrator connection, checking matching origin/options/database and managed
  provenance. Bind the reservation to cluster/database, table and creator; verify
  the actual Routine metadata user, target, source and retained unique history.
  Stop only after creator retirement, with explicit maintenance and exclusive
  managed-namespace attestation. Recheck reservation, JobId and creation identity,
  submit one UUID-name STOP, distinguish uncertainty from ACK verification failure,
  and preserve reservations/locks/history on every path. Test late delivery after
  disconnect, killed CLI and future timeout against fresh UUID successors.
  Every creator must obey the protocol indefinitely. Doris does not enforce our
  view reservation on raw Routine CREATE: privileged bypass, reservation removal,
  restore/ID or name reuse and endpoint/database rebinding remain explicit limits.
  Include a native bypass regression; never advertise protection against it.
  Do not mutate global quotas or claim LOAD privilege separates CREATE from STOP.

- Target the verified current Doris baseline (4.1.5) and later versions only after
  semantic revalidation. Do not add older-version support or regression matrices.
- Routine recovery uses private external receipts for location, never authority;
  validate current cluster/database/reservation and any historical JobId binding.
  Persist an immutable confirmation sidecar only after CREATE acknowledgement
  and verification. Missing history/receipt confirmation never means never-created,
  drained or safe to recover. Preserve a potentially stale job snapshot's read phase.
  Routine JSON is one versioned stdout object with no raw SQL/server errors/secrets;
  use stable exits 0/2/3/4/5/6/7 and let submitted/acknowledged mutation phases
  override later input/read failures. Never replay uncertain CREATE/STOP.
- Kafka data contracts must use actual acknowledged broker messages and a DUPLICATE
  KEY table. Anchor in-flight races in observed PREPARE ListenerId/JobId and verify
  terminal transactions, rows and numeric Progress. SHOW Progress is a reported next-offset-minus-one cursor (or sentinel),
  including configured starts before any commit; never label it consumption proof.
  Use plus one per partition only after drain, data and Kafka generation/range
  reconciliation; never increment sentinels or take a scalar/global maximum.
  Test both commit/STOP orders; no automatic offset transfer or replacement.
- Scope physical-volume attestation to one file operation group, never globally;
  recheck the live mount/device for every receipt/credential access, preserve
  private-file permissions and opened-inode validation, and reject nested or
  changed devices. Do not invoke a physical-disk probe separately for each
  file in one Routine command.
- In-flight Routine tests require matching FE PREPARE ListenerId/JobId and the
  BE-acknowledged task's JobId/TxnId, positive BeId and execution-start time.
  FE-only PREPARE also exists after resource refusal and is not BE admission.
  Keep resource failures visible with row/job diagnostics; never count them as
  successful data/STOP races or change production quotas to make tests pass.
- Kafka topology/retention tests must bind partition IDs to actual broker offsets,
  verify exact DUPLICATE KEY ID multiplicities again after STOP/transaction drain,
  preserve retained committed data, and
  expose wrong-vector/stale-snapshot loss or replay. Cover explicit versus dynamic
  partition discovery, expired prefix, topic-ID replacement and real tail regression
  under an owned stale-replica election. Only stop/start fault brokers whose exact
  Docker IDs, nonce labels, node/cluster IDs, image and external volumes match the
  private fault manifest; never stop the shared controller or alter global quotas.
  A numeric cursor and in-range offset do not prove an unchanged Kafka history.
- Prepare advisory file locks must explicitly unlock on guard drop while retaining
  their inode. A concurrent fork may inherit a descriptor until exec; test prompt
  release with a cloned descriptor and verify that closing it cannot unlock a successor.
- Kafka snapshot/preflight belongs to ops and is exposed by the CLI; it is server-read-only: Metadata with auto-create
  disabled, consumer ListOffsets and OffsetForLeaderEpoch, ApiVersions and optional
  TLS/SASL authentication only. Never Produce, Fetch, join groups, commit/reset offsets,
  transfer Doris Progress, create replacement jobs or recover locks from its results.
  Scope history evidence to a trusted private external checkpoint captured before
  source changes. Check cluster/topic IDs, full partition sets, ranges, retained epochs
  and observation-time metadata changes; missing/expired/ambiguous epoch evidence
  never means unchanged history. No atomic-snapshot, consumption or recovery claim.

- Every CLI namespace uses global --json and the single sqlx-doris-cli-result-v1
  schema; do not retain per-domain schema adapters. Controlled JSON results are
  one stdout object, empty stderr, no raw SQL/server errors/secrets. Keep help/version
  text. Use stable exits 0/2/3/4/5/6/7; mutation submission/acknowledgement overrides
  later failures. Never derive submission phases by matching error-message text.
- diagnose is an ops read-only composition exposed by the CLI: receipt location/binding, exact lock
  owners, migration history/source checks, retired identity/drain and optional Kafka
  preflight with explicit candidates. Never create metadata tables, repair history,
  retire identities, cancel work, recover locks or infer/transfer/reset offsets.
  Report component failures and coverage gaps, observation drift and non-atomic scope;
  even exit 0 never certifies takeover, data reconciliation or Kafka-to-Routine binding.

- CLI migration history reads must use DorisConnection::migration_table_identifier,
  the same quoted/current-database-qualified name parser used by the driver.
  Never substitute a simpler ASCII regex or create a state table from info/diagnose.
- Diagnose reason codes must come from typed failures and named observation stages,
  never raw error-message matching. Keep all independent conflicts, partial work,
  per-component evidence and concrete read-only follow-ups; escape server text.
- Sustained workloads and randomized contracts must be reproducible by seed/settings,
  validate returned data and retirement/no-replay behavior, bound their own memory,
  and keep samples/corpora/builds on the external volume. Resource refusal is failure.
