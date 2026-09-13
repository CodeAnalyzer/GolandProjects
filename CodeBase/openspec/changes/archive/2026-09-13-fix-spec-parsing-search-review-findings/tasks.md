## 1. Markdown parser line boundaries

- [x] 1.1 Add failing parser tests for scenario blocks containing blank/comment/extra Markdown lines and verify `go test ./internal/parser/openspecmd -run TestParseSpecFile` demonstrates the incorrect `LineEnd`.
- [x] 1.2 Change scenario closing to use the actual source line before the next structural header or EOF, remove count-based boundary calculation, and verify `go test ./internal/parser/openspecmd -run TestParseSpecFile` passes.
- [x] 1.3 Add a failing multi-line delta body test asserting `LineStart` and `LineEnd` before the first scenario, fix delta finalization to use the actual preceding line, and verify `go test ./internal/parser/openspecmd -run TestParseDeltaSpec` passes.

## 2. Shared LSA path and generation model

- [x] 2.1 Add a single config helper that resolves empty, relative, and absolute `lsa_model_path` values against the active config directory; add table-driven tests and verify `go test ./internal/config` passes.
- [x] 2.2 Extend serialized LSA model/state metadata with generation/fingerprint compatibility, treating old files as generation `legacy`; add round-trip and legacy-read tests and verify `go test ./internal/specfts` passes.
- [x] 2.3 Update indexer and search service to use the shared resolved model/state paths, add a regression test with a working directory different from the config directory, and verify the relevant `internal/indexer` and `internal/specsvc` tests pass.

## 3. Generation-aware LSA storage and migration

- [x] 3.1 Add generation columns and generation-aware indexes for `spec_vocab` and `spec_embeddings`, migrate existing rows to `legacy`, and verify schema integration tests observe the columns, defaults, and indexes.
- [x] 3.2 Replace destructive clear/insert helpers with transactional insertion of one generation, generation-filtered loading, and cleanup of inactive generations; add store integration tests proving rollback leaves the active generation intact.
- [x] 3.3 Implement same-directory temporary model writing plus atomic replacement on Windows and Unix semantics, add tests for successful replace and pre-replace failure, and verify `go test ./internal/specfts` passes.
- [x] 3.4 Implement the prepare → DB generation commit → atomic model switch → atomic state write → stale-generation cleanup protocol and add failure-injection tests for each boundary proving the previous model/generation remains queryable.

## 4. LSA invalidation and retry behavior

- [x] 4.1 Move cleanup of disappeared files before spec post-processing while preserving cleanup statistics and cancellation handling; add/update runner tests and verify `go test ./internal/indexer` passes.
- [x] 4.2 Remove the pre-fingerprint `corpusDelta == 0` shortcut so every enabled LSA pass loads the deterministic corpus and compares its fingerprint before deciding; verify an unchanged corpus skips SVD in an indexer test.
- [x] 4.3 Add regression coverage for deleting a spec file and verify the same update publishes a generation without the deleted capability when the retrain threshold is met.
- [x] 4.4 Persist `retry_fingerprint` before retraining, force retrain while the marker is present, clear it only after successful publication, and add regression coverage proving a failed attempt followed by a zero-change update retries independently of the threshold.

## 5. Weighted full-text vectors

- [x] 5.1 Centralize weighted `tsvector` expressions for capability, requirement, scenario, and usecase using A for names/titles and B for descriptive bodies; update per-file vector generation and verify store tests assert the expected weights.
- [x] 5.2 Add a versioned schema migration that backfills all existing spec search vectors exactly once and verify rerunning `InitSchema` is idempotent.
- [x] 5.3 Add a PostgreSQL integration test with equal-frequency matches in name and body and verify the A-weight name/title hit ranks above the B-weight body hit.

## 6. Technical identifier search

- [x] 6.1 Refactor trigram search into index-compatible artifact branches and a `spec_code_mentions` branch with case-insensitive exact match before similarity fallback; verify SQL integration tests find an identifier embedded in a long scenario.
- [x] 6.2 Map mention hits back to capability/requirement/scenario/usecase, preserve product/level/line/snippet fields, and deduplicate by `(level, entity_id)` while retaining the best rank; verify tests cover every source type and duplicate exact/trigram hits.
- [x] 6.3 Inspect the PostgreSQL plans for representative artifact and mention searches and verify the matching `gin_trgm_ops`/lower-name indexes are used rather than full scans on the test corpus.

## 7. Search and by-code error/result contracts

- [x] 7.1 Add tests for missing model in `both` and `semantic` modes plus model corruption and DB query failure; implement explicit unavailable metadata only for `both` + model-not-found and propagate all other errors.
- [x] 7.2 Replace `by-code` snippet selection with `CASE source_type` and non-empty fallbacks; add integration fixtures for capability, requirement, scenario, and usecase and verify each hit contains source-specific context and the mention file path.
- [x] 7.3 Run `go test ./internal/specsvc` and the PostgreSQL-backed `internal/specsvc` integration suite, verifying exact, semantic, technical-name, error, and by-code scenarios pass.

## 8. CLI and MCP documentation consistency

- [x] 8.1 Update README search examples to use canonical CLI `--query`, removing erroneous `--text` references; verify every changed example matches the current Cobra flags.
- [x] 8.2 Verify the `query/spec-search` delta uses canonical MCP `query` and CLI `--query` without changing CLI/MCP code; leave other capabilities and archived historical changes outside this change as explicitly requested.

## 9. Final validation

- [x] 9.1 Run `gofmt` on changed Go files and verify `go test ./internal/parser/openspecmd ./internal/config ./internal/specfts ./internal/indexer ./internal/store ./internal/specsvc ./internal/mcp ./cmd` passes.
- [x] 9.2 Run the affected PostgreSQL integration suites with the `integration` build tag and verify generation migration, weighted ranking, technical identifier lookup, LSA retry, and `by-code` context scenarios pass.
- [x] 9.3 Run `go test ./...` and `go vet ./...`, resolving any regression introduced by the change.
- [x] 9.4 Run `openspec validate fix-spec-parsing-search-review-findings --strict` and verify all proposal, delta-spec, design, and task artifacts remain valid after implementation-related documentation updates.
