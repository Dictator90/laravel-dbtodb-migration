# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Named migrations through `dbtodb_mapping.migrations`.
- Default migration fallback: `migrations.default`.
- `--migration` / `-m` CLI option.
- Migration-level `source` and `target`, so simple runs do not need `--source` / `--target`.
- Short table syntax: `source_table => [target_table => [source_column => target_column]]`.
- Full table syntax with `source`, `targets`, `filters`, `transforms`, `upsert_keys`, `operation`, runtime options.
- Ordered migration `steps`.
- End-to-end stress test (`tests/Feature/DbToDbStressTest.php`) that loads 30,000 rows in `legacy_users` and 30,000 in `legacy_orders` (plus profiles and event sources), exercises the full feature surface — keyset pagination, multi-step ordering, fan-out (1 source → users + admins + audit_log with filters and static columns), fan-in (legacy_users + legacy_orders → audit_log, events_a + events_b → unified_events with static `origin`/`source_table`/`event_type` columns), upsert / insert / lookup across connections, JSON / bool / int casts, map / replace / default / trim / lower / upper transforms, nested filters and `in` lists — and prints throughput statistics (rows, duration, rps, peak memory).

### Changed

- Config is now migration-centric.
- CLI startup output and profiling include selected migration/source/target.
- `db:to-db` resolves the migration name strictly from `--migration` (default `default`) — no more legacy fallback.

### Removed

- Legacy top-level `tables`, `columns`, `filters`, `transforms`, `upsert_keys`, `runtime.steps_in_tables`, and `runtime.tables` config shape (now actually deleted from the resolver — previously declared, still wired).
- Legacy transform rules `if_eq`, `multiply`, and `round_precision` from `DbToDbTransformEngine` (use `map` / `invoke` for equivalent behavior).
- Legacy "default vs legacy" migration name auto-detection in `DbToDbCommand`; the command now always reports `default` when `--migration` is omitted.
- Resolver helpers tied to the legacy shape: `normalizeLegacyTableDefinition`, `normalizeTargetTables`, `resolveFiltersForTable`, `resolveTransformsForTarget`, `resolveUpsertKeysForTarget`, `resolveLegacyTablesRootForPipelines`, `resolveLegacyPipelinesFromConfig`, `hasLegacyTablesConfig`, `assertNoConflictingLegacyTableModes`.
- Tests covering the removed legacy paths.

### Breaking Changes

- The legacy top-level config shape and the legacy transform rules are no longer accepted at runtime. Migrate any remaining configs to `dbtodb_mapping.migrations.<name>` and replace `if_eq` / `multiply` / `round_precision` with `map` / `invoke` (or closures).

## [1.0.1]

- Previous release. See git history for details.
