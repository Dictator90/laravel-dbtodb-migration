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

### Changed

- Config is now migration-centric.
- CLI startup output and profiling include selected migration/source/target.

### Removed

- Legacy global `tables`, `columns`, `filters`, `transforms`, `upsert_keys`, `runtime.steps_in_tables`, and `runtime.tables` config shape.

### Breaking Changes

- This release intentionally breaks the legacy config shape while the package is still in development. Legacy compatibility is not preserved so the configuration can move fully to the migration-centric structure.
