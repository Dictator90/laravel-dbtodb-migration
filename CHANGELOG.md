# Changelog

All notable changes to `mb4it/laravel-dbtodb-migration` will be documented in this file.

## Unreleased

### Added

- Added named migrations through `dbtodb_mapping.migrations`.
- Added `migrations.default` fallback when `--migration` / `-m` is omitted.
- Added migration-level `source` and `target` connection configuration so normal runs no longer require connection CLI options.
- Added `--migration` with short alias `-m` for selecting a named migration.
- Added short table syntax: `source_table => [target_table => [source_column => target_column]]`.
- Added full table syntax with per-source filters/runtime options and per-target columns, transforms, filters, upsert keys, and operation.
- Added ordered migration `steps` for staged runs.
- Added stress coverage for 30,000 source rows, multiple targets, source and target filters, keyset pagination, transforms, dry runs, reports, and continue-on-error behavior.

### Changed

- Reworked the published config example around `migrations.default`.
- Updated command startup and profiling context to include selected migration, source connection, and target connection.
- Updated documentation to describe the migration-centric config shape and current CLI options.

### Removed

- Removed support for legacy global `tables`, `columns`, `filters`, `transforms`, `upsert_keys`, `runtime.steps_in_tables`, and `runtime.tables` config shapes.
- Removed unconfigured string/list target table shorthands; table mappings now use the documented short or full migration table syntax only.
- Removed `--source` and `--target` CLI overrides; source and target connections must be declared in the selected migration.
