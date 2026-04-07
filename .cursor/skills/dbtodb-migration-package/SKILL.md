---
name: dbtodb-migration-package
description: Laravel package mb4it/laravel-dbtodb-migration only — Artisan db:to-db, dbtodb_mapping, DbToDbRoutingExecutor, source reader, target writer, service provider. Use when editing the package sources, config, or tests; not host-application-specific mapping.
---

# Db-to-db migration package (library only)

All paths are **relative to the package repository root** (Composer package `mb4it/laravel-dbtodb-migration`).

## Layout

- `src/Console/DbToDbCommand.php` — `php artisan db:to-db`, builds pipelines from config, progress bar, streaming JSON report.
- `src/Support/Database/DbToDbRoutingExecutor.php` — pipelines: chunked read, map/transform, batch/atomic writes, profiling hooks, `run()` + `runEach()`.
- `src/Support/Database/DbToDbSourceReader.php` — source `Query\Builder`, filters including `exists_in`.
- `src/Support/Database/DbToDbTargetWriter.php` — upsert/insert, PostgreSQL bind-parameter chunking, dedupe by conflict keys.
- `src/Support/Database/DbToDbMappingValidator.php` — pipeline shape; `keyset_column` identifier validation.
- `src/DbToDbServiceProvider.php` — merge config, singleton executor.
- `config/dbtodb_mapping.php` — default/publishable config; **env-backed values belong here**, not in `src/`.
- `tests/` — Feature + Unit (Orchestra Testbench).
- `README.md` — user-facing documentation.

## Mental model

1. **Pipelines:** one source table → N targets. Fields: `name`, `strict`, `transaction_mode` (`batch` | `atomic`), `source` (connection, table, chunk, filters, optional `keyset_column`), `targets[]` (connection, table, operation, map, transforms, filters, optional `keys`).
2. **Command** flattens `dbtodb_mapping.tables` (flat map, or per-step maps when `runtime.steps_in_tables` + optional `--step`) / `columns` / `transforms` / `filters` / `runtime` into pipelines, then `executor->runEach(...)` with a per-pipeline report callback.
3. **Executor** validates, counts rows, reads in chunks, maps and transforms, writes (per chunk). Dry-run skips writes.
4. **Report file:** JSON built incrementally; wrapper shape `dry_run`, `continue_on_error`, `pipelines` array.

## Config (`config('dbtodb_mapping')`)

- `strict`, `tables`, `columns`, `transforms`, `filters`, `upsert_keys`, `auto_transforms`, `profile_logging`.
- `runtime.defaults.chunk`, `runtime.defaults.max_rows_per_upsert`, `runtime.defaults.transaction_mode`.
- `runtime.tables.{source}` — overrides: `chunk`, `transaction_mode`, `keyset_column`.
- `runtime.memory.*` — periodic memory logs and optional GC between chunks.
- `runtime.profile_slow_chunk_seconds` — slow chunk threshold when `--profile` (env only in config file, e.g. `DB_TO_DB_SLOW_CHUNK_SECONDS`).
- `runtime.steps_in_tables` — when `true`, `tables` is `step => [ source => target ]`; `db:to-db --step=name` runs one step (env `DB_TO_DB_STEPS_IN_TABLES`).
- `runtime.cli_memory_limit` — optional CLI `memory_limit`.

## Rules and pitfalls

- **No `env()` in `src/`** — use `config()`; define env in `config/dbtodb_mapping.php`.
- **Atomic mode:** targets in one pipeline must use a single DB connection.
- **`--continue-on-error`:** failed pipelines are reported but the command may still exit successfully; document if you change this.
- **`profile_logging`** is a channel **name** string; the channel must be defined in the host app `config/logging.php`.

## Tests

```bash
cd dbtodb-migration-package   # package root
vendor/bin/phpunit
```

Key files: `tests/Feature/DbToDbCommandTest.php`, `tests/Unit/Support/Database/*`, `tests/Unit/Config/*`, provider test.
