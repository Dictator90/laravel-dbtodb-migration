# mb4it/laravel-dbtodb-migration

Laravel 12 package: an Artisan command that copies rows from **one source database connection** to **one or more target tables** on another connection, driven by `config/dbtodb_mapping.php`. Supports chunked reads, optional keyset pagination, per-target column maps and transforms, filters (including `exists_in` on the source query), upsert vs insert, strict validation, JSON reports, and optional profiling logs.

## Requirements

- PHP `^8.2`
- Laravel `^12.0`

## Installation

```bash
composer require mb4it/laravel-dbtodb-migration
```

Laravel discovers the service provider via `extra.laravel.providers` in `composer.json`. No manual registration is required.

## Developer guide: from zero to a working run

### What you are configuring

- **One source connection**, **one target connection** (any names; you pass them to the command).
- **`tables`** — which **source** tables participate. Each source table becomes **one pipeline**: read from source → map/transform → write to one or many **target** tables.
- **`columns`**, **`transforms`**, **`filters`**, **`upsert_keys`**, and **`runtime.tables.{source}`** are always keyed by **source table name**, never by step name (if you use staged `tables`, see below).

### Checklist

1. **Install** the package (`composer require` above).
2. **Publish config**: `php artisan vendor:publish --tag=dbtodb-migration-config` — you get `config/dbtodb_mapping.php`.
3. **Define two connections** in `config/database.php` (for example `source` and `target`, or `legacy_mysql` and `pgsql_app`). Point them at real databases.
4. **Edit `dbtodb_mapping.php`**:
   - Set **`tables`** to your source → target mapping.
   - Set **`columns`** for each `source_table` → `target_table` (see [Config: `dbtodb_mapping`](#config-dbtodb_mapping)).
   - Add **`transforms`** and **`filters`** as needed (empty arrays are fine to start).
5. **Optional**: add a log channel named like `db_to_db` in `config/logging.php` if you plan to use `--profile` (see [Profiling log channel](#profiling-log-channel)).
6. **First run**: use **`--dry-run`** to validate and read without writing, then run without it for a real copy.

### Minimal flat `tables` example

After publishing, replace the sample with your names. Single target per source:

```php
'tables' => [
    'legacy_users' => 'users',
],

'columns' => [
    'legacy_users' => [
        'users' => [
            'id' => 'id',
            'email' => 'email',
        ],
    ],
],

'transforms' => [
    'legacy_users' => [
        'users' => [],
    ],
],

'filters' => [
    'legacy_users' => [],
],
```

### Multiple targets from one source

Use an array of target table names in `tables`, then nest **`columns`** and **`transforms`** under each target:

```php
'tables' => [
    'events' => ['app_events', 'analytics_events'],
],
// columns.events.app_events[...], columns.events.analytics_events[...]
```

### Staged runs (`runtime.steps_in_tables`)

Use this when you must run **groups of tables in order** (for example dimensions first, then facts), but still want one config file.

1. Set **`runtime.steps_in_tables`** to `true` (in config or via `DB_TO_DB_STEPS_IN_TABLES=true`).
2. Change **`tables`** to **step name → inner map** (not a numeric list at the top level):

```php
'runtime' => [
    // ...
    'steps_in_tables' => true,
],

'tables' => [
    'dimensions' => [
        'legacy_countries' => 'countries',
        'legacy_cities' => 'cities',
    ],
    'facts' => [
        'legacy_orders' => 'orders',
    ],
],
```

- **No `--step`**: all steps run **in the order keys appear** in the `tables` array; the command builds **one ordered list of pipelines** (one progress bar for the whole run).
- **`--step=dimensions`**: only pipelines for that step’s sources.
- The **same source table must not appear in two steps** when you run all steps at once (configuration error).
- **`--tables=`** still limits by **source** table names **after** the effective map is resolved (flat or merged steps).

With **`steps_in_tables` false** (default), **`--step` is invalid** and triggers a clear `RuntimeException`.

### Command examples

```bash
# Defaults: connections "source" and "target"
php artisan db:to-db

# Explicit connections (typical in real apps)
php artisan db:to-db --source=legacy_mysql --target=pgsql_app

# Only some source tables (comma-separated, no spaces unless quoted)
php artisan db:to-db --source=legacy_mysql --target=pgsql_app --tables=legacy_users,legacy_roles

# Validate and read; no writes (good first check)
php artisan db:to-db --dry-run --source=legacy_mysql --target=pgsql_app

# Staged config: run one step only
php artisan db:to-db --source=legacy_mysql --target=pgsql_app --step=facts

# Custom JSON report path (default is under storage/logs/ with a timestamp)
php artisan db:to-db --report-file=storage/logs/db-to-db-last.json

# Keep going if one pipeline fails (check exit code and report anyway)
php artisan db:to-db --continue-on-error

# Per-chunk timing logs (needs channel in logging.php)
php artisan db:to-db --profile
```

## Publish configuration

```bash
php artisan vendor:publish --tag=dbtodb-migration-config
```

This copies `config/dbtodb_mapping.php`. Replace the example `source_items` → `items` mapping with your real source and target tables.

### Profiling log channel

`dbtodb_mapping.profile_logging` is the **name** of a Laravel log channel (same string you pass to `Log::channel()`). Define that channel in your app’s `config/logging.php` under `channels` (for example `db_to_db` with `daily` or `stack`), like any other channel. The package does not register channel drivers for you.

## Database connections

Define two connections in `config/database.php` — one **source**, one **target** (names are yours). The Artisan command defaults assume connections named `source` and `target`; override with `--source` and `--target` if you use different names.

Use `.env` for host, database, user, and password. The package does not read credentials directly.

Example `.env` fragment (wire these keys in `config/database.php` for your chosen connection names):

```env
SOURCE_DB_CONNECTION=mysql
SOURCE_DB_HOST=127.0.0.1
SOURCE_DB_DATABASE=source_db
SOURCE_DB_USERNAME=root
SOURCE_DB_PASSWORD=

TARGET_DB_CONNECTION=pgsql
TARGET_DB_HOST=127.0.0.1
TARGET_DB_DATABASE=app
TARGET_DB_USERNAME=postgres
TARGET_DB_PASSWORD=
```

## Command: `php artisan db:to-db`

| Option | Default | Description |
| --- | --- | --- |
| `--source=` | `source` | Laravel **database connection name** for reads (see `config/database.php`). |
| `--target=` | `target` | Laravel **database connection name** for writes. |
| `--tables=` | *(all allowed sources)* | Comma-separated **source** table names. Must be a subset of the sources that the config resolves to (after flattening `tables` / `--step`). |
| `--step=` | *(empty)* | Only when `runtime.steps_in_tables` is `true`: run a single named step’s `tables` map. Omit to run every step in config order. |
| `--dry-run` | off | Build pipelines, validate, read source data in chunks; **no** inserts/upserts on the target. |
| `--continue-on-error` | off | After a pipeline failure, continue with the next pipeline. Inspect the console table and JSON report for `failed` rows. |
| `--report-file=` | auto path under `storage/logs/` | Streaming JSON report (suitable for large runs). |
| `--profile` | off | Emit timing logs (per pipeline / chunk) to the channel in `dbtodb_mapping.profile_logging`. |

The command prints a summary table (and optional verbose timing with `-v`). On configuration or runtime errors it prints a sanitized message and exits with a non-zero code.

## Environment variables (published config)

These are read **only from** `config/dbtodb_mapping.php` (the package does not call `env()` from `src/`). Typical mappings:

| Variable | Config area | Role |
| --- | --- | --- |
| `DB_TO_DB_LOG_CHANNEL` | `profile_logging` | Log channel name for `--profile`. |
| `DB_TO_DB_MAX_ROWS_PER_UPSERT` | `runtime.defaults.max_rows_per_upsert` | Cap rows per single SQL upsert/insert batch. |
| `DB_TO_DB_MEMORY_LOG_EVERY_CHUNKS` | `runtime.memory.memory_log_every_chunks` | Log peak memory every N chunks (`0` = off). |
| `DB_TO_DB_FORCE_GC_EVERY_CHUNKS` | `runtime.memory.force_gc_every_chunks` | Run `gc_collect_cycles()` every N chunks. |
| `DB_TO_DB_SLOW_CHUNK_SECONDS` | `runtime.profile_slow_chunk_seconds` | With `--profile`, warn when a chunk step exceeds this duration. |
| `DB_TO_DB_STEPS_IN_TABLES` | `runtime.steps_in_tables` | Enable staged `tables` layout (`true` / `false` / `1` / `0` via `filter_var`). |
| `DB_TO_DB_MEMORY_LIMIT` | `runtime.cli_memory_limit` | Optional PHP `memory_limit` for the Artisan process. |

## Config: `dbtodb_mapping`

All keys below live under the `dbtodb_mapping` config array.

| Key | Purpose |
| --- | --- |
| `strict` | If `true`, target columns must exist and required NOT NULL columns without defaults must be present in the payload. |
| `tables` | Maps each **source** table name to one target table name (string) or several (array of strings). When `runtime.steps_in_tables` is `true`, use **step keys** instead: each step is `step_name => [ source_table => target or [targets…] ]` (same inner rules). Run one step with `php artisan db:to-db --step=step_name`, or omit `--step` to run all steps in config order. |
| `runtime.steps_in_tables` | If `true`, `tables` must be a non-list map of named steps (see above). Duplicate source tables across steps are rejected when running all steps. Env: `DB_TO_DB_STEPS_IN_TABLES` (boolean). Default `false` keeps the flat `tables` shape. |
| `columns` | `columns[source_table][target_table][source_col] = target_col`. For a single target, still use the nested `[target_table]` level. A **non-empty** map limits the source `SELECT` to those keys (plus filters / keyset). An **empty** map for that target means `SELECT *` and every column is read and copied. Omitting `columns.{source_table}` behaves like empty maps. |
| `transforms` | Per-source, per-target column transforms. Table-level rules share `transforms.{source_table}` with keys that are not target table names. |
| `filters` | Source filters: either a flat rule list applied to the source query, or a per-target shape with `default` and optional overrides per target table. |
| `runtime.defaults` | `chunk` (rows read/mapped per batch), `max_rows_per_upsert` (max rows per SQL `insert`/`upsert`; prevents huge binding arrays / OOM on PostgreSQL, default `500`). Env: `DB_TO_DB_MAX_ROWS_PER_UPSERT`. `transaction_mode`: `batch` or `atomic`. |
| `runtime.memory` | `memory_log_every_chunks` (0 = off), `force_gc_every_chunks` (0 = off). Env: `DB_TO_DB_MEMORY_LOG_EVERY_CHUNKS`, `DB_TO_DB_FORCE_GC_EVERY_CHUNKS`. |
| `runtime.profile_slow_chunk_seconds` | With `--profile`, chunk steps slower than this log as warning. Env: `DB_TO_DB_SLOW_CHUNK_SECONDS`. |
| `runtime.tables.{source_table}` | Optional overrides per **source** table key from `tables`: `chunk`, `transaction_mode`, `keyset_column` (stable ascending column for keyset pagination). |
| `runtime.cli_memory_limit` | Optional PHP `memory_limit` for the Artisan process (e.g. `512M`). Can also be driven via `DB_TO_DB_MEMORY_LIMIT` in the published config. |
| `auto_transforms` | `enabled`, `bool`, `bool_columns` (map target table name → list of columns forced to boolean coercion). |
| `upsert_keys` | Optional conflict columns for upsert: `upsert_keys[source_table] = ['id']` or `upsert_keys[source_table][target_table] = ['slug']` when the target PK cannot be discovered or you need an override. |
| `profile_logging` | Log channel **name** (string, default `db_to_db`). Must exist in `config/logging.php`. Env: `DB_TO_DB_LOG_CHANNEL`. Used with `--profile`. |

### Filters

Supported operators on the **source** query include: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `like`, `not_like`, `null`, `not_null`, `between`, `not_between`, and **`exists_in`**.

`exists_in` builds a semi-join against another table (see `DbToDbSourceReader`). It is only valid on **source** filters; target-side filters do not support `exists_in`.

### Order of tables and foreign keys

Process parent tables before children, or use `filters` / `exists_in` so you only migrate rows that already have referential integrity on the target. With staged `tables`, order steps so dependencies are satisfied, or run `--step` manually in sequence.

### Memory usage

Peak memory is roughly proportional to **chunk size × row width in PHP**, and to **how many rows are packed into one `upsert`** (Laravel flattens all parameters per statement). Keep `runtime.defaults.max_rows_per_upsert` modest (default `500`); lower it further for very wide tables. The source `SELECT` list comes from **`columns`**: a **non-empty** map limits read columns; an **empty** map (single target) means `SELECT *`. Use `runtime.cli_memory_limit` / `DB_TO_DB_MEMORY_LIMIT`, `runtime.memory`, and `--profile` when tuning.

### Typical failures

- **Out of memory** — reduce `runtime.defaults.chunk`, lower `max_rows_per_upsert`, narrow `columns` maps, or raise `runtime.cli_memory_limit` / `DB_TO_DB_MEMORY_LIMIT`.
- **Foreign key violations** — reorder pipelines or tighten filters.
- **Upsert errors** — ensure PostgreSQL has unique/PK columns matching upsert keys; use `upsert_keys` when discovery is wrong.

## Profiling and logs

- **`--profile`** emits structured log lines (pipeline start/end, chunk read/write, slow chunk warnings).
- **Channel** — string `dbtodb_mapping.profile_logging` (default `db_to_db`). Point it at a channel defined in `config/logging.php` (`driver`, `path`, `level`, etc. live there).
- **Slow chunk threshold** — `dbtodb_mapping.runtime.profile_slow_chunk_seconds` (default `5`), env `DB_TO_DB_SLOW_CHUNK_SECONDS` in the published config.

## Troubleshooting

1. **Command not found** — run `php artisan list` and confirm the package is installed; clear `bootstrap/cache/packages.php` if you disabled discovery.
2. **Wrong connection** — pass `--source` and `--target` explicitly.
3. **Empty pipelines** — `dbtodb_mapping.tables` must list your source tables (or your step must define them when `steps_in_tables` is true). `--tables` must be a subset of the **resolved** source keys.
4. **`--step` errors** — with flat `tables`, do not pass `--step`. With staged `tables`, the step name must exist as a key in `tables`.
5. **Strict mapping errors** — disable `strict` only as a temporary debug step; fix column maps and required columns properly.

## Developing this package

From the **repository root** (this package’s directory):

```bash
composer install
vendor/bin/phpunit
```

## License

MIT. See [LICENSE](LICENSE).
