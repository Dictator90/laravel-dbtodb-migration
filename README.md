# mb4it/laravel-dbtodb-migration

English | [Русский](README.ru.md)

Laravel 12 package: an Artisan command that copies rows from one configured database connection to one or more target tables on another connection. Runs are driven by named migrations in `config/dbtodb_mapping.php`, with chunked reads, optional keyset pagination, per-target column maps and transforms, filters, upsert vs insert, strict validation, JSON reports, and optional profiling logs.

## Requirements

- PHP `^8.2`
- Laravel `^12.0`

## Installation

```bash
composer require mb4it/laravel-dbtodb-migration
php artisan vendor:publish --tag=dbtodb-migration-config
```

Define the database connections referenced by your migrations in `config/database.php`.

## Supported target drivers

Target-table metadata resolution, required-column validation, and automatic primary-key detection are supported for these Laravel target connection drivers:

- `sqlite`
- `pgsql`
- `mysql`
- `mariadb`
- `sqlsrv`

Table prefixes configured on the target connection are honored when reading target metadata.

## Quick start

Use this path when you only need to copy a few columns from one source table into one target table. The command uses `migrations.default` when `--migration` is omitted, so a simple run does not need `--source` or `--target` once the migration defines them.

1. Install and publish the package config:

    ```bash
    composer require mb4it/laravel-dbtodb-migration
    php artisan vendor:publish --tag=dbtodb-migration-config
    ```

2. Add the source and target Laravel database connections in `config/database.php` (for example `legacy_mysql` and `pgsql_app`).

3. Edit `config/dbtodb_mapping.php` and replace the published example with a `default` migration:

    ```php
    return [
        'migrations' => [
            'default' => [
                'source' => 'legacy_mysql',
                'target' => 'pgsql_app',

                'tables' => [
                    // source table => target table => source column => target column
                    'legacy_users' => [
                        'users' => [
                            'id' => 'id',
                            'email' => 'email',
                        ],
                    ],
                ],
            ],
        ],
    ];
    ```

4. Run a safe validation pass first. `--dry-run` reads data, applies filters/transforms, validates target columns in strict mode, and writes only the JSON report:

    ```bash
    php artisan db:to-db --dry-run
    ```

5. If the report looks correct, run the migration for real:

    ```bash
    php artisan db:to-db
    ```

6. For production-sized data, prefer adding `keyset_column` and an explicit `chunk` size in the full table syntax below.

## Documentation coverage and safety notes

This README documents the public configuration surface for the current migration-centric format: installation, supported drivers, quick start, named migrations, ordered steps, CLI options, runtime/memory settings, automatic target-type transforms, profile logging, sequence synchronization, filters, column transforms, fan-out/fan-in routing, and edge-case behavior. Before running against production data, review the target schema, run `--dry-run`, keep `strict` enabled unless you intentionally want looser validation, and store the generated JSON report for audit/debugging.

The package no longer accepts the older top-level `tables` / `columns` / `filters` config shape; use `migrations.<name>` for every run.


## Complete complex config example

The example below shows a production-oriented `config/dbtodb_mapping.php` with global runtime defaults, auto transforms, ordered steps, source SQL filters, target routing filters, transforms, deduplication, mixed write operations, and sequence synchronization. It assumes that the Laravel connections `legacy_mysql` and `pgsql_app` already exist in `config/database.php`, and that target tables/columns match the mapped payloads.

```php
<?php

return [
    // Keep strict mode enabled for real migrations: every mapped target column
    // must exist and every required target column must receive a value.
    'strict' => true,

    // Used only when the command is run with --profile.
    'profile_logging' => env('DB_TO_DB_LOG_CHANNEL', 'db_to_db'),

    // After a successful non-dry run, realign auto-increment/serial/identity
    // counters for the listed target tables.
    'sync_serial_sequences' => true,
    'sync_serial_sequence_tables' => [
        'users',
        ['table' => 'orders', 'column' => 'id'],
    ],

    'runtime' => [
        'defaults' => [
            // Default chunk for source reads unless a migration/table overrides it.
            'chunk' => 1000,
            // Maximum rows per SQL insert/upsert statement.
            'max_rows_per_upsert' => 500,
            // batch = transaction per chunk; atomic = one transaction per pipeline.
            'transaction_mode' => 'batch',
        ],
        'memory' => [
            // 0 disables periodic memory logs; useful to enable during profiling.
            'memory_log_every_chunks' => 10,
            'force_gc_every_chunks' => 20,
        ],
        'profile_slow_chunk_seconds' => 5.0,
        'cli_memory_limit' => '1G',
    ],

    'auto_transforms' => [
        'enabled' => true,
        'bool' => true,
        'integer' => true,
        'float' => true,
        'json' => true,
        'date' => true,
        'datetime' => true,
        'string' => false,
        'empty_string_to_null' => true,
        'json_invalid' => 'fail', // fail the row when invalid JSON is mapped to json/jsonb.
        'bool_columns' => [
            // Force these columns to boolean even if a driver reports a generic type.
            'users' => ['is_active', 'is_admin'],
        ],
    ],

    'migrations' => [
        'default' => [
            'source' => 'legacy_mysql',
            'target' => 'pgsql_app',

            // Migration-level overrides; table-level source.runtime can override again.
            'runtime' => [
                'defaults' => [
                    'chunk' => 2000,
                    'transaction_mode' => 'batch',
                ],
            ],

            // Steps run in this order when --step is omitted. Use this when facts
            // depend on dimensions that should be migrated first.
            'steps' => [
                'dimensions' => [
                    'tables' => [
                        'legacy_countries' => [
                            'targets' => [
                                'countries' => [
                                    'columns' => [
                                        'iso' => 'iso_code',
                                        'title' => 'name',
                                    ],
                                    'transforms' => [
                                        'iso_code' => ['trim', 'upper'],
                                        'name' => ['trim', 'null_if_empty'],
                                    ],
                                    'upsert_keys' => ['iso_code'],
                                    'operation' => 'upsert',
                                ],
                            ],
                        ],
                    ],
                ],

                'users_and_orders' => [
                    'tables' => [
                        'legacy_users' => [
                            // Source filters are pushed into SQL and reduce rows for all targets.
                            'source' => [
                                'filters' => [
                                    ['column' => 'deleted_at', 'operator' => 'null'],
                                    ['column' => 'email', 'operator' => 'not_null'],
                                    ['column' => 'created_at', 'operator' => 'year', 'value' => 2026],
                                    ['or' => [
                                        ['column' => 'status', 'operator' => 'in', 'values' => ['active', 'pending']],
                                        ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                                    ]],
                                ],
                                'runtime' => [
                                    // Keyset pagination is preferable for large tables.
                                    'chunk' => 5000,
                                    'keyset_column' => 'id',
                                ],
                            ],

                            'targets' => [
                                // Main target: upsert all filtered users.
                                'users' => [
                                    'columns' => [
                                        'id' => 'id',
                                        'email' => 'email',
                                        'first_name' => 'name',
                                        'status' => 'status',
                                        'country_iso' => 'country_id',
                                        'is_admin' => 'is_admin',
                                        'created_at' => 'created_at',
                                    ],
                                    'transforms' => [
                                        'email' => ['trim', 'lower', 'null_if_empty'],
                                        'name' => [
                                            ['rule' => 'from_columns', 'columns' => ['first_name', 'last_name'], 'separator' => ' '],
                                            ['rule' => 'default', 'value' => 'Anonymous'],
                                        ],
                                        'status' => ['rule' => 'map', 'map' => ['active' => 'enabled', 'pending' => 'new'], 'default' => 'disabled'],
                                        'country_id' => [
                                            'rule' => 'lookup',
                                            'target' => [
                                                'connection' => 'pgsql_app',
                                                'table' => 'countries',
                                                'key' => 'iso_code',
                                                'value' => 'id',
                                            ],
                                        ],
                                    ],
                                    'upsert_keys' => ['id'],
                                    'operation' => 'upsert',
                                    'deduplicate' => ['keys' => ['id'], 'strategy' => 'last'],
                                    'on_row_error' => 'fail',
                                ],

                                // Routed target: only admin source rows are inserted here.
                                'admins' => [
                                    'columns' => [
                                        'id' => 'user_id',
                                        'email' => 'email',
                                    ],
                                    'filters' => [
                                        ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                                    ],
                                    'operation' => 'insert',
                                    // For insert-like operations, skip bad rows but keep the pipeline running.
                                    'on_row_error' => 'skip_row',
                                ],
                            ],
                        ],

                        'legacy_orders' => [
                            'source' => [
                                'filters' => [
                                    ['column' => 'total', 'operator' => '>=', 'value' => 0],
                                    ['column' => 'user_id', 'operator' => 'exists_in', 'value' => [
                                        'table' => 'legacy_users',
                                        'column' => 'id',
                                        'where' => [
                                            ['column' => 'deleted_at', 'operator' => 'null'],
                                        ],
                                    ]],
                                ],
                                'runtime' => [
                                    'chunk' => 1000,
                                    'keyset_column' => 'id',
                                ],
                            ],
                            'targets' => [
                                'orders' => [
                                    'columns' => [
                                        'id' => 'id',
                                        'user_id' => 'user_id',
                                        'total' => 'total',
                                        'payload_json' => 'payload',
                                        'created_at' => 'created_at',
                                    ],
                                    'transforms' => [
                                        'total' => ['rule' => 'cast', 'type' => 'float'],
                                        'payload' => ['rule' => 'cast', 'type' => 'json'],
                                    ],
                                    'upsert_keys' => ['id'],
                                    'operation' => 'upsert',
                                ],
                            ],
                        ],
                    ],
                ],

                'refresh_reports' => [
                    'tables' => [
                        'legacy_order_report' => [
                            'targets' => [
                                'order_reports' => [
                                    'columns' => [
                                        'order_id' => 'order_id',
                                        'summary' => 'summary',
                                    ],
                                    // Truncate the target once, then insert the fresh snapshot.
                                    'operation' => 'truncate_insert',
                                    'on_row_error' => 'skip_row',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ],
    ],
];
```

## Named migrations

Add as many migrations as you need under the top-level `migrations` node. Select one with `--migration=name`.

```php
'migrations' => [
    'default' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'tables' => [
            'legacy_users' => [
                'users' => ['id' => 'id', 'email' => 'email'],
            ],
        ],
    ],

    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'tables' => [
            'top_banners' => [
                'catalog_banners' => [
                    'link' => 'link',
                ],
                'catalog_banners_2' => [
                    'name' => 'name',
                ],
            ],
        ],
    ],
],
```

```bash
php artisan db:to-db --migration=catalog
php artisan db:to-db --migration=catalog --source=legacy_mysql --target=pgsql_app
php artisan db:to-db --migration=catalog --dry-run
```

`--source` and `--target` are still available as overrides for local/debug runs, but normal migrations should define `source` and `target` in config.

## Full table syntax

Use the short syntax for simple column maps. Switch to the full target definition when you need source/target filters, transforms, upsert keys, operation, deduplication, row-error handling, or source runtime settings. Supported target operations are `upsert`, `insert`, and `truncate_insert` (`truncate_insert` clears each target table once per run before the first write).

```php
'migrations' => [
    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',

        'tables' => [
            'top_banners' => [
                // Optional source-table settings.
                'source' => [
                    'filters' => [
                        ['column' => 'active', 'operator' => '=', 'value' => 1],
                    ],
                    'chunk' => 1000,
                    'keyset_column' => 'id',
                ],

                'targets' => [
                    'catalog_banners' => [
                        // You can leave columns empty (`'columns' => []`) if all
                        // columns are generated by transforms or maps are fully implicit.
                        'columns' => [
                            'id' => 'id',
                            'link' => 'link',
                        ],
                        'transforms' => [
                            'link' => ['trim', 'null_if_empty'],
                            // You can add static rules for columns that don't exist in the source:
                            'type' => ['rule' => 'static', 'value' => 1],
                        ],
                        // Optional target filters are evaluated per target after source filters.
                        // Use them to route only some already-read source rows into this target.
                        'filters' => [
                            ['column' => 'link', 'operator' => 'not_null'],
                        ],
                        'upsert_keys' => ['id'],
                        'operation' => 'upsert',
                        // Optional: prevent target unique-key collisions after mapping/transforms.
                        'deduplicate' => ['keys' => ['id'], 'strategy' => 'last'],
                        // Optional: for insert/truncate_insert, skip only rows that fail to write.
                        'on_row_error' => 'fail', // fail|skip_row
                    ],
                ],
            ],
        ],
    ],
],
```

## Ordered steps

A migration can contain ordered `steps`. Omit `--step` to run all steps in order, or pass `--step=name` to run one step.

```php
'migrations' => [
    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'steps' => [
            'dimensions' => [
                'tables' => [
                    'legacy_brands' => ['brands' => ['id' => 'id', 'name' => 'name']],
                ],
            ],
            'facts' => [
                'tables' => [
                    'legacy_products' => ['products' => ['id' => 'id', 'brand_id' => 'brand_id']],
                ],
            ],
        ],
    ],
],
```

```bash
php artisan db:to-db --migration=catalog --step=dimensions
```

## Command options

| Option | Default | Description |
| --- | --- | --- |
| `--migration=` | `default` | Named migration from `dbtodb_mapping.migrations`. When omitted, `migrations.default` is used. |
| `--tables=` | all source tables in selected migration/step | Comma-separated source table names. |
| `--step=` | all steps | Run one step from the selected migration. |
| `--source=` | migration `source` | Override source connection. |
| `--target=` | migration `target` | Override target connection. |
| `--dry-run` | off | Validate and read without writing. |
| `--continue-on-error` | off | Continue after per-pipeline failures. |
| `--report-file=` | timestamped file in `storage/logs` | Write JSON report. |
| `--profile` | off | Log timings to `dbtodb_mapping.profile_logging`. |

## Runtime and memory

Global defaults live in `runtime.defaults`; a migration may override them with `migrations.{name}.runtime.defaults`, and a source table may override `chunk`, `transaction_mode`, and `keyset_column` in its `source` definition.

| Key | Default | Purpose |
| --- | --- | --- |
| `runtime.defaults.chunk` | `500` | Rows read per chunk. |
| `runtime.defaults.max_rows_per_upsert` | `500` | Cap on rows per insert/upsert statement. The writer also auto-splits batches that would exceed PostgreSQL's 65535 placeholder limit, so `max_rows_per_upsert` mostly controls statement size, not correctness. |
| `runtime.defaults.transaction_mode` | `'batch'` | `batch` wraps each chunk write in one transaction (efficient). `atomic` wraps the entire pipeline in a single transaction — **all targets must share one connection**, otherwise the run fails fast. |
| `runtime.memory.memory_log_every_chunks` | `0` (off) | When `>0`, logs `memory_get_usage()` every N chunks via the profile channel. |
| `runtime.memory.force_gc_every_chunks` | `20` | Calls `gc_collect_cycles()` every N chunks. Set to `0` to disable. |
| `runtime.profile_slow_chunk_seconds` | `5.0` | Chunks slower than this are tagged `slow: true` in profile logs. |
| `runtime.cli_memory_limit` | unset | When set (e.g. `'1G'`), passes the value to `ini_set('memory_limit', …)` at command start. |

A source table can opt in to keyset pagination by setting `keyset_column` (e.g. `'id'`). Requirements: the column must be present in the source SELECT (the resolver adds it automatically when you use a column map), it must be sortable, and values should be unique and monotonically increasing — otherwise rows can be skipped or duplicated across chunks. Without `keyset_column`, the reader falls back to offset pagination.

## Auto transforms

The package coerces values to match common target column shapes after your transforms run. Configure under `auto_transforms`:

```php
'auto_transforms' => [
    'enabled' => true,              // Master switch.
    'bool' => true,                 // Cast to bool when target column type looks boolean.
    'integer' => true,              // Cast int/smallint/bigint/serial-like target columns.
    'float' => true,                // Cast float/double/real/numeric/decimal target columns.
    'json' => true,                 // Encode arrays/objects for json/jsonb target columns.
    'date' => true,                 // Format date target columns as Y-m-d.
    'datetime' => true,             // Format datetime/timestamp target columns as Y-m-d H:i:s.
    'string' => false,              // Optional scalar-to-string coercion for text/varchar/uuid.
    'empty_string_to_null' => true, // For nullable target columns.
    'json_invalid' => 'keep',       // keep|null|fail for invalid JSON strings.
    'bool_columns' => [
        // Force per-table columns to bool even when the driver does not report a boolean type.
        'users' => ['is_admin', 'is_active'],
    ],
],
```

Auto transforms run **after** explicit column transforms and use resolved target-table metadata. `bool` casting accepts `1`, `true`, `'true'`, `'1'`, `'yes'`, `'on'` (and their `false` counterparts). Empty strings become `null` only when `empty_string_to_null` is enabled and the target column is nullable. Invalid JSON strings are kept by default; set `json_invalid` to `null` or `fail` for stricter behavior.

## Profile logging and sequence sync

```php
'profile_logging' => env('DB_TO_DB_LOG_CHANNEL', 'db_to_db'),

'sync_serial_sequences' => true,
'sync_serial_sequence_tables' => [
    // Optional explicit list. When omitted, the command derives target tables
    // from the executed pipelines.
    'users',
    ['table' => 'orders', 'column' => 'order_id'],
],
```

- `--profile` writes per-pipeline / per-chunk timings, memory snapshots, and start/end events to the channel named in `profile_logging`. Define the channel in `config/logging.php` (e.g. a daily file).
- `sync_serial_sequences` realigns auto-increment / serial / identity counters on the target connection after a successful run. Drivers supported: `pgsql` (`setval`), `mysql`/`mariadb` (`ALTER TABLE … AUTO_INCREMENT`), `sqlite` (`sqlite_sequence`), `sqlsrv` (`DBCC CHECKIDENT`). The sync only fires when no pipelines failed and the run was not a `--dry-run`.

## Filters

Source filters support: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `like`, `not_like`, `null`, `not_null`, `between`, `not_between`, `exists_in`, `where_column`, `date`, `year`, and `month`. Filters may be written as a simple associative map, a list of rule arrays, nested `and` / `or` groups, or a PHP callable that receives the source `Illuminate\Database\Query\Builder`.

Source and target filters run at different stages:

1. **Source filters** are applied to the SQL query before rows are read from the source database. They reduce the shared input set for the whole source-table pipeline.
2. **Target filters** are evaluated in PHP for each target, after a row has passed source filters and before target column mapping/transforms. They do not query the target table and do not see transformed target values; they inspect the original source row that was read. Use target filters for fan-out/routing, for example: write every active user to `users`, but only source rows with `is_admin = 1` to `admins`.

Target filters use the same DSL where it can be evaluated against the PHP source row. `exists_in` is SQL-only and is rejected for target filters with a clear validation error. Builder callables are source-only and should not be used for target filters.

Complex source filter example:

```php
'source' => [
    'connection' => 'db_source',
    'table' => 'orders',
    'filters' => [
        ['column' => 'total', 'operator' => '>=', 'value' => 100],
        ['column' => 'total', 'operator' => 'where_column', 'value' => 'paid_total', 'comparison' => '<='],
        ['column' => 'created_at', 'operator' => 'date', 'value' => '2026-05-08'],
        ['column' => 'created_at', 'operator' => 'year', 'value' => 2026],
        ['or' => [
            ['column' => 'status', 'operator' => 'in', 'values' => ['paid', 'shipped']],
            ['and' => [
                ['column' => 'status', 'operator' => '=', 'value' => 'pending'],
                ['column' => 'customer_email', 'operator' => 'like', 'value' => '%@example.com'],
            ]],
        ]],
        ['column' => 'customer_id', 'operator' => 'exists_in', 'value' => [
            'table' => 'customers',
            'column' => 'id',
            'where' => [
                ['column' => 'active', 'operator' => '=', 'value' => 1],
                ['column' => 'deleted_at', 'operator' => 'null'],
            ],
        ]],
    ],
],
```


Builder callback source filter example (runtime PHP config only; do not use closures if you rely on Laravel `config:cache`):

```php
'source' => [
    'filters' => fn (Illuminate\Database\Query\Builder $query) => $query
        ->where('active', 1)
        ->whereExists(function ($sub) {
            $sub->selectRaw('1')
                ->from('customers')
                ->whereColumn('customers.id', 'orders.customer_id');
        }),
],
```

Equivalent target/PHP filters can use nested groups and row-level operators. In a target definition, these rules filter the rows already selected by `source.filters` for that one target only:

```php
'targets' => [
    'paid_order_exports' => [
        'columns' => ['id' => 'id', 'status' => 'status', 'total' => 'total'],
        'filters' => [
            ['or' => [
                ['column' => 'status', 'operator' => '=', 'value' => 'paid'],
                ['column' => 'customer_email', 'operator' => 'like', 'value' => '%@example.com'],
            ]],
            ['column' => 'created_at', 'operator' => 'month', 'value' => 5],
            ['column' => 'total', 'operator' => 'where_column', 'value' => 'paid_total', 'comparison' => '<='],
        ],
    ],
],
```

## Column transforms

Transforms are keyed by **target column** in the target definition.

```php
'targets' => [
    'users' => [
        'columns' => [
            'legacy_id' => 'id',
            'email_address' => 'email',
            'first_name' => 'name',
            'status_code' => 'status',
            'created' => 'created_at',
            'country_code' => 'country_id',
        ],

        'transforms' => [
            'email' => ['trim', 'lower', 'null_if_empty'],
            'name' => [
                ['rule' => 'from_columns', 'columns' => ['first_name', 'last_name'], 'separator' => ' '],
                ['rule' => 'default', 'value' => 'Anonymous'],
            ],
            'status' => ['rule' => 'map', 'map' => ['A' => 'active', 'B' => 'blocked'], 'default' => 'new'],
            'created_at' => ['rule' => 'date_format', 'from' => 'Y-m-d H:i:s', 'format' => 'Y-m-d'],
            'country_id' => [
                'rule' => 'lookup',
                'target' => [
                    'connection' => 'pgsql_app',
                    'table' => 'countries',
                    'key' => 'iso_code',
                    'value' => 'id',
                ],
            ],

            'id' => ['rule' => 'cast', 'type' => 'int'],
        ],
    ],
],
```

Supported string rules: `trim`, `null_if_empty`, `zero_date_to_null`, `lower`, `upper`, and `slug`.

Supported array rules:

- `default`: use `value` when the current value is `null`.
- `static`: always replace the current value with `value`.
- `map` / `enum`: map values with `map` (or `values`) and optional `default`.
- `replace`: plain `search` / `replace` string replacement.
- `regex_replace`: `pattern` / `replace` regular-expression replacement.
- `substr`: `start` (or `offset`) and optional `length`.
- `lower`, `upper`, `slug`: case and slug normalization; `slug` accepts optional `separator`.
- `date_format`: output `format`, with optional input `from` format.
- `cast`: `type` (or `to`) can be `int`, `float`, `string`, `bool`, `json`, or `array`.
- `concat`: combine `items` with optional `separator`; items may be `'$value'`, `['column' => 'source_col']`, or `['value' => 'literal']`.
- `coalesce`: return the first non-null item from `items`, with optional `default`.
- `from_columns`: build a value from multiple source columns using `columns` plus either `separator` or a `template` such as `'{last_name}, {first_name}'`.
- `lookup`: resolve the current value (or `from_column`) through a source/target database table using `connection`, `table`, `key`, and `value` directly on the rule or inside a nested `source` / `target` array; lookups are cached per transform engine instance.
- `invoke`: call a `[class, method]` pair.

Closures and `invoke` callables receive the current value, full source row, source column, target column, and target table: `(mixed $value, array $sourceRow, ?string $sourceColumn, ?string $targetColumn, ?string $targetTable) => mixed`. Two-argument closures (`fn ($value, $row) => …`) remain compatible.

```php
'transforms' => [
    // Five-argument closure (preferred).
    'audit_label' => fn ($value, array $row, ?string $src, ?string $dst, ?string $table)
        => sprintf('%s:%s=%s', $table, $dst, $value),

    // Two-argument form (still supported).
    'full_name' => fn ($value, array $row) => trim(($row['first_name'] ?? '').' '.($row['last_name'] ?? '')),

    // Template via `from_columns`.
    'address' => [
        'rule' => 'from_columns',
        'columns' => ['street', 'city', 'country'],
        'template' => '{street}, {city} ({country})',
    ],
],
```

`lookup` caches results per `DbToDbTransformEngine` instance for the lifetime of one command run. The cache is keyed by `(connection, table, key column, value column, lookup value)`, so high-cardinality lookups grow memory linearly with distinct values. For very large fan-outs, prefer denormalising on the source side or use `invoke` with your own bounded cache.

## Routing patterns

### Fan-out (1 source → N targets)

```php
'tables' => [
    'legacy_users' => [
        'targets' => [
            'users' => [
                'columns' => ['id' => 'id', 'email' => 'email', 'is_admin' => 'is_admin'],
                'upsert_keys' => ['id'],
                'operation' => 'upsert',
            ],
            'admins' => [
                'columns' => ['id' => 'id', 'email' => 'email'],
                'filters' => [
                    ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                ],
                'operation' => 'insert',
            ],
        ],
    ],
],
```

Each target reads the same source row but applies its own filters/transforms/operation. When a source maps to more than one target, every target must declare a non-empty `columns` map.

### Fan-in (N sources → 1 target) with static columns

Two source tables write into the same target table. Use a `static` transform to mark the origin per pipeline.

```php
'tables' => [
    'legacy_users' => [
        'targets' => [
            'audit_log' => [
                'columns' => [
                    'id' => 'ref_id',
                    'name' => 'source_table',  // placeholder — overwritten by static below.
                    'email' => 'event_type',   // placeholder — overwritten by static below.
                ],
                'transforms' => [
                    'source_table' => ['rule' => 'static', 'value' => 'users'],
                    'event_type'   => ['rule' => 'static', 'value' => 'user_imported'],
                ],
                'operation' => 'insert',
            ],
        ],
    ],
    'legacy_orders' => [
        'targets' => [
            'audit_log' => [
                'columns' => [
                    'id' => 'ref_id',
                    'currency' => 'source_table',
                    'status'   => 'event_type',
                ],
                'transforms' => [
                    'source_table' => ['rule' => 'static', 'value' => 'orders'],
                    'event_type'   => ['rule' => 'static', 'value' => 'order_imported'],
                ],
                'operation' => 'insert',
            ],
        ],
    ],
],
```

Because transforms only fire for columns produced by the column map, the recipe above wires any source column into the target column you want to fill statically. The `static` transform discards the mapped value and writes the literal.

## Behavior reference

These semantics are not always obvious; lock them in before writing migrations against production data:

- **Missing source column.** When a column listed in `columns` is absent from a chunk row, that target column is silently skipped for that row. In `strict` mode, a required (NOT NULL, no default) target column with no value still raises an error.
- **Target deduplication.** Set `deduplicate => ['keys' => [...], 'strategy' => 'first'|'last']` on a target to collapse mapped payload rows by target columns before writing. This is useful for `insert` / `truncate_insert` targets with unique indexes; `last` is the default strategy. If `keys` is omitted, the target `upsert_keys` are used when available.
- **Row-level write errors.** Set `on_row_error => 'skip_row'` (or `write_error_mode => 'skip_row'`) to keep the fast batch write path, then fall back to row-by-row writes only if a batch fails. Failed rows are counted as `skipped` and unique error messages are added to the target report. The default is `fail`.
- **Upsert deduplication.** Within a single chunk, rows that share the same upsert keys are deduplicated **last-wins** before the SQL `UPSERT` is issued — important when the same key appears multiple times in the source.
- **`truncate_insert`.** The target table is truncated **once per command run**, just before the first write to it. Subsequent chunks (or other source pipelines pointing to the same target) only insert.
- **Mixed operations across pipelines.** Multiple sources can fan into one target with different operations (e.g. one `truncate_insert`, another `insert`). The truncate happens once.
- **`atomic` transaction mode.** Every target in the pipeline must use the same connection. The executor refuses to start the pipeline otherwise. Use `batch` for cross-connection routing.
- **`dry-run`.** Reads the source, applies filters/transforms, validates strictness, but never opens a write transaction or fires `sync_serial_sequences`.
- **`continue-on-error`.** Failures in one pipeline are reported in the JSON report and the table view; remaining pipelines still run.
- **Strict mode.** When `strict` is true (default), every target column referenced by `columns` must exist on the target table, and required columns must end up in the payload after transforms.
- **Auto-increment sync.** Runs only after a successful write phase. Skipped automatically for unsupported drivers.
- **PostgreSQL placeholder limit.** Insert/upsert batches that would exceed 65535 placeholders are split automatically based on the column count of the target table.

## Developing this package

```bash
composer install
vendor/bin/phpunit
```

## License

MIT. See [LICENSE](LICENSE).
