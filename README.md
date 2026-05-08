# mb4it/laravel-dbtodb-migration

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

The command uses the legacy top-level mapping when `tables` exists at the config root; otherwise it uses `migrations.default` when `--migration` is omitted. Simple named-migration runs do not need `--source` or `--target` when the migration defines them.

```php
// config/dbtodb_mapping.php
return [
    'migrations' => [
        'default' => [
            'source' => 'legacy_mysql',
            'target' => 'pgsql_app',

            'tables' => [
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

Run it:

```bash
php artisan db:to-db --dry-run
php artisan db:to-db
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

Use the short syntax for simple column maps. Switch to the full target definition when you need filters, transforms, upsert keys, operation, or source runtime settings. Supported target operations are `upsert`, `insert`, and `truncate_insert` (`truncate_insert` clears each target table once per run before the first write).

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
                        'columns' => [
                            'id' => 'id',
                            'link' => 'link',
                        ],
                        'transforms' => [
                            'link' => ['trim', 'null_if_empty'],
                        ],
                        'filters' => [],
                        'upsert_keys' => ['id'],
                        'operation' => 'upsert',
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
| `--migration=` | legacy top-level mapping or `default` | Named migration from `dbtodb_mapping.migrations`; when omitted, top-level legacy `tables`/`columns` config is honored if present, otherwise `migrations.default` is used. |
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

`runtime.defaults.max_rows_per_upsert` caps rows per SQL insert/upsert statement and helps avoid huge binding arrays. `runtime.cli_memory_limit`, `runtime.memory.memory_log_every_chunks`, `runtime.memory.force_gc_every_chunks`, and `--profile` are useful for tuning large migrations.

## Filters

Source filters support: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `like`, `not_like`, `null`, `not_null`, `between`, `not_between`, `exists_in`, `where_column`, `date`, `year`, and `month`. Filters may be written as a simple associative map, a list of rule arrays, or nested `and` / `or` groups.

Target filters use the same DSL where it can be evaluated against the PHP source row. `exists_in` is SQL-only and is rejected for target filters with a clear validation error. Target filters run in PHP after the source row is read.

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

Equivalent target/PHP filters can use nested groups and row-level operators:

```php
'filters' => [
    ['or' => [
        ['column' => 'status', 'operator' => '=', 'value' => 'paid'],
        ['column' => 'customer_email', 'operator' => 'like', 'value' => '%@example.com'],
    ]],
    ['column' => 'created_at', 'operator' => 'month', 'value' => 5],
    ['column' => 'total', 'operator' => 'where_column', 'value' => 'paid_total', 'comparison' => '<='],
],
```

## Column transforms

Transforms run before target type coercion. You can keep the legacy addressing style where `transforms` is keyed by **source column**, or use the newer style where it is keyed by **target column** in the target definition. When both keys exist for a mapped column, the target-column definition wins.

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
            // New style: key by target column.
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

            // Legacy style is still supported: key by source column.
            'legacy_id' => ['rule' => 'cast', 'type' => 'int'],
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
- Legacy rules: `if_eq`, `multiply`, and `round_precision`.
- `invoke`: call a `[class, method]` pair.

Closures and `invoke` callables receive the current value, full source row, source column, target column, and target table: `(mixed $value, array $sourceRow, ?string $sourceColumn, ?string $targetColumn, ?string $targetTable) => mixed`. Existing two-argument closures remain compatible in PHP configs.

## Developing this package

```bash
composer install
vendor/bin/phpunit
```

## License

MIT. See [LICENSE](LICENSE).
