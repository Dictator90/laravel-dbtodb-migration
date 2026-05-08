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

## Quick start

The command uses `migrations.default` when `--migration` / `-m` is omitted, and reads `source` / `target` connections from the selected migration.

```php
// config/dbtodb_mapping.php
return [
    'migrations' => [
        'default' => [
            'source' => 'source_mysql',
            'target' => 'pgsql_app',

            'tables' => [
                'source_users' => [
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

Add as many migrations as you need under `migrations`. Select one with `--migration=name` or the short alias `-m=name`.

```php
'migrations' => [
    'default' => [
        'source' => 'source_mysql',
        'target' => 'pgsql_app',
        'tables' => [
            'source_users' => [
                'users' => ['id' => 'id', 'email' => 'email'],
            ],
        ],
    ],

    'catalog' => [
        'source' => 'source_mysql',
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
php artisan db:to-db -m=catalog
php artisan db:to-db --migration=catalog --dry-run
```

## Full table syntax

Use the short syntax for simple column maps. Switch to the full target definition when you need filters, transforms, upsert keys, operation, or source runtime settings.

```php
'migrations' => [
    'catalog' => [
        'source' => 'source_mysql',
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
                        'values' => [
                            'source_type' => 'catalog',
                        ],
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
        'source' => 'source_mysql',
        'target' => 'pgsql_app',
        'steps' => [
            'dimensions' => [
                'tables' => [
                    'source_brands' => ['brands' => ['id' => 'id', 'name' => 'name']],
                ],
            ],
            'facts' => [
                'tables' => [
                    'source_products' => ['products' => ['id' => 'id', 'brand_id' => 'brand_id']],
                ],
            ],
        ],
    ],
],
```

```bash
php artisan db:to-db -m=catalog --step=dimensions
```

## Command options

| Option | Default | Description |
| --- | --- | --- |
| `--migration=` / `-m=` | `default` | Named migration from `dbtodb_mapping.migrations`. |
| `--tables=` | all source tables in selected migration/step | Comma-separated source table names. |
| `--step=` | all steps | Run one step from the selected migration. |
| `--dry-run` | off | Validate and read without writing. |
| `--continue-on-error` | off | Continue after per-pipeline failures. |
| `--report-file=` | timestamped file in `storage/logs` | Write JSON report. |
| `--profile` | off | Log timings to `dbtodb_mapping.profile_logging`. |

### Static target values

Use `values` in a full target definition when the target needs columns that do not come from the source row (for example an `origin`, `source_type`, or status marker):

```php
'orders_a' => [
    'unified_orders' => [
        'columns' => ['id' => 'id', 'name' => 'title'],
        'values' => ['origin' => 'orders_a'],
        'upsert_keys' => ['id'],
    ],
],
```

## Runtime and memory

Global defaults live in `runtime.defaults`; a migration may override them with `migrations.{name}.runtime.defaults`, and a source table may override `chunk`, `transaction_mode`, and `keyset_column` in its `source` definition.

`runtime.defaults.max_rows_per_upsert` caps rows per SQL insert/upsert statement and helps avoid huge binding arrays. `runtime.cli_memory_limit`, `runtime.memory.memory_log_every_chunks`, `runtime.memory.force_gc_every_chunks`, and `--profile` are useful for tuning large migrations.

## Filters

Source filters support: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `like`, `not_like`, `null`, `not_null`, `between`, `not_between`, and `exists_in`.

Target filters support the same row-level operators except `exists_in`; target filters run in PHP after the source row is read.

## Column transforms

Transforms run before target type coercion. Each mapped source column can use a single rule or a list applied in order.

- Strings: `trim`, `null_if_empty`, `zero_date_to_null`.
- Rule arrays: `if_eq`, `multiply`, `round_precision`, `invoke`.
- Closures are supported in PHP config: `(mixed $value, array $sourceRow) => mixed`.

## Developing this package

```bash
composer install
vendor/bin/phpunit
```

## License

MIT. See [LICENSE](LICENSE).
