<?php

/**
 * Example mapping for mb4it/laravel-dbtodb-migration.
 *
 * Copy to config/dbtodb_mapping.php via:
 *   php artisan vendor:publish --tag=dbtodb-migration-config
 *
 * Then define matching connections in config/database.php (e.g. source + target)
 * and run: php artisan db:to-db --source=... --target=...
 */

return [

    /*
    |--------------------------------------------------------------------------
    | Strict mapping
    |--------------------------------------------------------------------------
    |
    | When true, every mapped target column must exist on the target table and
    | required (NOT NULL, no default) columns must be present in the payload.
    |
    */
    'strict' => true,

    /*
    |--------------------------------------------------------------------------
    | Profile logging channel (Artisan --profile)
    |--------------------------------------------------------------------------
    |
    | Name of a channel defined in config/logging.php (same as Log::channel()).
    | Env: DB_TO_DB_LOG_CHANNEL
    |
    */
    'profile_logging' => env('DB_TO_DB_LOG_CHANNEL', 'db_to_db'),

    /*
    |--------------------------------------------------------------------------
    | Runtime defaults
    |--------------------------------------------------------------------------
    |
    | chunk: rows per read/write batch (source read / mapped batch size).
    | max_rows_per_upsert: hard cap on rows per single insert/upsert statement (PostgreSQL also obeys 65535 bind params).
    |   Lower this if you hit memory limits: Laravel flattens all bindings for each statement.
    | transaction_mode: "batch" (default) or "atomic" (single DB transaction per source row in atomic mode).
    | keyset_column: optional stable ascending column for keyset pagination instead of OFFSET.
    | cli_memory_limit: optional php.ini memory_limit override for the Artisan process (e.g. "512M").
    |
    */
    'runtime' => [
        'defaults' => [
            'chunk' => 500,
            'max_rows_per_upsert' => (int) env('DB_TO_DB_MAX_ROWS_PER_UPSERT', 500),
            'transaction_mode' => 'batch',
        ],

        /*
        |--------------------------------------------------------------------------
        | Chunk loop memory controls
        |--------------------------------------------------------------------------
        |
        | memory_log_every_chunks: log peak memory usage every N chunks (0 = off).
        | force_gc_every_chunks: run gc_collect_cycles() every N chunks (0 = off).
        |
        */
        'memory' => [
            'memory_log_every_chunks' => (int) env('DB_TO_DB_MEMORY_LOG_EVERY_CHUNKS', 0),
            'force_gc_every_chunks' => (int) env('DB_TO_DB_FORCE_GC_EVERY_CHUNKS', 20),
        ],

        // With --profile, chunk steps slower than this (seconds) log as warning. Env: DB_TO_DB_SLOW_CHUNK_SECONDS
        'profile_slow_chunk_seconds' => (float) env('DB_TO_DB_SLOW_CHUNK_SECONDS', 5.0),

        // When true, `tables` must be step_name => [ source_table => target, ... ]. Use `php artisan db:to-db --step=name`.
        // Env: DB_TO_DB_STEPS_IN_TABLES
        'steps_in_tables' => filter_var(env('DB_TO_DB_STEPS_IN_TABLES', false), FILTER_VALIDATE_BOOLEAN),

        'tables' => [
            // 'source_items' => [
            //     'chunk' => 1000,
            //     'transaction_mode' => 'batch',
            //     'keyset_column' => 'id',
            // ],
        ],
        'cli_memory_limit' => env('DB_TO_DB_MEMORY_LIMIT'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Auto transforms (target column types)
    |--------------------------------------------------------------------------
    |
    | When enabled, boolean-like target columns get coerced from common int/string forms.
    | bool_columns lets you force specific columns per target table name.
    |
    */
    'auto_transforms' => [
        'enabled' => true,
        'bool' => true,
        'bool_columns' => [
            // 'items' => ['is_active'],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Source table → one or more target table names
    |--------------------------------------------------------------------------
    |
    | Default: flat map source_table => target (string) or [targets].
    | If runtime.steps_in_tables is true: nested steps, e.g.
    |   'first' => [ 'orders' => 'orders' ], 'second' => [ 'order_lines' => 'order_lines' ]
    | Duplicate source keys across steps are not allowed when running all steps at once.
    |
    */
    'tables' => [
        'source_items' => 'items',
    ],

    /*
    |--------------------------------------------------------------------------
    | Column map: source_table => [ target_table => [ source_col => target_col ] ]
    |--------------------------------------------------------------------------
    */
    'columns' => [
        'source_items' => [
            'items' => [
                'id' => 'id',
                'name' => 'name',
                'created_at' => 'created_at',
            ],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Per-source / per-target transforms (see README)
    |--------------------------------------------------------------------------
    |
    | String rules: trim, null_if_empty, zero_date_to_null
    | Object rules: if_eq; multiply (by); round_precision (precision, optional when/in/then_precision); invoke (using)
    | Closures in a rule list: fn(mixed $value, array $sourceRow): mixed
    |
    */
    'transforms' => [
        'source_items' => [
            'items' => [
                // 'name' => ['trim', 'null_if_empty'],
            ],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Filters on source rows (and optional per-target filter shapes)
    |--------------------------------------------------------------------------
    */
    'filters' => [
        'source_items' => [
            // ['column' => 'active', 'operator' => '=', 'value' => 1],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Upsert conflict keys (when PK cannot be discovered or needs override)
    |--------------------------------------------------------------------------
    |
    | Format: upsert_keys[source_table] = ['id']
    | Or per target: upsert_keys[source_table][target_table] = ['slug']
    |
    */
    'upsert_keys' => [
        // 'source_items' => ['id'],
    ],
];
