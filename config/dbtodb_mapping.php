<?php

/**
 * Example mapping for mb4it/laravel-dbtodb-migration.
 *
 * Copy to config/dbtodb_mapping.php via:
 *   php artisan vendor:publish --tag=dbtodb-migration-config
 *
 * Then define matching connections in config/database.php and run:
 *   php artisan db:to-db
 * or select a named migration:
 *   php artisan db:to-db --migration=catalog
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
    */
    'profile_logging' => env('DB_TO_DB_LOG_CHANNEL', 'db_to_db'),

    /*
    |--------------------------------------------------------------------------
    | Auto-increment / serial / identity sync (after run)
    |--------------------------------------------------------------------------
    */
    'sync_serial_sequences' => filter_var(env('DB_TO_DB_SYNC_SERIAL_SEQUENCES', false), FILTER_VALIDATE_BOOLEAN),

    'sync_serial_sequence_tables' => [
        // 'submodellines',
        // ['table' => 'watches', 'column' => 'idw'],
    ],

    /*
    |--------------------------------------------------------------------------
    | Runtime defaults
    |--------------------------------------------------------------------------
    */
    'runtime' => [
        'defaults' => [
            'chunk' => 500,
            'max_rows_per_upsert' => (int) env('DB_TO_DB_MAX_ROWS_PER_UPSERT', 500),
            'transaction_mode' => 'batch',
        ],

        'memory' => [
            'memory_log_every_chunks' => (int) env('DB_TO_DB_MEMORY_LOG_EVERY_CHUNKS', 0),
            'force_gc_every_chunks' => (int) env('DB_TO_DB_FORCE_GC_EVERY_CHUNKS', 20),
        ],

        'profile_slow_chunk_seconds' => (float) env('DB_TO_DB_SLOW_CHUNK_SECONDS', 5.0),
        'cli_memory_limit' => env('DB_TO_DB_MEMORY_LIMIT'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Auto transforms (target column types)
    |--------------------------------------------------------------------------
    */
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
        'json_invalid' => 'keep', // keep|null|fail
        'bool_columns' => [
            // 'items' => ['is_active'],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Named migrations
    |--------------------------------------------------------------------------
    |
    | The command uses migrations.default when --migration is omitted.
    | Every migration declares its source and target connections here, so simple
    | runs do not need --source or --target:
    |
    |   php artisan db:to-db
    |   php artisan db:to-db --migration=catalog
    |
    | Short table syntax:
    |   source_table => [target_table => [source_column => target_column]]
    |
    | Full table syntax is available when you need source SQL filters, per-target
    | routing filters, transforms, runtime overrides, upsert keys, or ordered
    | steps. Source filters reduce rows before reading; target filters route
    | already-read source rows before target mapping/transforms.
    |
    */
    'migrations' => [
        'default' => [
            'source' => env('DB_TO_DB_SOURCE_CONNECTION', 'source'),
            'target' => env('DB_TO_DB_TARGET_CONNECTION', 'target'),

            'tables' => [
                'source_items' => [
                    'items' => [
                        'id' => 'id',
                        'name' => 'name',
                        'created_at' => 'created_at',
                    ],
                ],
            ],
        ],

        // 'catalog' => [
        //     'source' => 'legacy_mysql',
        //     'target' => 'pgsql_app',
        //     'steps' => [
        //         'dimensions' => [
        //             'tables' => [
        //                 'brands' => [
        //                     'catalog_brands' => ['id' => 'id', 'name' => 'name'],
        //                 ],
        //             ],
        //         ],
        //         'facts' => [
        //             'tables' => [
        //                 'top_banners' => [
        //                     'catalog_banners' => [
        //                         'link' => 'link',
        //                     ],
        //                     'catalog_banners_2' => [
        //                         'name' => 'name',
        //                     ],
        //                 ],
        //             ],
        //         ],
        //     ],
        // ],
    ],
];
