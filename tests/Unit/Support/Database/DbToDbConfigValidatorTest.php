<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbConfigValidator;
use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
use MB\DbToDb\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

class DbToDbConfigValidatorTest extends TestCase
{
    /**
     * @param  array<string, mixed>  $override
     */
    #[DataProvider('invalidConfigProvider')]
    public function test_invalid_configs_include_full_config_path(array $override, string $expectedPath): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage($expectedPath);

        (new DbToDbConfigValidator)->validate($this->configWith($override));
    }

    public function test_resolver_validates_config_before_normalization(): void
    {
        $config = $this->configWith([
            'migrations' => [
                'catalog' => [
                    'source' => 'legacy_mysql',
                    'target' => 'pgsql_app',
                    'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => 'not-a-map',
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.columns');

        (new DbToDbMappingConfigResolver)->resolvePipelinesFromConfig($config, 'catalog');
    }

    /**
     * @return list<array{0: array<string, mixed>, 1: string}>
     */
    public static function invalidConfigProvider(): array
    {
        return [
            'migrations must be map' => [
                ['migrations' => [
                    ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => []],
                ]],
                'dbtodb_mapping.migrations',
            ],
            'migration name must be non-empty string' => [
                ['migrations' => [
                    '' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => []],
                ]],
                'dbtodb_mapping.migrations',
            ],
            'tables must be map' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => ['top_banners']],
                ]],
                'dbtodb_mapping.migrations.catalog.tables',
            ],
            'source table name must be non-empty string' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        '' => ['catalog_banners' => ['id' => 'id']],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables',
            ],
            'columns must be map' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id'],
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.columns',
            ],
            'filters must be array' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'filters' => 'active = 1',
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.filters',
            ],
            'transforms must be array' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'transforms' => 'trim',
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.transforms',
            ],
            'operation must be known' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'operation' => 'replace',
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.operation',
            ],
            'upsert keys must be list' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'upsert_keys' => ['primary' => 'id'],
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.upsert_keys',
            ],
            'runtime chunk must be positive integer' => [
                ['migrations' => [
                    'catalog' => ['source' => 'legacy_mysql', 'target' => 'pgsql_app', 'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'runtime' => ['chunk' => 0],
                            ],
                        ],
                    ]],
                ]],
                'dbtodb_mapping.migrations.catalog.tables.top_banners.catalog_banners.runtime.chunk',
            ],
        ];
    }

    /**
     * @param  array<string, mixed>  $override
     * @return array<string, mixed>
     */
    private function configWith(array $override): array
    {
        return array_replace_recursive($this->validConfig(), $override);
    }

    /** @return array<string, mixed> */
    private function validConfig(): array
    {
        return [
            'migrations' => [
                'catalog' => [
                    'source' => 'legacy_mysql',
                    'target' => 'pgsql_app',
                    'tables' => [
                        'top_banners' => [
                            'catalog_banners' => [
                                'columns' => ['id' => 'id'],
                                'filters' => [],
                                'transforms' => [],
                                'operation' => 'upsert',
                                'upsert_keys' => ['id'],
                                'runtime' => ['chunk' => 100],
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }
}
