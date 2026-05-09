<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
use MB\DbToDb\Tests\TestCase;
use RuntimeException;

class DbToDbTablesStepsResolutionTest extends TestCase
{
    /**
     * @param  array<string, mixed>  $migrations
     */
    private function applyDbtodbMigrations(array $migrations): void
    {
        $base = (array) config('dbtodb_mapping');
        $base['migrations'] = $migrations;
        $base['runtime']['defaults']['chunk'] = 100;
        $base['runtime']['defaults']['transaction_mode'] = 'batch';

        config(['dbtodb_mapping' => $base]);
    }


    /**
     * @return list<array<string, mixed>>
     */
    private function resolvePipelines(array $options = [], ?string $source = null, ?string $target = null): array
    {
        return (new DbToDbMappingConfigResolver())->resolvePipelinesFromConfig(
            (array) config('dbtodb_mapping'),
            isset($options['--migration']) ? (string) $options['--migration'] : null,
            isset($options['--tables']) ? (string) $options['--tables'] : '',
            isset($options['--step']) ? (string) $options['--step'] : '',
            $source,
            $target,
        );
    }

    public function test_default_migration_is_used_when_option_is_omitted(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'z_src' => ['z_tgt' => ['id' => 'id']],
                ],
            ],
            'second' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'a_src' => ['a_tgt' => ['id' => 'id']],
                ],
            ],
        ]);

        $pipelines = $this->resolvePipelines();
        $this->assertCount(1, $pipelines);
        $this->assertSame('z_src', $pipelines[0]['source']['table']);
        $this->assertSame('db_source', $pipelines[0]['source']['connection']);
        $this->assertSame('db_target', $pipelines[0]['targets'][0]['connection']);
    }

    public function test_migration_option_selects_named_migration(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'z_src' => ['z_tgt' => ['id' => 'id']],
                ],
            ],
            'catalog' => [
                'source' => 'legacy_mysql',
                'target' => 'pgsql_app',
                'tables' => [
                    'top_banners' => [
                        'catalog_banners' => ['link' => 'link'],
                    ],
                ],
            ],
        ]);

        $pipelines = $this->resolvePipelines(['--migration' => 'catalog']);
        $this->assertCount(1, $pipelines);
        $this->assertSame('top_banners', $pipelines[0]['source']['table']);
        $this->assertSame('legacy_mysql', $pipelines[0]['source']['connection']);
        $this->assertSame('pgsql_app', $pipelines[0]['targets'][0]['connection']);
        $this->assertSame(['link' => 'link'], $pipelines[0]['targets'][0]['map']);
    }

    public function test_unknown_migration_throws(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'z_src' => ['z_tgt' => ['id' => 'id']],
                ],
            ],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown migration "missing"');

        $this->resolvePipelines(['--migration' => 'missing']);
    }


    public function test_single_step_selects_only_that_step_tables(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'steps' => [
                    'first' => [
                        'tables' => ['z_src' => ['z_tgt' => ['id' => 'id']]],
                    ],
                    'second' => [
                        'tables' => ['a_src' => ['a_tgt' => ['id' => 'id']]],
                    ],
                ],
            ],
        ]);

        $pipelines = $this->resolvePipelines(['--step' => 'second']);
        $this->assertCount(1, $pipelines);
        $this->assertSame('a_src', $pipelines[0]['source']['table']);
    }

    public function test_steps_merge_in_step_order(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'steps' => [
                    'first' => [
                        'tables' => ['z_src' => ['z_tgt' => ['id' => 'id']]],
                    ],
                    'second' => [
                        'tables' => ['a_src' => ['a_tgt' => ['id' => 'id']]],
                    ],
                ],
            ],
        ]);

        $pipelines = $this->resolvePipelines();
        $this->assertCount(2, $pipelines);
        $this->assertSame('z_src', $pipelines[0]['source']['table']);
        $this->assertSame('a_src', $pipelines[1]['source']['table']);
    }

    public function test_duplicate_source_across_steps_throws_when_running_all_steps(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'steps' => [
                    'first' => [
                        'tables' => ['dup' => ['t1' => ['id' => 'id']]],
                    ],
                    'second' => [
                        'tables' => ['dup' => ['t2' => ['id' => 'id']]],
                    ],
                ],
            ],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Duplicate source table "dup"');

        $this->resolvePipelines();
    }

    public function test_named_migration_steps_support_tables_wrapped_under_step_names(): void
    {
        $this->applyDbtodbMigrations([
            'catalog' => [
                'source' => 'legacy_mysql',
                'target' => 'pgsql_app',
                'steps' => [
                    'dimensions' => [
                        'tables' => [
                            'legacy_brands' => [
                                'brands' => [
                                    'id' => 'id',
                                    'name' => 'name',
                                ],
                            ],
                        ],
                    ],
                    'facts' => [
                        'tables' => [
                            'legacy_products' => [
                                'products' => [
                                    'id' => 'id',
                                    'brand_id' => 'brand_id',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $allPipelines = $this->resolvePipelines(['--migration' => 'catalog']);
        $this->assertCount(2, $allPipelines);
        $this->assertSame(['legacy_brands', 'legacy_products'], array_column(array_column($allPipelines, 'source'), 'table'));

        $dimensionPipelines = $this->resolvePipelines([
            '--migration' => 'catalog',
            '--step' => 'dimensions',
        ]);

        $this->assertCount(1, $dimensionPipelines);
        $this->assertSame('catalog:source:legacy_brands', $dimensionPipelines[0]['name']);
        $this->assertSame('legacy_mysql', $dimensionPipelines[0]['source']['connection']);
        $this->assertSame('pgsql_app', $dimensionPipelines[0]['targets'][0]['connection']);
        $this->assertSame('brands', $dimensionPipelines[0]['targets'][0]['table']);
        $this->assertSame(['id' => 'id', 'name' => 'name'], $dimensionPipelines[0]['targets'][0]['map']);
    }

    public function test_same_source_table_is_allowed_in_different_migration_names(): void
    {
        $this->applyDbtodbMigrations([
            'catalog' => [
                'source' => 'legacy_mysql',
                'target' => 'pgsql_app',
                'tables' => [
                    'shared_source' => [
                        'catalog_items' => [
                            'columns' => [
                                'id' => 'catalog_id',
                                'title' => 'name',
                            ],
                            'filters' => [
                                ['column' => 'type', 'operator' => '=', 'value' => 'catalog'],
                            ],
                            'transforms' => [
                                'name' => ['trim'],
                            ],
                        ],
                    ],
                ],
            ],
            'archive' => [
                'source' => 'legacy_mysql',
                'target' => 'warehouse',
                'tables' => [
                    'shared_source' => [
                        'archive_items' => [
                            'columns' => [
                                'id' => 'archive_id',
                                'deleted_at' => 'archived_at',
                            ],
                            'filters' => [
                                ['column' => 'deleted_at', 'operator' => '!=', 'value' => null],
                            ],
                            'transforms' => [
                                'archived_at' => ['date:Y-m-d H:i:s'],
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $catalogPipelines = $this->resolvePipelines(['--migration' => 'catalog']);
        $archivePipelines = $this->resolvePipelines(['--migration' => 'archive']);

        $this->assertCount(1, $catalogPipelines);
        $this->assertCount(1, $archivePipelines);
        $this->assertSame('shared_source', $catalogPipelines[0]['source']['table']);
        $this->assertSame('shared_source', $archivePipelines[0]['source']['table']);
        $this->assertSame('catalog_items', $catalogPipelines[0]['targets'][0]['table']);
        $this->assertSame('archive_items', $archivePipelines[0]['targets'][0]['table']);
        $this->assertSame(['id' => 'catalog_id', 'title' => 'name'], $catalogPipelines[0]['targets'][0]['map']);
        $this->assertSame(['id' => 'archive_id', 'deleted_at' => 'archived_at'], $archivePipelines[0]['targets'][0]['map']);
        $this->assertSame([['column' => 'type', 'operator' => '=', 'value' => 'catalog']], $catalogPipelines[0]['targets'][0]['filters']);
        $this->assertSame([['column' => 'deleted_at', 'operator' => '!=', 'value' => null]], $archivePipelines[0]['targets'][0]['filters']);
        $this->assertSame(['name' => ['trim']], $catalogPipelines[0]['targets'][0]['transforms']);
        $this->assertSame(['archived_at' => ['date:Y-m-d H:i:s']], $archivePipelines[0]['targets'][0]['transforms']);
    }

}
