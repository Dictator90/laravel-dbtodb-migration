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


    public function test_without_migration_uses_legacy_top_level_format(): void
    {
        config(['dbtodb_mapping' => [
            'strict' => true,
            'runtime' => [
                'defaults' => [
                    'chunk' => 25,
                    'transaction_mode' => 'batch',
                ],
                'tables' => [
                    'legacy_users' => [
                        'chunk' => 10,
                        'keyset_column' => 'id',
                    ],
                ],
            ],
            'tables' => [
                'legacy_users' => 'users',
                'legacy_roles' => 'roles',
            ],
            'columns' => [
                'legacy_users' => [
                    'users' => [
                        'id' => 'id',
                        'email' => 'email',
                    ],
                ],
                'legacy_roles' => [
                    'roles' => [
                        'id' => 'id',
                    ],
                ],
            ],
            'transforms' => [
                'legacy_users' => [
                    'email' => ['trim'],
                ],
            ],
            'filters' => [
                'legacy_users' => [
                    ['column' => 'active', 'operator' => '=', 'value' => 1],
                ],
            ],
            'upsert_keys' => [
                'legacy_users' => ['id'],
            ],
            'migrations' => [
                'catalog' => [
                    'source' => 'ignored_source',
                    'target' => 'ignored_target',
                    'tables' => [
                        'top_banners' => ['catalog_banners' => ['id' => 'id']],
                    ],
                ],
            ],
        ]]);

        $pipelines = $this->resolvePipelines(['--tables' => 'legacy_users'], 'legacy_mysql', 'pgsql_app');

        $this->assertCount(1, $pipelines);
        $this->assertSame('legacy:source:legacy_users', $pipelines[0]['name']);
        $this->assertSame('legacy_users', $pipelines[0]['source']['table']);
        $this->assertSame('legacy_mysql', $pipelines[0]['source']['connection']);
        $this->assertSame('pgsql_app', $pipelines[0]['targets'][0]['connection']);
        $this->assertSame(['id'], $pipelines[0]['targets'][0]['keys']);
        $this->assertSame(['email' => ['trim']], $pipelines[0]['targets'][0]['transforms']);
        $this->assertSame(10, $pipelines[0]['source']['chunk']);
        $this->assertSame('id', $pipelines[0]['source']['keyset_column']);
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
}
