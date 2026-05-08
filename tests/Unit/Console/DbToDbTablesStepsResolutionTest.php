<?php

namespace MB\DbToDb\Tests\Unit\Console;

use MB\DbToDb\Console\DbToDbCommand;
use MB\DbToDb\Tests\TestCase;
use ReflectionClass;
use RuntimeException;
use Symfony\Component\Console\Input\ArrayInput;

class DbToDbTablesStepsResolutionTest extends TestCase
{
    /**
     * @param  array<string, string|bool|array<string, mixed>>  $options
     */
    private function bindCommandInput(DbToDbCommand $command, array $options): void
    {
        $input = new ArrayInput($options);
        $input->bind($command->getDefinition());

        $ref = new ReflectionClass($command);
        while ($ref !== false) {
            if ($ref->hasProperty('input')) {
                $prop = $ref->getProperty('input');
                $prop->setAccessible(true);
                $prop->setValue($command, $input);

                return;
            }
            $ref = $ref->getParentClass();
        }

        $this->fail('Could not bind Symfony input on DbToDbCommand.');
    }

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

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $pipelines = $command->resolvePipelines();
        $this->assertCount(1, $pipelines);
        $this->assertSame('z_src', $pipelines[0]['source']['table']);
        $this->assertSame('db_source', $pipelines[0]['source']['connection']);
        $this->assertSame('db_target', $pipelines[0]['targets'][0]['connection']);
    }

    public function test_migration_alias_selects_named_migration(): void
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
                'source' => 'source_mysql',
                'target' => 'pgsql_app',
                'tables' => [
                    'top_banners' => [
                        'catalog_banners' => ['link' => 'link'],
                    ],
                ],
            ],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, ['-m' => 'catalog']);

        $pipelines = $command->resolvePipelines();
        $this->assertCount(1, $pipelines);
        $this->assertSame('top_banners', $pipelines[0]['source']['table']);
        $this->assertSame('source_mysql', $pipelines[0]['source']['connection']);
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

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, ['--migration' => 'missing']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown migration "missing"');

        $command->resolvePipelines();
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

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, ['--step' => 'second']);

        $pipelines = $command->resolvePipelines();
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

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $pipelines = $command->resolvePipelines();
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

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Duplicate source table "dup"');

        $command->resolvePipelines();
    }
    public function test_full_table_syntax_resolves_source_and_target_options(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'top_banners' => [
                        'source' => [
                            'filters' => [
                                ['column' => 'active', 'operator' => '=', 'value' => 1],
                            ],
                            'chunk' => 123,
                            'keyset_column' => 'id',
                        ],
                        'targets' => [
                            'catalog_banners' => [
                                'columns' => ['link' => 'link'],
                                'transforms' => ['link' => ['trim']],
                                'filters' => ['kind' => 'main'],
                                'values' => ['origin' => 'catalog'],
                                'upsert_keys' => ['id'],
                                'operation' => 'insert',
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $pipelines = $command->resolvePipelines();

        $this->assertCount(1, $pipelines);
        $this->assertSame('top_banners', $pipelines[0]['source']['table']);
        $this->assertSame(123, $pipelines[0]['source']['chunk']);
        $this->assertSame('id', $pipelines[0]['source']['keyset_column']);
        $this->assertSame([
            ['column' => 'active', 'operator' => '=', 'value' => 1],
        ], $pipelines[0]['source']['filters']);
        $this->assertSame('insert', $pipelines[0]['targets'][0]['operation']);
        $this->assertSame(['link' => 'link'], $pipelines[0]['targets'][0]['map']);
        $this->assertSame(['link' => ['trim']], $pipelines[0]['targets'][0]['transforms']);
        $this->assertSame(['kind' => 'main'], $pipelines[0]['targets'][0]['filters']);
        $this->assertSame(['origin' => 'catalog'], $pipelines[0]['targets'][0]['values']);
        $this->assertSame(['id'], $pipelines[0]['targets'][0]['keys']);
    }

    public function test_string_table_definition_is_not_supported(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'z_src' => 'z_tgt',
                ],
            ],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Invalid table definition for source "z_src"');

        $command->resolvePipelines();
    }

    public function test_list_target_definition_is_not_supported(): void
    {
        $this->applyDbtodbMigrations([
            'default' => [
                'source' => 'db_source',
                'target' => 'db_target',
                'tables' => [
                    'z_src' => ['z_tgt', 'z_tgt_2'],
                ],
            ],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Invalid table definition for source "z_src"');

        $command->resolvePipelines();
    }

}
