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
     * @return array<string, mixed>
     */
    private function mappingWithStepTables(bool $stepsInTables, array $tables): array
    {
        return [
            'strict' => true,
            'runtime' => [
                'steps_in_tables' => $stepsInTables,
                'defaults' => [
                    'chunk' => 100,
                    'max_rows_per_upsert' => 500,
                    'transaction_mode' => 'batch',
                ],
                'memory' => [
                    'memory_log_every_chunks' => 0,
                    'force_gc_every_chunks' => 20,
                ],
                'profile_slow_chunk_seconds' => 5.0,
                'tables' => [],
            ],
            'tables' => $tables,
            'columns' => [
                'z_src' => ['z_tgt' => ['id' => 'id']],
                'a_src' => ['a_tgt' => ['id' => 'id']],
                'dup' => ['t1' => ['id' => 'id']],
            ],
            'transforms' => [
                'z_src' => ['z_tgt' => []],
                'a_src' => ['a_tgt' => []],
                'dup' => ['t1' => []],
            ],
            'filters' => [
                'z_src' => [],
                'a_src' => [],
                'dup' => [],
            ],
        ];
    }

    /**
     * @param  array<string, mixed>  $tables
     */
    private function applyDbtodbMapping(bool $stepsInTables, array $tables): void
    {
        $base = (array) config('dbtodb_mapping');
        $patch = $this->mappingWithStepTables($stepsInTables, $tables);
        $merged = array_replace_recursive($base, $patch);
        $merged['tables'] = $patch['tables'];
        $merged['columns'] = $patch['columns'];
        $merged['transforms'] = $patch['transforms'];
        $merged['filters'] = $patch['filters'];
        $merged['strict'] = $patch['strict'];
        $merged['runtime'] = array_replace_recursive($base['runtime'] ?? [], $patch['runtime']);
        $merged['runtime']['steps_in_tables'] = $stepsInTables;

        config(['dbtodb_mapping' => $merged]);
    }

    public function test_steps_in_tables_false_with_step_option_throws(): void
    {
        $this->applyDbtodbMapping(false, [
            'z_src' => 'z_tgt',
            'a_src' => 'a_tgt',
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, ['--step' => 'first']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('--step option is only valid when');

        $command->resolvePipelines('db_source', 'db_target');
    }

    public function test_steps_in_tables_true_single_step_selects_only_that_step_tables(): void
    {
        $this->applyDbtodbMapping(true, [
            'first' => ['z_src' => 'z_tgt'],
            'second' => ['a_src' => 'a_tgt'],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, ['--step' => 'second']);

        $pipelines = $command->resolvePipelines('db_source', 'db_target');
        $this->assertCount(1, $pipelines);
        $this->assertSame('a_src', $pipelines[0]['source']['table']);
    }

    public function test_steps_in_tables_true_all_steps_merges_in_step_order(): void
    {
        $this->applyDbtodbMapping(true, [
            'first' => ['z_src' => 'z_tgt'],
            'second' => ['a_src' => 'a_tgt'],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $pipelines = $command->resolvePipelines('db_source', 'db_target');
        $this->assertCount(2, $pipelines);
        $this->assertSame('z_src', $pipelines[0]['source']['table']);
        $this->assertSame('a_src', $pipelines[1]['source']['table']);
    }

    public function test_steps_in_tables_true_duplicate_source_across_steps_throws(): void
    {
        $this->applyDbtodbMapping(true, [
            'first' => ['dup' => 't1'],
            'second' => ['dup' => 't2'],
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Duplicate source table "dup"');

        $command->resolvePipelines('db_source', 'db_target');
    }

    public function test_steps_in_tables_false_flat_tables_unchanged(): void
    {
        $this->applyDbtodbMapping(false, [
            'a_src' => 'a_tgt',
            'z_src' => 'z_tgt',
        ]);

        $command = $this->app->make(DbToDbCommand::class);
        $this->bindCommandInput($command, []);

        $pipelines = $command->resolvePipelines('db_source', 'db_target');
        $this->assertCount(2, $pipelines);
        $this->assertSame('a_src', $pipelines[0]['source']['table']);
        $this->assertSame('z_src', $pipelines[1]['source']['table']);
    }
}
