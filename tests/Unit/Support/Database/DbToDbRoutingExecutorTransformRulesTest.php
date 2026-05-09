<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbMappingValidator;
use MB\DbToDb\Support\Database\DbToDbRoutingExecutor;
use MB\DbToDb\Support\Database\DbToDbSourceReader;
use MB\DbToDb\Support\Database\DbToDbTargetWriter;
use MB\DbToDb\Tests\TestCase;
use ReflectionMethod;

class DbToDbRoutingExecutorTransformRulesTest extends TestCase
{
    private function invokeApplyTransforms(mixed $value, mixed $definition, array $sourceRow = []): mixed
    {
        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $m = new ReflectionMethod(DbToDbRoutingExecutor::class, 'applyTransforms');
        $m->setAccessible(true);

        return $m->invoke($executor, $value, $definition, $sourceRow);
    }

    public function test_closure_transform_receives_row(): void
    {
        $definition = [
            function (mixed $v, array $row): mixed {
                return ((float) $v) * (float) ($row['k'] ?? 1);
            },
        ];

        $this->assertSame(50.0, $this->invokeApplyTransforms(10.0, $definition, ['k' => 5]));
    }

    public function test_invoke_rule_calls_static_method(): void
    {
        $definition = [
            ['rule' => 'invoke', 'using' => [self::class, 'addTwo']],
        ];

        $this->assertSame(12.0, $this->invokeApplyTransforms(10.0, $definition, ['ignored' => true]));
    }

    public function test_map_row_prefers_target_column_transforms_over_legacy_source_column_transforms(): void
    {
        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $m = new ReflectionMethod(DbToDbRoutingExecutor::class, 'mapRowToTarget');
        $m->setAccessible(true);

        $payload = $m->invoke($executor, [
            'legacy_name' => 'Ada Lovelace',
        ], [
            'connection' => 'sqlite',
            'table' => 'users',
            'map' => ['legacy_name' => 'name'],
            'transforms' => [
                'legacy_name' => ['rule' => 'static', 'value' => 'legacy'],
                'name' => ['rule' => 'static', 'value' => 'target'],
            ],
        ], [
            'table' => 'users',
            'columns' => ['name' => ['type' => 'string']],
            'required' => [],
        ], true);

        $this->assertSame(['name' => 'target'], $payload);
    }

    public function test_map_row_supports_legacy_source_column_transforms_for_backward_compatibility(): void
    {
        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $m = new ReflectionMethod(DbToDbRoutingExecutor::class, 'mapRowToTarget');
        $m->setAccessible(true);

        $payload = $m->invoke($executor, [
            'legacy_name' => '  Ada  ',
        ], [
            'connection' => 'sqlite',
            'table' => 'users',
            'map' => ['legacy_name' => 'name'],
            'transforms' => [
                'legacy_name' => ['trim'],
            ],
        ], [
            'table' => 'users',
            'columns' => ['name' => ['type' => 'string']],
            'required' => [],
        ], true);

        $this->assertSame(['name' => 'Ada'], $payload);
    }


    public function test_map_row_auto_transforms_common_target_types(): void
    {
        config(['dbtodb_mapping.auto_transforms' => [
            'enabled' => true,
            'bool' => true,
            'integer' => true,
            'float' => true,
            'json' => true,
            'date' => true,
            'datetime' => true,
            'empty_string_to_null' => true,
        ]]);

        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $m = new ReflectionMethod(DbToDbRoutingExecutor::class, 'mapRowToTarget');
        $m->setAccessible(true);

        $payload = $m->invoke($executor, [
            'active' => 'yes',
            'count' => '42',
            'price' => '12.50',
            'payload' => ['a' => 1],
            'birthday' => '2026-05-09 10:11:12',
            'seen_at' => '2026-05-09T10:11:12+00:00',
            'optional' => '',
        ], [
            'connection' => 'sqlite',
            'table' => 'typed_users',
            'map' => [
                'active' => 'active',
                'count' => 'count',
                'price' => 'price',
                'payload' => 'payload',
                'birthday' => 'birthday',
                'seen_at' => 'seen_at',
                'optional' => 'optional',
            ],
            'transforms' => [],
        ], [
            'table' => 'typed_users',
            'columns' => [
                'active' => ['type' => 'boolean', 'nullable' => false],
                'count' => ['type' => 'integer', 'nullable' => false],
                'price' => ['type' => 'decimal', 'nullable' => false],
                'payload' => ['type' => 'json', 'nullable' => true],
                'birthday' => ['type' => 'date', 'nullable' => true],
                'seen_at' => ['type' => 'datetime', 'nullable' => true],
                'optional' => ['type' => 'varchar', 'nullable' => true],
            ],
            'required' => [],
        ], true);

        $this->assertSame(true, $payload['active']);
        $this->assertSame(42, $payload['count']);
        $this->assertSame(12.5, $payload['price']);
        $this->assertSame('{"a":1}', $payload['payload']);
        $this->assertSame('2026-05-09', $payload['birthday']);
        $this->assertSame('2026-05-09 10:11:12', $payload['seen_at']);
        $this->assertNull($payload['optional']);
    }

    public function test_target_deduplicate_keeps_last_row_by_target_keys(): void
    {
        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $m = new ReflectionMethod(DbToDbRoutingExecutor::class, 'deduplicateRowsForTarget');
        $m->setAccessible(true);

        $rows = $m->invoke($executor, [
            ['email' => 'a@example.test', 'name' => 'first'],
            ['email' => 'a@example.test', 'name' => 'second'],
            ['email' => 'b@example.test', 'name' => 'third'],
        ], [
            'connection' => 'sqlite',
            'table' => 'users',
            'deduplicate' => ['keys' => ['email'], 'strategy' => 'last'],
        ]);

        $this->assertSame([
            ['email' => 'a@example.test', 'name' => 'second'],
            ['email' => 'b@example.test', 'name' => 'third'],
        ], $rows);
    }

    public static function addTwo(mixed $value, array $row): float
    {
        return (float) $value + 2.0;
    }
}
