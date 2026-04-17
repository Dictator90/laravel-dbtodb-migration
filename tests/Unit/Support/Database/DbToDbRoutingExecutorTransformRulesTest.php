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

    public function test_multiply_then_round_precision_matches_legacy_yii_course_27(): void
    {
        $definition = [
            ['rule' => 'multiply', 'by' => 27],
            ['rule' => 'round_precision', 'precision' => -1],
        ];

        $this->assertSame(2680.0, $this->invokeApplyTransforms(99.2, $definition, ['idb' => 1]));
        $this->assertSame(2700.0, $this->invokeApplyTransforms(100.0, $definition, ['idb' => 1]));
    }

    public function test_round_precision_then_precision_when_idb_in_list(): void
    {
        $definition = [
            ['rule' => 'multiply', 'by' => 27],
            [
                'rule' => 'round_precision',
                'precision' => -1,
                'when' => ['column' => 'idb', 'in' => [755]],
                'then_precision' => 0,
            ],
        ];

        $this->assertSame(2678.0, $this->invokeApplyTransforms(99.2, $definition, ['idb' => 755]));
        $this->assertSame(2680.0, $this->invokeApplyTransforms(99.2, $definition, ['idb' => 1]));
    }

    public function test_multiply_returns_null_for_null_input(): void
    {
        $this->assertNull($this->invokeApplyTransforms(null, [['rule' => 'multiply', 'by' => 27]], []));
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

    public static function addTwo(mixed $value, array $row): float
    {
        return (float) $value + 2.0;
    }
}
