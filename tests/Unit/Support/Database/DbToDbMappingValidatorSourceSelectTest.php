<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbMappingValidator;
use MB\DbToDb\Tests\TestCase;
use RuntimeException;

class DbToDbMappingValidatorSourceSelectTest extends TestCase
{
    public function test_accepts_valid_source_select(): void
    {
        $v = new DbToDbMappingValidator;
        $v->validatePipeline([
            'name' => 'p',
            'strict' => true,
            'transaction_mode' => 'batch',
            'source' => [
                'connection' => 'sqlite',
                'table' => 't',
                'chunk' => 10,
                'select' => ['id', 'col_a'],
                'filters' => [],
            ],
            'targets' => [[
                'connection' => 'sqlite',
                'table' => 'u',
                'operation' => 'upsert',
                'map' => ['id' => 'id'],
            ]],
        ]);

        $this->assertTrue(true);
    }

    public function test_rejects_invalid_source_select_entry(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('source.select entries must be non-empty identifiers');

        $v = new DbToDbMappingValidator;
        $v->validatePipeline([
            'name' => 'p',
            'strict' => true,
            'transaction_mode' => 'batch',
            'source' => [
                'connection' => 'sqlite',
                'table' => 't',
                'chunk' => 10,
                'select' => ['id', 'bad-name'],
                'filters' => [],
            ],
            'targets' => [[
                'connection' => 'sqlite',
                'table' => 'u',
                'operation' => 'upsert',
                'map' => ['id' => 'id'],
            ]],
        ]);
    }
}
