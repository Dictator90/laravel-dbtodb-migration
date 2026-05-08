<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use Illuminate\Support\Facades\DB;
use MB\DbToDb\Support\Database\DbToDbFilterEngine;
use MB\DbToDb\Support\Database\DbToDbSourceReader;
use MB\DbToDb\Tests\TestCase;
use RuntimeException;

class DbToDbFilterEngineTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        DB::connection('sqlite')->statement('create table dbtodb_filter_source (id integer primary key, status text, amount integer, min_amount integer, name text, created_at text, deleted_at text, external_id integer)');
        DB::connection('sqlite')->statement('create table dbtodb_filter_refs (external_id integer, enabled integer, tag text)');
    }

    public function test_source_query_generation_supports_complex_filter_dsl(): void
    {
        $reader = new DbToDbSourceReader;
        $query = $reader->buildQuery([
            'connection' => 'sqlite',
            'table' => 'dbtodb_filter_source',
            'filters' => [
                ['column' => 'amount', 'operator' => '>=', 'value' => 10],
                ['column' => 'amount', 'operator' => 'where_column', 'value' => 'min_amount', 'comparison' => '>='],
                ['column' => 'created_at', 'operator' => 'date', 'value' => '2026-05-08'],
                ['column' => 'created_at', 'operator' => 'year', 'value' => 2026],
                ['column' => 'created_at', 'operator' => 'month', 'value' => 5],
                ['column' => 'external_id', 'operator' => 'exists_in', 'value' => [
                    'table' => 'dbtodb_filter_refs',
                    'column' => 'external_id',
                    'where' => [
                        ['column' => 'enabled', 'operator' => '=', 'value' => 1],
                    ],
                ]],
                ['or' => [
                    ['column' => 'status', 'operator' => 'in', 'values' => ['new', 'active']],
                    ['column' => 'name', 'operator' => 'like', 'value' => 'VIP%'],
                ]],
            ],
        ]);

        $sql = strtolower(preg_replace('/["`]/', '', $query->toSql()) ?? '');

        $this->assertStringContainsString('amount >= ?', $sql);
        $this->assertStringContainsString('amount >= min_amount', $sql);
        $this->assertStringContainsString('strftime', $sql);
        $this->assertStringContainsString('external_id in (select distinct external_id from dbtodb_filter_refs where enabled = ?)', $sql);
        $this->assertStringContainsString('(status in (?, ?) or name like ?)', $sql);
    }

    public function test_target_row_matching_supports_same_possible_dsl(): void
    {
        $engine = new DbToDbFilterEngine;
        $filters = [
            ['column' => 'status', 'operator' => 'not_in', 'values' => ['archived']],
            ['column' => 'amount', 'operator' => 'between', 'value' => [10, 20]],
            ['column' => 'amount', 'operator' => 'where_column', 'value' => 'min_amount', 'comparison' => '>='],
            ['column' => 'created_at', 'operator' => 'date', 'value' => '2026-05-08'],
            ['column' => 'created_at', 'operator' => 'year', 'value' => 2026],
            ['column' => 'created_at', 'operator' => 'month', 'value' => 5],
            ['column' => 'deleted_at', 'operator' => 'null'],
            ['column' => 'name', 'operator' => 'not_like', 'value' => 'Test%'],
        ];

        $this->assertTrue($engine->passesRow([
            'status' => 'active',
            'amount' => 15,
            'min_amount' => 10,
            'created_at' => '2026-05-08 12:30:00',
            'deleted_at' => null,
            'name' => 'Real customer',
        ], $filters));

        $this->assertFalse($engine->passesRow([
            'status' => 'active',
            'amount' => 9,
            'min_amount' => 10,
            'created_at' => '2026-05-08 12:30:00',
            'deleted_at' => null,
            'name' => 'Real customer',
        ], $filters));
    }

    public function test_nested_and_or_groups_match_rows(): void
    {
        $engine = new DbToDbFilterEngine;
        $filters = [
            'or' => [
                ['and' => [
                    ['column' => 'status', 'operator' => '=', 'value' => 'active'],
                    ['column' => 'amount', 'operator' => '>', 'value' => 100],
                ]],
                ['and' => [
                    ['column' => 'status', 'operator' => '=', 'value' => 'draft'],
                    ['column' => 'name', 'operator' => 'like', 'value' => 'VIP%'],
                ]],
            ],
        ];

        $this->assertTrue($engine->passesRow(['status' => 'active', 'amount' => 150, 'name' => 'Plain'], $filters));
        $this->assertTrue($engine->passesRow(['status' => 'draft', 'amount' => 1, 'name' => 'VIP user'], $filters));
        $this->assertFalse($engine->passesRow(['status' => 'active', 'amount' => 50, 'name' => 'VIP user'], $filters));
    }

    public function test_invalid_operator_throws_clear_error(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported filter operator "regexp" for column "name".');

        (new DbToDbFilterEngine)->passesRow(['name' => 'abc'], [
            ['column' => 'name', 'operator' => 'regexp', 'value' => '^a'],
        ]);
    }

    public function test_sql_only_exists_in_is_rejected_for_target_matching(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Target filter operator "exists_in" is not supported.');

        (new DbToDbFilterEngine)->passesRow(['external_id' => 10], [
            ['column' => 'external_id', 'operator' => 'exists_in', 'value' => ['table' => 'dbtodb_filter_refs']],
        ]);
    }
}
