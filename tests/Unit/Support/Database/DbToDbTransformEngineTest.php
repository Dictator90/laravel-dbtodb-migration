<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Support\Database\DbToDbTransformEngine;
use MB\DbToDb\Tests\TestCase;

class DbToDbTransformEngineTest extends TestCase
{
    private function transform(mixed $value, mixed $definition, array $row = []): mixed
    {
        return (new DbToDbTransformEngine)->apply($value, $definition, $row, 'source_col', 'target_col', 'target_table');
    }

    protected function tearDown(): void
    {
        Schema::dropIfExists('lookup_statuses');

        parent::tearDown();
    }

    public function test_default_rule_uses_value_only_for_null(): void
    {
        $rule = ['rule' => 'default', 'value' => 'fallback'];

        $this->assertSame('fallback', $this->transform(null, $rule));
        $this->assertSame('actual', $this->transform('actual', $rule));
    }

    public function test_static_rule_replaces_current_value(): void
    {
        $this->assertSame('fixed', $this->transform('ignored', ['rule' => 'static', 'value' => 'fixed']));
    }

    public function test_map_and_enum_rules_map_known_values_and_keep_or_default_unknown_values(): void
    {
        $this->assertSame('active', $this->transform('A', ['rule' => 'map', 'map' => ['A' => 'active']]));
        $this->assertSame('unknown', $this->transform('X', ['rule' => 'enum', 'values' => ['A' => 'active'], 'default' => 'unknown']));
        $this->assertSame('X', $this->transform('X', ['rule' => 'map', 'map' => ['A' => 'active']]));
    }

    public function test_replace_rule_replaces_plain_text(): void
    {
        $this->assertSame('a-b-c', $this->transform('a b c', ['rule' => 'replace', 'search' => ' ', 'replace' => '-']));
    }

    public function test_regex_replace_rule_replaces_by_pattern(): void
    {
        $this->assertSame('A-123', $this->transform('A 123', ['rule' => 'regex_replace', 'pattern' => '/\s+/', 'replace' => '-']));
    }

    public function test_substr_rule_extracts_substring(): void
    {
        $this->assertSame('bcd', $this->transform('abcdef', ['rule' => 'substr', 'start' => 1, 'length' => 3]));
    }

    public function test_lower_upper_and_slug_rules_transform_strings(): void
    {
        $this->assertSame('hello', $this->transform('HeLLo', ['rule' => 'lower']));
        $this->assertSame('HELLO', $this->transform('HeLLo', ['rule' => 'upper']));
        $this->assertSame('hello-world', $this->transform('Hello World!', ['rule' => 'slug']));
    }

    public function test_date_format_rule_formats_date_strings(): void
    {
        $this->assertSame('08.05.2026', $this->transform('2026-05-08', ['rule' => 'date_format', 'format' => 'd.m.Y']));
    }

    public function test_cast_rule_casts_supported_types(): void
    {
        $this->assertSame(12, $this->transform('12.7', ['rule' => 'cast', 'type' => 'int']));
        $this->assertSame(12.7, $this->transform('12.7', ['rule' => 'cast', 'type' => 'float']));
        $this->assertSame('12', $this->transform(12, ['rule' => 'cast', 'type' => 'string']));
        $this->assertTrue($this->transform('yes', ['rule' => 'cast', 'type' => 'bool']));
        $this->assertSame('{"a":1}', $this->transform(['a' => 1], ['rule' => 'cast', 'type' => 'json']));
        $this->assertSame(['a' => 1], $this->transform('{"a":1}', ['rule' => 'cast', 'type' => 'array']));
    }

    public function test_concat_rule_combines_current_literals_and_columns(): void
    {
        $this->assertSame('John Doe #7', $this->transform('John', [
            'rule' => 'concat',
            'items' => ['$value', ['value' => 'Doe'], ['column' => 'id']],
            'separator' => ' ',
        ], ['id' => '#7']));
    }

    public function test_coalesce_rule_returns_first_present_value(): void
    {
        $this->assertSame('email@example.test', $this->transform(null, [
            'rule' => 'coalesce',
            'items' => ['$value', ['column' => 'email'], ['value' => 'fallback']],
        ], ['email' => 'email@example.test']));
    }

    public function test_from_columns_rule_builds_value_from_multiple_source_columns(): void
    {
        $this->assertSame('Ada Lovelace', $this->transform(null, [
            'rule' => 'from_columns',
            'columns' => ['first_name', 'last_name'],
            'separator' => ' ',
        ], ['first_name' => 'Ada', 'last_name' => 'Lovelace']));

        $this->assertSame('Lovelace, Ada', $this->transform(null, [
            'rule' => 'from_columns',
            'columns' => ['first_name', 'last_name'],
            'template' => '{last_name}, {first_name}',
        ], ['first_name' => 'Ada', 'last_name' => 'Lovelace']));
    }

    public function test_lookup_rule_reads_database_once_per_cached_key(): void
    {
        Schema::create('lookup_statuses', function (Blueprint $table): void {
            $table->string('code')->primary();
            $table->integer('id');
        });
        DB::table('lookup_statuses')->insert(['code' => 'A', 'id' => 10]);

        $engine = new DbToDbTransformEngine;
        $rule = [
            'rule' => 'lookup',
            'target' => ['connection' => 'sqlite', 'table' => 'lookup_statuses', 'key' => 'code', 'value' => 'id'],
        ];

        $this->assertSame(10, $engine->apply('A', $rule));
        DB::table('lookup_statuses')->where('code', 'A')->update(['id' => 20]);
        $this->assertSame(10, $engine->apply('A', $rule));
    }

    public function test_closure_and_invoke_receive_context_and_remain_supported(): void
    {
        $closure = function (mixed $value, array $row, ?string $sourceColumn, ?string $targetColumn, ?string $targetTable): string {
            return implode(':', [$value, $row['id'], $sourceColumn, $targetColumn, $targetTable]);
        };

        $this->assertSame('v:5:source_col:target_col:target_table', $this->transform('v', $closure, ['id' => 5]));
        $this->assertSame('v:5:source_col:target_col:target_table', $this->transform('v', ['rule' => 'invoke', 'using' => [self::class, 'contextValue']], ['id' => 5]));
    }

    public static function contextValue(mixed $value, array $row, ?string $sourceColumn, ?string $targetColumn, ?string $targetTable): string
    {
        return implode(':', [$value, $row['id'], $sourceColumn, $targetColumn, $targetTable]);
    }
}
