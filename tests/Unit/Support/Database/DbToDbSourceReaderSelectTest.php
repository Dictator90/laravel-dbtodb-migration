<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbSourceReader;
use MB\DbToDb\Tests\TestCase;

class DbToDbSourceReaderSelectTest extends TestCase
{
    public function test_build_query_without_select_uses_star_semantics(): void
    {
        $reader = new DbToDbSourceReader;
        $query = $reader->buildQuery([
            'connection' => 'sqlite',
            'table' => 'dbtodb_sel_t',
            'filters' => [],
        ]);

        $sql = $query->toSql();
        $this->assertMatchesRegularExpression('/select\s+\*\s+from/i', $sql);
    }

    public function test_build_query_with_select_lists_qualified_columns_only(): void
    {
        $reader = new DbToDbSourceReader;
        $query = $reader->buildQuery([
            'connection' => 'sqlite',
            'table' => 'dbtodb_sel_t',
            'select' => ['id', 'name'],
            'filters' => [],
        ]);

        $sql = strtolower(preg_replace('/["`]/', '', $query->toSql()) ?? '');
        $this->assertStringContainsString('dbtodb_sel_t.id', $sql);
        $this->assertStringContainsString('dbtodb_sel_t.name', $sql);
        $this->assertStringNotContainsString('*', $sql);
    }
}
