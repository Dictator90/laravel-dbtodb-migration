<?php

namespace MB\DbToDb\Tests\Unit\Config;

use MB\DbToDb\Tests\TestCase;

class DbToDbMappingConfigMergedTest extends TestCase
{
    public function test_package_merges_dbtodb_mapping_with_documented_shape(): void
    {
        $config = config('dbtodb_mapping');

        $this->assertIsArray($config);
        $this->assertArrayHasKey('strict', $config);
        $this->assertArrayHasKey('profile_logging', $config);
        $this->assertIsString($config['profile_logging']);
        $this->assertSame('db_to_db', $config['profile_logging']);
        $this->assertArrayHasKey('runtime', $config);
        $this->assertArrayHasKey('defaults', $config['runtime']);
        $this->assertArrayHasKey('max_rows_per_upsert', $config['runtime']['defaults']);
        $this->assertArrayHasKey('memory', $config['runtime']);
        $this->assertArrayHasKey('memory_log_every_chunks', $config['runtime']['memory']);
        $this->assertArrayHasKey('force_gc_every_chunks', $config['runtime']['memory']);
        $this->assertArrayHasKey('profile_slow_chunk_seconds', $config['runtime']);
        $this->assertArrayHasKey('steps_in_tables', $config['runtime']);
        $this->assertIsBool($config['runtime']['steps_in_tables']);
        $this->assertFalse($config['runtime']['steps_in_tables']);
        $this->assertArrayHasKey('tables', $config);
        $this->assertArrayHasKey('source_items', $config['tables']);
        $this->assertArrayHasKey('columns', $config);
        $this->assertArrayHasKey('transforms', $config);
        $this->assertArrayHasKey('filters', $config);
        $this->assertArrayHasKey('upsert_keys', $config);
        $this->assertArrayHasKey('auto_transforms', $config);
    }
}
