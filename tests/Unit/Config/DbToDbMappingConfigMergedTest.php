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
        $this->assertArrayHasKey('migrations', $config);
        $this->assertArrayHasKey('default', $config['migrations']);
        $this->assertArrayHasKey('source', $config['migrations']['default']);
        $this->assertArrayHasKey('target', $config['migrations']['default']);
        $this->assertArrayHasKey('tables', $config['migrations']['default']);
        $this->assertArrayHasKey('source_items', $config['migrations']['default']['tables']);
        $this->assertArrayHasKey('auto_transforms', $config);
        $this->assertArrayHasKey('sync_serial_sequences', $config);
        $this->assertIsBool($config['sync_serial_sequences']);
        $this->assertArrayHasKey('sync_serial_sequence_tables', $config);
        $this->assertIsArray($config['sync_serial_sequence_tables']);
    }
}
