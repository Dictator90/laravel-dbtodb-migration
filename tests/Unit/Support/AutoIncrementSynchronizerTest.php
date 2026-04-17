<?php

declare(strict_types=1);

namespace MB\DbToDb\Tests\Unit\Support;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Support\Database\AutoIncrementSynchronizer;
use MB\DbToDb\Tests\TestCase;
use RuntimeException;

class AutoIncrementSynchronizerTest extends TestCase
{
    protected function tearDown(): void
    {
        Schema::connection('db_target')->dropIfExists('auto_increment_sync_demo');
        Schema::connection('db_target')->dropIfExists('auto_increment_sync_no_ai');

        parent::tearDown();
    }

    public function test_sync_sqlite_skips_table_without_sqlite_sequence(): void
    {
        Schema::connection('db_target')->create('auto_increment_sync_no_ai', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });

        $skipped = false;
        (new AutoIncrementSynchronizer)->sync('db_target', [['table' => 'auto_increment_sync_no_ai', 'column' => 'id']], function (string $message) use (&$skipped): void {
            if (str_contains($message, 'sqlite_sequence')) {
                $skipped = true;
            }
        });

        $this->assertTrue($skipped);
    }

    public function test_sync_rejects_invalid_table_identifier(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Invalid table name');

        (new AutoIncrementSynchronizer)->sync('db_target', [['table' => 'bad-name', 'column' => 'id']]);
    }

    public function test_sync_rejects_malformed_entry(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Each auto-increment sync entry must be');

        (new AutoIncrementSynchronizer)->sync('db_target', [123]);
    }

    public function test_sync_sqlite_updates_sqlite_sequence_when_autoincrement(): void
    {
        Schema::connection('db_target')->create('auto_increment_sync_demo', function (Blueprint $table): void {
            $table->increments('id');
            $table->string('name')->nullable();
        });

        DB::connection('db_target')->table('auto_increment_sync_demo')->insert([
            'id' => 50,
            'name' => 'x',
        ]);

        DB::connection('db_target')->update(
            'update sqlite_sequence set seq = 1 where name = ?',
            ['auto_increment_sync_demo'],
        );

        (new AutoIncrementSynchronizer)->sync('db_target', [['table' => 'auto_increment_sync_demo', 'column' => null]]);

        $seq = (int) DB::connection('db_target')->table('sqlite_sequence')
            ->where('name', 'auto_increment_sync_demo')
            ->value('seq');
        $this->assertSame(50, $seq);
    }
}
