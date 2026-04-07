<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Support\Database\DbToDbMappingValidator;
use MB\DbToDb\Support\Database\DbToDbRoutingExecutor;
use MB\DbToDb\Support\Database\DbToDbSourceReader;
use MB\DbToDb\Support\Database\DbToDbTargetWriter;
use MB\DbToDb\Tests\TestCase;

class DbToDbRoutingExecutorProgressTest extends TestCase
{
    protected function tearDown(): void
    {
        Schema::dropIfExists('dbtodb_src_progress');
        Schema::dropIfExists('dbtodb_dst_progress');

        parent::tearDown();
    }

    public function test_pipeline_progress_callback_tracks_read_and_write_against_source_count(): void
    {
        Schema::create('dbtodb_src_progress', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });
        Schema::create('dbtodb_dst_progress', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });

        for ($i = 1; $i <= 5; $i++) {
            DB::table('dbtodb_src_progress')->insert(['id' => $i]);
        }

        $pipeline = [
            'name' => 'test_progress',
            'strict' => true,
            'transaction_mode' => 'batch',
            'source' => [
                'connection' => 'sqlite',
                'table' => 'dbtodb_src_progress',
                'chunk' => 2,
            ],
            'targets' => [[
                'connection' => 'sqlite',
                'table' => 'dbtodb_dst_progress',
                'operation' => 'upsert',
                'map' => ['id' => 'id'],
            ]],
        ];

        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $events = [];
        $executor->run(
            [$pipeline],
            false,
            false,
            null,
            null,
            false,
            function (array $p, string $phase, int $current, int $total) use (&$events): void {
                $events[] = [$phase, $current, $total];
            },
        );

        $this->assertSame([
            ['read', 0, 5],
            ['read', 2, 5],
            ['write', 2, 5],
            ['read', 4, 5],
            ['write', 4, 5],
            ['read', 5, 5],
            ['write', 5, 5],
        ], $events);
    }

    public function test_pipeline_progress_callback_emits_read_only_on_dry_run(): void
    {
        Schema::create('dbtodb_src_progress', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });
        Schema::create('dbtodb_dst_progress', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });

        DB::table('dbtodb_src_progress')->insert(['id' => 1]);

        $pipeline = [
            'name' => 'test_dry',
            'strict' => true,
            'transaction_mode' => 'batch',
            'source' => [
                'connection' => 'sqlite',
                'table' => 'dbtodb_src_progress',
                'chunk' => 1000,
            ],
            'targets' => [[
                'connection' => 'sqlite',
                'table' => 'dbtodb_dst_progress',
                'operation' => 'upsert',
                'map' => ['id' => 'id'],
            ]],
        ];

        $executor = new DbToDbRoutingExecutor(
            new DbToDbMappingValidator,
            new DbToDbSourceReader,
            new DbToDbTargetWriter,
        );

        $events = [];
        $executor->run(
            [$pipeline],
            true,
            false,
            null,
            null,
            false,
            function (array $p, string $phase, int $current, int $total) use (&$events): void {
                $events[] = [$phase, $current, $total];
            },
        );

        $this->assertSame([
            ['read', 0, 1],
            ['read', 1, 1],
        ], $events);
    }
}
