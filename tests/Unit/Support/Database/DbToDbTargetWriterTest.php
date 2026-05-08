<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Support\Database\DbToDbTargetWriter;
use MB\DbToDb\Tests\TestCase;

class DbToDbTargetWriterTest extends TestCase
{
    protected function tearDown(): void
    {
        Schema::dropIfExists('dedupe_upsert_test');
        Schema::dropIfExists('pk_cache_reset_test');
        Schema::dropIfExists('chunk_upsert_test');
        Schema::dropIfExists('insert_operation_test');
        Schema::dropIfExists('truncate_insert_writer_test');

        parent::tearDown();
    }

    public function test_upsert_deduplicates_rows_with_same_conflict_key_in_one_batch(): void
    {
        Schema::create('dedupe_upsert_test', function (Blueprint $table): void {
            $table->integer('a');
            $table->integer('b');
            $table->string('val');
            $table->primary(['a', 'b']);
        });

        $writer = new DbToDbTargetWriter;
        $written = $writer->write([
            'connection' => 'sqlite',
            'table' => 'dedupe_upsert_test',
            'operation' => 'upsert',
            'keys' => ['a', 'b'],
        ], [
            ['a' => 1, 'b' => 2, 'val' => 'first'],
            ['a' => 1, 'b' => 2, 'val' => 'second'],
        ]);

        $this->assertSame(1, $written);
        $this->assertSame('second', (string) DB::table('dedupe_upsert_test')->where('a', 1)->where('b', 2)->value('val'));
    }

    public function test_reset_primary_key_cache_allows_fresh_resolution(): void
    {
        Schema::create('pk_cache_reset_test', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });

        $writer = new DbToDbTargetWriter;
        $writer->write([
            'connection' => 'sqlite',
            'table' => 'pk_cache_reset_test',
            'operation' => 'upsert',
        ], [['id' => 1]]);

        $writer->resetPrimaryKeyCache();

        $writer->write([
            'connection' => 'sqlite',
            'table' => 'pk_cache_reset_test',
            'operation' => 'upsert',
        ], [['id' => 2]]);

        $this->assertSame(2, (int) DB::table('pk_cache_reset_test')->count());
    }

    public function test_insert_operation_does_not_upsert_existing_rows(): void
    {
        Schema::create('insert_operation_test', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('val');
        });

        DB::table('insert_operation_test')->insert(['id' => 1, 'val' => 'existing']);

        $writer = new DbToDbTargetWriter;
        $this->expectException(\Illuminate\Database\QueryException::class);

        $writer->write([
            'connection' => 'sqlite',
            'table' => 'insert_operation_test',
            'operation' => 'insert',
            'keys' => ['id'],
        ], [
            ['id' => 1, 'val' => 'new'],
        ]);
    }

    public function test_truncate_insert_truncates_target_only_once_per_writer_run(): void
    {
        Schema::create('truncate_insert_writer_test', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('val');
        });

        DB::table('truncate_insert_writer_test')->insert(['id' => 99, 'val' => 'old']);

        $writer = new DbToDbTargetWriter;
        $writer->write([
            'connection' => 'sqlite',
            'table' => 'truncate_insert_writer_test',
            'operation' => 'truncate_insert',
        ], [
            ['id' => 1, 'val' => 'first'],
        ]);

        DB::table('truncate_insert_writer_test')->insert(['id' => 100, 'val' => 'between_chunks']);

        $writer->write([
            'connection' => 'sqlite',
            'table' => 'truncate_insert_writer_test',
            'operation' => 'truncate_insert',
        ], [
            ['id' => 2, 'val' => 'second'],
        ]);

        $this->assertSame([1, 2, 100], DB::table('truncate_insert_writer_test')->orderBy('id')->pluck('id')->all());
    }

    public function test_upsert_splits_batch_by_max_rows_per_upsert(): void
    {
        Schema::create('chunk_upsert_test', function (Blueprint $table): void {
            $table->integer('id')->primary();
        });

        config(['dbtodb_mapping.runtime.defaults.max_rows_per_upsert' => 2]);

        $writer = new DbToDbTargetWriter;
        $written = $writer->write([
            'connection' => 'sqlite',
            'table' => 'chunk_upsert_test',
            'operation' => 'upsert',
            'keys' => ['id'],
        ], [
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ]);

        $this->assertSame(5, $written);
        $this->assertSame(5, (int) DB::table('chunk_upsert_test')->count());
    }
}
