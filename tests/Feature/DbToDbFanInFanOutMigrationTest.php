<?php

namespace MB\DbToDb\Tests\Feature;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Tests\TestCase;

class DbToDbFanInFanOutMigrationTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        foreach (['fanout_source', 'fanin_primary_source', 'fanin_secondary_source'] as $table) {
            Schema::connection('db_source')->dropIfExists($table);
        }
        foreach (['fanout_public', 'fanout_audit', 'fanin_unified'] as $table) {
            Schema::connection('db_target')->dropIfExists($table);
        }

        Schema::connection('db_source')->create('fanout_source', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
            $table->string('email');
        });

        Schema::connection('db_source')->create('fanin_primary_source', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('title');
        });

        Schema::connection('db_source')->create('fanin_secondary_source', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('label');
        });

        Schema::connection('db_target')->create('fanout_public', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('display_name');
        });

        Schema::connection('db_target')->create('fanout_audit', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('email');
            $table->string('source_type');
        });

        Schema::connection('db_target')->create('fanin_unified', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('title');
            $table->string('origin');
        });

        DB::connection('db_source')->table('fanout_source')->insert([
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.test'],
            ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.test'],
        ]);

        DB::connection('db_source')->table('fanin_primary_source')->insert([
            ['id' => 101, 'title' => 'Primary A'],
            ['id' => 102, 'title' => 'Primary B'],
        ]);

        DB::connection('db_source')->table('fanin_secondary_source')->insert([
            ['id' => 201, 'label' => 'Secondary A'],
            ['id' => 202, 'label' => 'Secondary B'],
            ['id' => 203, 'label' => 'Secondary C'],
        ]);

        config([
            'dbtodb_mapping.strict' => true,
            'dbtodb_mapping.migrations' => [
                'fan' => [
                    'source' => 'db_source',
                    'target' => 'db_target',
                    'tables' => [
                        'fanout_source' => [
                            'targets' => [
                                'fanout_public' => [
                                    'operation' => 'upsert',
                                    'upsert_keys' => ['id'],
                                    'columns' => [
                                        'id' => 'id',
                                        'name' => 'display_name',
                                    ],
                                ],
                                'fanout_audit' => [
                                    'operation' => 'upsert',
                                    'upsert_keys' => ['id'],
                                    'columns' => [
                                        'id' => 'id',
                                        'email' => 'email',
                                    ],
                                    'values' => [
                                        'source_type' => 'fanout_source',
                                    ],
                                ],
                            ],
                        ],
                        'fanin_primary_source' => [
                            'fanin_unified' => [
                                'columns' => [
                                    'id' => 'id',
                                    'title' => 'title',
                                ],
                                'upsert_keys' => ['id'],
                                'values' => [
                                    'origin' => 'primary',
                                ],
                            ],
                        ],
                        'fanin_secondary_source' => [
                            'fanin_unified' => [
                                'columns' => [
                                    'id' => 'id',
                                    'label' => 'title',
                                ],
                                'upsert_keys' => ['id'],
                                'values' => [
                                    'origin' => 'secondary',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]);
    }

    protected function tearDown(): void
    {
        foreach (['fanout_source', 'fanin_primary_source', 'fanin_secondary_source'] as $table) {
            Schema::connection('db_source')->dropIfExists($table);
        }
        foreach (['fanout_public', 'fanout_audit', 'fanin_unified'] as $table) {
            Schema::connection('db_target')->dropIfExists($table);
        }

        parent::tearDown();
    }

    public function test_one_source_can_fan_out_and_multiple_sources_can_fan_into_one_target_with_static_values(): void
    {
        $startedAt = microtime(true);

        $exit = Artisan::call('db:to-db', [
            '-m' => 'fan',
        ]);

        $durationMs = (int) round((microtime(true) - $startedAt) * 1000);

        $this->assertSame(0, $exit);
        $this->assertSame(2, (int) DB::connection('db_target')->table('fanout_public')->count());
        $this->assertSame(2, (int) DB::connection('db_target')->table('fanout_audit')->count());
        $this->assertSame(5, (int) DB::connection('db_target')->table('fanin_unified')->count());

        $this->assertSame('Alice', DB::connection('db_target')->table('fanout_public')->where('id', 1)->value('display_name'));
        $this->assertSame('fanout_source', DB::connection('db_target')->table('fanout_audit')->where('id', 1)->value('source_type'));
        $this->assertSame('primary', DB::connection('db_target')->table('fanin_unified')->where('id', 101)->value('origin'));
        $this->assertSame('secondary', DB::connection('db_target')->table('fanin_unified')->where('id', 201)->value('origin'));
        $this->assertSame('Secondary A', DB::connection('db_target')->table('fanin_unified')->where('id', 201)->value('title'));

        fwrite(STDERR, sprintf(
            "\nFan in/out migration stats: source_rows=%d target_rows=%d duration_ms=%d\n",
            7,
            9,
            $durationMs,
        ));
    }
}
