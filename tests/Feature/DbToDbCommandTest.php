<?php

namespace MB\DbToDb\Tests\Feature;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Tests\TestCase;

class DbToDbCommandTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        config([
            'dbtodb_mapping.strict' => true,
            'dbtodb_mapping.tables' => ['source_items' => 'items'],
            'dbtodb_mapping.columns' => [
                'source_items' => [
                    'items' => [
                        'id' => 'id',
                        'name' => 'name',
                    ],
                ],
            ],
            'dbtodb_mapping.transforms' => [
                'source_items' => [
                    'items' => [],
                ],
            ],
            'dbtodb_mapping.filters' => [
                'source_items' => [],
            ],
            'dbtodb_mapping.runtime.defaults' => [
                'chunk' => 100,
                'transaction_mode' => 'batch',
            ],
            'dbtodb_mapping.runtime.tables' => [],
        ]);

        Schema::connection('db_source')->dropIfExists('source_items');
        Schema::connection('db_target')->dropIfExists('items');

        Schema::connection('db_source')->create('source_items', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
        });

        Schema::connection('db_target')->create('items', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
        });

        DB::connection('db_source')->table('source_items')->insert([
            'id' => 10,
            'name' => 'alpha',
        ]);
    }

    protected function tearDown(): void
    {
        Schema::connection('db_source')->dropIfExists('source_items');
        Schema::connection('db_target')->dropIfExists('items');

        parent::tearDown();
    }

    public function test_dry_run_does_not_write_to_target(): void
    {
        $exit = Artisan::call('db:to-db', [
            '--dry-run' => true,
            '--source' => 'db_source',
            '--target' => 'db_target',
        ]);

        $this->assertSame(0, $exit);
        $this->assertSame(0, (int) DB::connection('db_target')->table('items')->count());
    }

    public function test_command_copies_rows_to_target(): void
    {
        $exit = Artisan::call('db:to-db', [
            '--source' => 'db_source',
            '--target' => 'db_target',
        ]);

        $this->assertSame(0, $exit);
        $this->assertSame(1, (int) DB::connection('db_target')->table('items')->count());
        $row = (array) DB::connection('db_target')->table('items')->where('id', 10)->first();
        $this->assertSame('alpha', $row['name']);
    }
}
