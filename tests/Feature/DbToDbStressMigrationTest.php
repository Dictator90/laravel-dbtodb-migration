<?php

namespace MB\DbToDb\Tests\Feature;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Tests\TestCase;

class DbToDbStressMigrationTest extends TestCase
{
    private string $reportFile;

    protected function setUp(): void
    {
        parent::setUp();

        $this->reportFile = sys_get_temp_dir().'/dbtodb-stress-'.uniqid('', true).'.json';

        foreach (['stress_source_items', 'stress_allowed_categories', 'stress_invalid_source'] as $table) {
            Schema::connection('db_source')->dropIfExists($table);
        }
        foreach (['stress_items', 'stress_item_archive', 'stress_invalid_target'] as $table) {
            Schema::connection('db_target')->dropIfExists($table);
        }

        Schema::connection('db_source')->create('stress_allowed_categories', function (Blueprint $table): void {
            $table->integer('category_id')->primary();
            $table->boolean('enabled');
        });

        Schema::connection('db_source')->create('stress_source_items', function (Blueprint $table): void {
            $table->integer('row_id')->primary();
            $table->integer('item_id');
            $table->string('sku');
            $table->string('name');
            $table->string('status');
            $table->integer('category_id');
            $table->decimal('price', 12, 2);
            $table->string('active_raw');
            $table->string('published_at')->nullable();
            $table->string('nullable_text')->nullable();
            $table->integer('archive_flag');
        });

        Schema::connection('db_source')->create('stress_invalid_source', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
        });

        Schema::connection('db_target')->create('stress_items', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('sku');
            $table->string('title');
            $table->integer('price_cents');
            $table->boolean('is_active')->nullable();
            $table->string('published_at')->nullable();
            $table->string('nullable_text')->nullable();
        });

        Schema::connection('db_target')->create('stress_item_archive', function (Blueprint $table): void {
            $table->integer('id');
            $table->string('sku');
            $table->string('title');
        });

        Schema::connection('db_target')->create('stress_invalid_target', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
        });

        DB::connection('db_source')->table('stress_allowed_categories')->insert([
            ['category_id' => 1, 'enabled' => 1],
            ['category_id' => 2, 'enabled' => 0],
            ['category_id' => 3, 'enabled' => 1],
            ['category_id' => 4, 'enabled' => 0],
            ['category_id' => 5, 'enabled' => 1],
            ['category_id' => 6, 'enabled' => 0],
            ['category_id' => 7, 'enabled' => 1],
            ['category_id' => 8, 'enabled' => 0],
            ['category_id' => 9, 'enabled' => 1],
            ['category_id' => 10, 'enabled' => 0],
        ]);

        DB::connection('db_source')->table('stress_invalid_source')->insert([
            'id' => 1,
            'name' => 'bad row',
        ]);

        $batch = [];
        for ($i = 1; $i <= 30000; $i++) {
            $batch[] = [
                'row_id' => $i,
                'item_id' => $i,
                'sku' => sprintf('SKU-%05d', $i),
                'name' => ' Item '.$i.' ',
                'status' => $i % 5 === 0 ? 'draft' : 'published',
                'category_id' => ($i % 10) + 1,
                'price' => $i + 0.25,
                'active_raw' => $i % 4 === 0 ? 'no' : 'yes',
                'published_at' => $i % 14 === 0 ? '0000-00-00 00:00:00' : sprintf('2024-01-%02d 12:00:00', ($i % 28) + 1),
                'nullable_text' => $i % 4 === 0 ? '   ' : 'Text '.$i,
                'archive_flag' => $i % 6 === 0 ? 1 : 0,
            ];

            if (count($batch) === 1000) {
                DB::connection('db_source')->table('stress_source_items')->insert($batch);
                $batch = [];
            }
        }

        if ($batch !== []) {
            DB::connection('db_source')->table('stress_source_items')->insert($batch);
        }

        config([
            'dbtodb_mapping.strict' => true,
            'dbtodb_mapping.runtime.defaults' => [
                'chunk' => 750,
                'max_rows_per_upsert' => 250,
                'transaction_mode' => 'batch',
            ],
            'dbtodb_mapping.auto_transforms.enabled' => true,
            'dbtodb_mapping.auto_transforms.bool' => true,
            'dbtodb_mapping.auto_transforms.bool_columns' => [
                'stress_items' => ['is_active'],
            ],
            'dbtodb_mapping.migrations' => [
                'stress' => [
                    'source' => 'db_source',
                    'target' => 'db_target',
                    'runtime' => [
                        'defaults' => [
                            'chunk' => 750,
                            'max_rows_per_upsert' => 250,
                            'transaction_mode' => 'batch',
                        ],
                    ],
                    'steps' => [
                        'main' => [
                            'tables' => [
                                'stress_source_items' => [
                                    'source' => [
                                        'filters' => [
                                            ['column' => 'status', 'operator' => '=', 'value' => 'published'],
                                            [
                                                'column' => 'category_id',
                                                'operator' => 'exists_in',
                                                'value' => [
                                                    'table' => 'stress_allowed_categories',
                                                    'column' => 'category_id',
                                                    'where' => [
                                                        ['column' => 'enabled', 'operator' => '=', 'value' => 1],
                                                    ],
                                                ],
                                            ],
                                        ],
                                        'keyset_column' => 'row_id',
                                        'chunk' => 750,
                                    ],
                                    'targets' => [
                                        'stress_items' => [
                                            'operation' => 'upsert',
                                            'upsert_keys' => ['id'],
                                            'columns' => [
                                                'item_id' => 'id',
                                                'sku' => 'sku',
                                                'name' => 'title',
                                                'price' => 'price_cents',
                                                'active_raw' => 'is_active',
                                                'published_at' => 'published_at',
                                                'nullable_text' => 'nullable_text',
                                            ],
                                            'transforms' => [
                                                'name' => ['trim'],
                                                'price' => [
                                                    ['rule' => 'multiply', 'by' => 100],
                                                    ['rule' => 'round_precision', 'precision' => 0],
                                                ],
                                                'published_at' => ['zero_date_to_null'],
                                                'nullable_text' => ['trim', 'null_if_empty'],
                                            ],
                                        ],
                                        'stress_item_archive' => [
                                            'operation' => 'insert',
                                            'filters' => [
                                                ['column' => 'archive_flag', 'operator' => '=', 'value' => 1],
                                            ],
                                            'columns' => [
                                                'item_id' => 'id',
                                                'sku' => 'sku',
                                                'name' => 'title',
                                            ],
                                            'transforms' => [
                                                'name' => ['trim'],
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                        ],
                        'invalid' => [
                            'tables' => [
                                'stress_invalid_source' => [
                                    'stress_invalid_target' => [
                                        'id' => 'id',
                                        'name' => 'missing_target_column',
                                    ],
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
        if (isset($this->reportFile) && is_file($this->reportFile)) {
            unlink($this->reportFile);
        }

        foreach (['stress_source_items', 'stress_allowed_categories', 'stress_invalid_source'] as $table) {
            Schema::connection('db_source')->dropIfExists($table);
        }
        foreach (['stress_items', 'stress_item_archive', 'stress_invalid_target'] as $table) {
            Schema::connection('db_target')->dropIfExists($table);
        }

        parent::tearDown();
    }

    public function test_stress_migration_processes_30000_rows_with_filters_transforms_and_multiple_targets(): void
    {
        $startedAt = microtime(true);

        $dryRunExit = Artisan::call('db:to-db', [
            '-m' => 'stress',
            '--step' => 'main',
            '--dry-run' => true,
            '--report-file' => $this->reportFile,
        ]);

        $this->assertSame(0, $dryRunExit);
        $this->assertSame(0, (int) DB::connection('db_target')->table('stress_items')->count());
        $this->assertSame(0, (int) DB::connection('db_target')->table('stress_item_archive')->count());

        $exit = Artisan::call('db:to-db', [
            '-m' => 'stress',
            '--step' => 'main',
            '--report-file' => $this->reportFile,
        ]);

        $this->assertSame(0, $exit);
        $this->assertSame(12000, (int) DB::connection('db_target')->table('stress_items')->count());
        $this->assertSame(4000, (int) DB::connection('db_target')->table('stress_item_archive')->count());

        $row = (array) DB::connection('db_target')->table('stress_items')->where('id', 2)->first();
        $this->assertSame('SKU-00002', $row['sku']);
        $this->assertSame('Item 2', $row['title']);
        $this->assertSame(225, (int) $row['price_cents']);
        $this->assertSame(1, (int) $row['is_active']);
        $this->assertSame('Text 2', $row['nullable_text']);

        $emptyTextRow = (array) DB::connection('db_target')->table('stress_items')->where('id', 4)->first();
        $this->assertNull($emptyTextRow['nullable_text']);
        $this->assertSame(0, (int) $emptyTextRow['is_active']);

        $zeroDateRow = (array) DB::connection('db_target')->table('stress_items')->where('id', 14)->first();
        $this->assertNull($zeroDateRow['published_at']);

        $this->assertNull(DB::connection('db_target')->table('stress_items')->where('id', 5)->first());
        $this->assertNull(DB::connection('db_target')->table('stress_items')->where('id', 1)->first());

        $archiveRow = (array) DB::connection('db_target')->table('stress_item_archive')->where('id', 6)->first();
        $this->assertSame('SKU-00006', $archiveRow['sku']);
        $this->assertSame('Item 6', $archiveRow['title']);

        $report = json_decode((string) file_get_contents($this->reportFile), true, 512, JSON_THROW_ON_ERROR);
        $this->assertFalse($report['dry_run']);
        $this->assertSame('ok', $report['pipelines'][0]['status']);
        $this->assertSame(12000, $report['pipelines'][0]['read']);
        $this->assertSame(16000, $report['pipelines'][0]['written']);

        fwrite(STDERR, sprintf(
            "
Stress migration stats: source_rows=%d matched_rows=%d target_rows=%d written=%d duration_ms=%d
",
            30000,
            12000,
            16000,
            16000,
            (int) round((microtime(true) - $startedAt) * 1000),
        ));
    }

    public function test_stress_migration_continue_on_error_reports_invalid_pipeline_after_valid_pipeline(): void
    {
        $startedAt = microtime(true);

        $exit = Artisan::call('db:to-db', [
            '-m' => 'stress',
            '--continue-on-error' => true,
            '--report-file' => $this->reportFile,
        ]);

        $this->assertSame(0, $exit);
        $this->assertSame(12000, (int) DB::connection('db_target')->table('stress_items')->count());
        $this->assertSame(4000, (int) DB::connection('db_target')->table('stress_item_archive')->count());

        $report = json_decode((string) file_get_contents($this->reportFile), true, 512, JSON_THROW_ON_ERROR);
        $this->assertFalse($report['dry_run']);
        $this->assertTrue($report['continue_on_error']);
        $this->assertCount(2, $report['pipelines']);
        $this->assertSame('ok', $report['pipelines'][0]['status']);
        $this->assertSame('failed', $report['pipelines'][1]['status']);
        $this->assertStringContainsString('missing_target_column', $report['pipelines'][1]['reason']);

        fwrite(STDERR, sprintf(
            "
Stress continue-on-error stats: source_rows=%d successful_target_rows=%d failed_pipelines=%d duration_ms=%d
",
            30001,
            16000,
            1,
            (int) round((microtime(true) - $startedAt) * 1000),
        ));
    }
}
