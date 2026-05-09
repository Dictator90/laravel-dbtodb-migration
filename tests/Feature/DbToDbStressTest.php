<?php

namespace MB\DbToDb\Tests\Feature;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Tests\TestCase;

class DbToDbStressTest extends TestCase
{
    private const USERS_ROWS = 30000;
    private const ORDERS_ROWS = 30000;
    private const PROFILES_ROWS = 15000;
    private const EVENTS_PER_SOURCE = 3000;

    /**
     * @var array<string, array{rows: int, duration: float}>
     */
    private static array $stats = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->createSourceSchema();
        $this->createTargetSchema();
        $this->seedSource();
    }

    protected function tearDown(): void
    {
        foreach ([
            'legacy_users', 'legacy_orders', 'legacy_profiles',
            'events_a', 'events_b',
        ] as $t) {
            Schema::connection('db_source')->dropIfExists($t);
        }
        foreach ([
            'users', 'admins', 'orders', 'audit_log',
            'countries', 'unified_events',
        ] as $t) {
            Schema::connection('db_target')->dropIfExists($t);
        }

        parent::tearDown();
    }

    public function test_full_feature_stress_30k_rows(): void
    {
        config(['dbtodb_mapping' => $this->buildStressConfig()]);

        $startMem = memory_get_usage(true);
        $started = microtime(true);

        $exit = Artisan::call('db:to-db', [
            '--migration' => 'stress',
        ]);

        $duration = microtime(true) - $started;
        $peakMem = memory_get_peak_usage(true);

        $this->assertSame(0, $exit, Artisan::output());

        // Correctness: users count equals source minus filtered (none filtered for users target)
        $usersCount = (int) DB::connection('db_target')->table('users')->count();
        $this->assertSame(self::USERS_ROWS, $usersCount);

        // Admins: only is_admin=1 AND status=active. Seeded ratio ~10% admin AND ~60% active.
        $expectedAdminsMin = (int) (self::USERS_ROWS * 0.04);
        $adminsCount = (int) DB::connection('db_target')->table('admins')->count();
        $this->assertGreaterThan($expectedAdminsMin, $adminsCount);

        // Orders count
        $ordersCount = (int) DB::connection('db_target')->table('orders')->count();
        $this->assertSame(self::ORDERS_ROWS, $ordersCount);

        // Audit log: fan-in from users + orders. Static columns should be set.
        $auditUsers = (int) DB::connection('db_target')->table('audit_log')
            ->where('source_table', 'users')->count();
        $auditOrders = (int) DB::connection('db_target')->table('audit_log')
            ->where('source_table', 'orders')->count();
        $this->assertSame(self::USERS_ROWS, $auditUsers);
        $this->assertSame(self::ORDERS_ROWS, $auditOrders);

        // Each audit row has event_type set via static transform.
        $auditEventTypes = DB::connection('db_target')->table('audit_log')
            ->select('event_type')->distinct()->pluck('event_type')->all();
        sort($auditEventTypes);
        $this->assertSame(['order_imported', 'user_imported'], $auditEventTypes);

        // Spot-check transforms on users.
        $sampleUser = DB::connection('db_target')->table('users')->where('id', 1)->first();
        $this->assertNotNull($sampleUser);
        // status mapped to int (active=1, inactive=0, banned=-1).
        $this->assertContains((int) $sampleUser->status, [1, 0, -1]);
        // email lowered + trimmed (no whitespace, lowercase).
        if ($sampleUser->email !== null) {
            $this->assertSame(strtolower(trim($sampleUser->email)), $sampleUser->email);
        }
        // country_id resolved by lookup or default 0.
        $this->assertIsInt((int) $sampleUser->country_id);
        // profile_data is JSON-castable string.
        if ($sampleUser->profile_data !== null) {
            $decoded = json_decode((string) $sampleUser->profile_data, true);
            $this->assertSame(JSON_ERROR_NONE, json_last_error(), 'profile_data must be valid JSON');
        }

        // bio populated by lookup transform from legacy_profiles for users that have a profile.
        $usersWithBio = (int) DB::connection('db_target')->table('users')
            ->whereNotNull('bio')->where('bio', '!=', '')->count();
        $this->assertGreaterThan(0, $usersWithBio);

        $totalSourceRows = self::USERS_ROWS + self::ORDERS_ROWS;
        $totalTargetRows = $usersCount + $adminsCount + $ordersCount + $auditUsers + $auditOrders;
        $rps = $duration > 0 ? (int) round($totalSourceRows / $duration) : 0;

        self::$stats['stress_30k'] = [
            'source_rows' => $totalSourceRows,
            'target_rows' => $totalTargetRows,
            'duration' => $duration,
            'rps' => $rps,
            'peak_mem_mb' => round($peakMem / 1024 / 1024, 1),
            'mem_delta_mb' => round(($peakMem - $startMem) / 1024 / 1024, 1),
        ];

        fwrite(STDERR, sprintf(
            "\n[stress 30k] source=%d target=%d duration=%.2fs rps=%d peak_mem=%.1fMB delta=%.1fMB admins=%d users_with_bio=%d\n",
            $totalSourceRows,
            $totalTargetRows,
            $duration,
            $rps,
            self::$stats['stress_30k']['peak_mem_mb'],
            self::$stats['stress_30k']['mem_delta_mb'],
            $adminsCount,
            $usersWithBio,
        ));
    }

    public function test_fan_in_from_two_sources_with_static_origin_column(): void
    {
        config(['dbtodb_mapping' => $this->buildFanInOutConfig()]);

        $started = microtime(true);

        $exit = Artisan::call('db:to-db', [
            '--migration' => 'distribution',
        ]);

        $duration = microtime(true) - $started;

        $this->assertSame(0, $exit, Artisan::output());

        // Fan-in: events_a + events_b -> unified_events with static origin per source.
        $countA = (int) DB::connection('db_target')->table('unified_events')
            ->where('origin', 'a')->count();
        $countB = (int) DB::connection('db_target')->table('unified_events')
            ->where('origin', 'b')->count();
        $this->assertSame(self::EVENTS_PER_SOURCE, $countA);
        $this->assertSame(self::EVENTS_PER_SOURCE, $countB);

        // Fan-out: legacy_users -> users + admins (different filters).
        $allUsers = (int) DB::connection('db_target')->table('users')->count();
        $admins = (int) DB::connection('db_target')->table('admins')->count();
        $this->assertGreaterThan(0, $allUsers);
        $this->assertGreaterThan(0, $admins);
        // Admins must be a subset of users.
        $this->assertLessThanOrEqual($allUsers, $admins);

        $rps = $duration > 0 ? (int) round((self::EVENTS_PER_SOURCE * 2 + self::USERS_ROWS) / $duration) : 0;

        fwrite(STDERR, sprintf(
            "\n[fan-in/out] events_a=%d events_b=%d users=%d admins=%d duration=%.2fs rps=%d\n",
            $countA, $countB, $allUsers, $admins, $duration, $rps,
        ));
    }

    private function createSourceSchema(): void
    {
        Schema::connection('db_source')->create('legacy_users', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('email')->nullable();
            $t->string('name')->nullable();
            $t->string('status');
            $t->integer('is_admin');
            $t->string('country_code', 8)->nullable();
            $t->float('balance');
            $t->text('raw_payload')->nullable();
            $t->string('created_at');
        });
        Schema::connection('db_source')->create('legacy_orders', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->integer('user_id');
            $t->float('amount');
            $t->string('currency', 8);
            $t->string('status');
            $t->string('created_at');
        });
        Schema::connection('db_source')->create('legacy_profiles', function (Blueprint $t): void {
            $t->integer('user_id')->primary();
            $t->string('bio')->nullable();
            $t->string('phone')->nullable();
        });
        Schema::connection('db_source')->create('events_a', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('payload');
            $t->string('emitted_at');
        });
        Schema::connection('db_source')->create('events_b', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('payload');
            $t->string('emitted_at');
        });
    }

    private function createTargetSchema(): void
    {
        Schema::connection('db_target')->create('countries', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('code', 8)->unique();
            $t->string('name');
        });
        Schema::connection('db_target')->create('users', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('email')->nullable();
            $t->string('name');
            $t->integer('status');
            $t->boolean('is_admin');
            $t->integer('country_id');
            $t->float('balance');
            $t->text('profile_data')->nullable();
            $t->string('bio')->nullable();
        });
        Schema::connection('db_target')->create('admins', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->string('email');
            $t->string('name');
        });
        Schema::connection('db_target')->create('orders', function (Blueprint $t): void {
            $t->integer('id')->primary();
            $t->integer('user_id');
            $t->float('amount');
            $t->string('currency', 8);
            $t->integer('status');
        });
        Schema::connection('db_target')->create('audit_log', function (Blueprint $t): void {
            $t->increments('id');
            $t->string('source_table');
            $t->integer('ref_id');
            $t->string('event_type');
        });
        Schema::connection('db_target')->create('unified_events', function (Blueprint $t): void {
            $t->increments('id');
            $t->string('origin', 4);
            $t->integer('ref_id');
            $t->string('payload');
        });

        DB::connection('db_target')->table('countries')->insert([
            ['id' => 1, 'code' => 'US', 'name' => 'United States'],
            ['id' => 2, 'code' => 'DE', 'name' => 'Germany'],
            ['id' => 3, 'code' => 'JP', 'name' => 'Japan'],
        ]);
    }

    private function seedSource(): void
    {
        $batch = 1000;
        $statuses = ['active', 'active', 'active', 'active', 'active', 'active', 'inactive', 'inactive', 'inactive', 'banned'];
        $countries = ['US', 'DE', 'JP', 'XX', null];

        // legacy_users
        for ($offset = 0; $offset < self::USERS_ROWS; $offset += $batch) {
            $rows = [];
            for ($i = 1; $i <= $batch && ($offset + $i) <= self::USERS_ROWS; $i++) {
                $id = $offset + $i;
                $isEdgeCase = ($id % 11) === 0;
                $rows[] = [
                    'id' => $id,
                    'email' => $isEdgeCase ? '   ' : ('USER' . $id . '@Example.COM'),
                    'name' => ($id % 7) === 0 ? null : ('  Name ' . $id . '  '),
                    'status' => $statuses[$id % count($statuses)],
                    'is_admin' => ($id % 10 === 0) ? 1 : 0,
                    'country_code' => $countries[$id % count($countries)],
                    'balance' => $id * 1.5,
                    'raw_payload' => json_encode(['k' => 'v', 'id' => $id]),
                    'created_at' => '2024-01-01 00:00:00',
                ];
            }
            DB::connection('db_source')->table('legacy_users')->insert($rows);
        }

        // legacy_orders
        for ($offset = 0; $offset < self::ORDERS_ROWS; $offset += $batch) {
            $rows = [];
            for ($i = 1; $i <= $batch && ($offset + $i) <= self::ORDERS_ROWS; $i++) {
                $id = $offset + $i;
                $rows[] = [
                    'id' => $id,
                    'user_id' => (($id - 1) % self::USERS_ROWS) + 1,
                    'amount' => $id * 0.7,
                    'currency' => $id % 2 ? 'USD' : 'EUR',
                    'status' => $statuses[$id % count($statuses)],
                    'created_at' => '2024-02-01 00:00:00',
                ];
            }
            DB::connection('db_source')->table('legacy_orders')->insert($rows);
        }

        // legacy_profiles
        for ($offset = 0; $offset < self::PROFILES_ROWS; $offset += $batch) {
            $rows = [];
            for ($i = 1; $i <= $batch && ($offset + $i) <= self::PROFILES_ROWS; $i++) {
                $id = $offset + $i;
                $rows[] = [
                    'user_id' => $id,
                    'bio' => 'Bio for user ' . $id,
                    'phone' => '+1' . str_pad((string) $id, 10, '0', STR_PAD_LEFT),
                ];
            }
            DB::connection('db_source')->table('legacy_profiles')->insert($rows);
        }

        // events_a / events_b
        foreach (['events_a' => 'a', 'events_b' => 'b'] as $table => $tag) {
            for ($offset = 0; $offset < self::EVENTS_PER_SOURCE; $offset += $batch) {
                $rows = [];
                for ($i = 1; $i <= $batch && ($offset + $i) <= self::EVENTS_PER_SOURCE; $i++) {
                    $id = $offset + $i;
                    $rows[] = [
                        'id' => $id,
                        'payload' => $tag . '-payload-' . $id,
                        'emitted_at' => '2024-03-01 00:00:00',
                    ];
                }
                DB::connection('db_source')->table($table)->insert($rows);
            }
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function buildStressConfig(): array
    {
        return [
            'strict' => true,
            'profile_logging' => 'db_to_db',
            'sync_serial_sequences' => false,
            'runtime' => [
                'defaults' => [
                    'chunk' => 1000,
                    'max_rows_per_upsert' => 500,
                    'transaction_mode' => 'batch',
                ],
                'memory' => [
                    'memory_log_every_chunks' => 0,
                    'force_gc_every_chunks' => 20,
                ],
                'profile_slow_chunk_seconds' => 10.0,
            ],
            'auto_transforms' => [
                'enabled' => true,
                'bool' => true,
                'bool_columns' => [],
            ],
            'migrations' => [
                'stress' => [
                    'source' => 'db_source',
                    'target' => 'db_target',
                    'strict' => true,
                    'steps' => [
                        'dimensions' => [
                            'tables' => [
                                'legacy_users' => [
                                    'source' => [
                                        'chunk' => 1000,
                                        'keyset_column' => 'id',
                                        'filters' => [
                                            // Nested and/or, exists_in, in, between, not_null, where_column, like.
                                            'and' => [
                                                ['column' => 'id', 'operator' => '>=', 'value' => 1],
                                                ['column' => 'id', 'operator' => '<=', 'value' => self::USERS_ROWS],
                                                ['column' => 'status', 'operator' => 'in', 'value' => ['active', 'inactive', 'banned']],
                                            ],
                                        ],
                                    ],
                                    'targets' => [
                                        'users' => [
                                            'columns' => [
                                                'id' => 'id',
                                                'email' => 'email',
                                                'name' => 'name',
                                                'status' => 'status',
                                                'is_admin' => 'is_admin',
                                                'country_code' => 'country_id',
                                                'balance' => 'balance',
                                                'raw_payload' => 'profile_data',
                                                'created_at' => 'bio',
                                            ],
                                            'transforms' => [
                                                'email' => ['trim', 'lower', 'null_if_empty'],
                                                'name' => [
                                                    'trim',
                                                    ['rule' => 'default', 'value' => 'Anonymous'],
                                                ],
                                                'status' => [
                                                    'rule' => 'map',
                                                    'map' => ['active' => 1, 'inactive' => 0, 'banned' => -1],
                                                    'default' => 0,
                                                ],
                                                'is_admin' => ['rule' => 'cast', 'type' => 'bool'],
                                                'country_id' => [
                                                    'rule' => 'lookup',
                                                    'connection' => 'db_target',
                                                    'table' => 'countries',
                                                    'key' => 'code',
                                                    'value' => 'id',
                                                    'from_column' => 'country_code',
                                                    'default' => 0,
                                                ],
                                                'balance' => ['rule' => 'cast', 'type' => 'float'],
                                                'profile_data' => ['rule' => 'cast', 'type' => 'json'],
                                                'bio' => [
                                                    'rule' => 'lookup',
                                                    'connection' => 'db_source',
                                                    'table' => 'legacy_profiles',
                                                    'key' => 'user_id',
                                                    'value' => 'bio',
                                                    'from_column' => 'id',
                                                ],
                                            ],
                                            'upsert_keys' => ['id'],
                                            'operation' => 'upsert',
                                        ],
                                        'admins' => [
                                            'columns' => [
                                                'id' => 'id',
                                                'email' => 'email',
                                                'name' => 'name',
                                            ],
                                            'filters' => [
                                                ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                                                ['column' => 'status', 'operator' => '=', 'value' => 'active'],
                                            ],
                                            'transforms' => [
                                                'email' => ['trim', 'lower'],
                                                'name' => [
                                                    'trim',
                                                    ['rule' => 'default', 'value' => 'Admin'],
                                                    'upper',
                                                ],
                                            ],
                                            'operation' => 'insert',
                                        ],
                                        'audit_log' => [
                                            'columns' => [
                                                'id' => 'ref_id',
                                                'name' => 'source_table',
                                                'email' => 'event_type',
                                            ],
                                            'transforms' => [
                                                'ref_id' => ['rule' => 'cast', 'type' => 'int'],
                                                'source_table' => ['rule' => 'static', 'value' => 'users'],
                                                'event_type' => ['rule' => 'static', 'value' => 'user_imported'],
                                            ],
                                            'operation' => 'insert',
                                        ],
                                    ],
                                ],
                            ],
                        ],
                        'facts' => [
                            'tables' => [
                                'legacy_orders' => [
                                    'source' => [
                                        'chunk' => 1000,
                                        'keyset_column' => 'id',
                                    ],
                                    'targets' => [
                                        'orders' => [
                                            'columns' => [
                                                'id' => 'id',
                                                'user_id' => 'user_id',
                                                'amount' => 'amount',
                                                'currency' => 'currency',
                                                'status' => 'status',
                                            ],
                                            'transforms' => [
                                                'currency' => ['upper'],
                                                'status' => [
                                                    'rule' => 'map',
                                                    'map' => ['active' => 1, 'inactive' => 0, 'banned' => -1],
                                                    'default' => 0,
                                                ],
                                            ],
                                            'upsert_keys' => ['id'],
                                            'operation' => 'upsert',
                                        ],
                                        'audit_log' => [
                                            'columns' => [
                                                'id' => 'ref_id',
                                                'currency' => 'source_table',
                                                'status' => 'event_type',
                                            ],
                                            'transforms' => [
                                                'ref_id' => ['rule' => 'cast', 'type' => 'int'],
                                                'source_table' => ['rule' => 'static', 'value' => 'orders'],
                                                'event_type' => ['rule' => 'static', 'value' => 'order_imported'],
                                            ],
                                            'operation' => 'insert',
                                        ],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function buildFanInOutConfig(): array
    {
        return [
            'strict' => true,
            'runtime' => [
                'defaults' => [
                    'chunk' => 1000,
                    'max_rows_per_upsert' => 500,
                    'transaction_mode' => 'batch',
                ],
            ],
            'auto_transforms' => ['enabled' => true, 'bool' => true, 'bool_columns' => []],
            'migrations' => [
                'distribution' => [
                    'source' => 'db_source',
                    'target' => 'db_target',
                    'tables' => [
                        // Fan-in: events_a -> unified_events with static origin='a'.
                        'events_a' => [
                            'targets' => [
                                'unified_events' => [
                                    'columns' => [
                                        'id' => 'ref_id',
                                        'payload' => 'payload',
                                        'emitted_at' => 'origin',
                                    ],
                                    'transforms' => [
                                        'ref_id' => ['rule' => 'cast', 'type' => 'int'],
                                        'origin' => ['rule' => 'static', 'value' => 'a'],
                                    ],
                                    'operation' => 'insert',
                                ],
                            ],
                        ],
                        // Fan-in: events_b -> unified_events with static origin='b'.
                        'events_b' => [
                            'targets' => [
                                'unified_events' => [
                                    'columns' => [
                                        'id' => 'ref_id',
                                        'payload' => 'payload',
                                        'emitted_at' => 'origin',
                                    ],
                                    'transforms' => [
                                        'ref_id' => ['rule' => 'cast', 'type' => 'int'],
                                        'origin' => ['rule' => 'static', 'value' => 'b'],
                                    ],
                                    'operation' => 'insert',
                                ],
                            ],
                        ],
                        // Fan-out: legacy_users -> users + admins.
                        'legacy_users' => [
                            'source' => [
                                'chunk' => 1000,
                                'keyset_column' => 'id',
                            ],
                            'targets' => [
                                'users' => [
                                    'columns' => [
                                        'id' => 'id',
                                        'email' => 'email',
                                        'name' => 'name',
                                        'status' => 'status',
                                        'is_admin' => 'is_admin',
                                        'country_code' => 'country_id',
                                        'balance' => 'balance',
                                    ],
                                    'transforms' => [
                                        'email' => ['trim', 'lower', 'null_if_empty'],
                                        'name' => ['trim', ['rule' => 'default', 'value' => 'Anonymous']],
                                        'status' => ['rule' => 'map', 'map' => ['active' => 1, 'inactive' => 0, 'banned' => -1], 'default' => 0],
                                        'is_admin' => ['rule' => 'cast', 'type' => 'bool'],
                                        'country_id' => [
                                            'rule' => 'lookup',
                                            'connection' => 'db_target',
                                            'table' => 'countries',
                                            'key' => 'code',
                                            'value' => 'id',
                                            'from_column' => 'country_code',
                                            'default' => 0,
                                        ],
                                    ],
                                    'upsert_keys' => ['id'],
                                    'operation' => 'upsert',
                                ],
                                'admins' => [
                                    'columns' => [
                                        'id' => 'id',
                                        'email' => 'email',
                                        'name' => 'name',
                                    ],
                                    'filters' => [
                                        ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                                    ],
                                    'transforms' => [
                                        'email' => ['trim', 'lower'],
                                        'name' => ['trim', ['rule' => 'default', 'value' => 'Admin']],
                                    ],
                                    'operation' => 'insert',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }
}
