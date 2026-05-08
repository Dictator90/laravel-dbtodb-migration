<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use MB\DbToDb\Support\Database\TargetTableMetadataResolver;
use MB\DbToDb\Tests\TestCase;

class TargetTableMetadataResolverTest extends TestCase
{
    protected function tearDown(): void
    {
        Schema::dropIfExists('metadata_resolver_sqlite');

        parent::tearDown();
    }

    public function test_sqlite_resolves_current_metadata_fields(): void
    {
        Schema::create('metadata_resolver_sqlite', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->string('name');
            $table->string('status')->nullable()->default('active');
            $table->boolean('enabled')->default(true);
        });

        $columns = (new TargetTableMetadataResolver)->resolve('sqlite', 'metadata_resolver_sqlite');

        $this->assertArrayHasKey('id', $columns);
        $this->assertTrue($columns['id']['is_pk']);
        $this->assertSame('integer', $columns['id']['type']);
        $this->assertFalse($columns['name']['nullable']);
        $this->assertFalse($columns['name']['has_default']);
        $this->assertTrue($columns['status']['nullable']);
        $this->assertTrue($columns['status']['has_default']);
        $this->assertSame('tinyint(1)', $columns['enabled']['type']);
    }

    public function test_mysql_family_resolves_prefixed_information_schema_rows(): void
    {
        $connection = new FakeMetadataConnection('mysql', 'app_', [
            (object) [
                'column_name' => 'id',
                'nullable' => 0,
                'has_default' => '0',
                'is_pk' => 1,
                'data_type' => 'BIGINT',
            ],
            (object) [
                'column_name' => 'name',
                'nullable' => '1',
                'has_default' => '1',
                'is_pk' => '0',
                'data_type' => 'VARCHAR',
            ],
        ]);

        $columns = (new TargetTableMetadataResolver(static fn (): object => $connection))->resolve('db_target', 'users');

        $this->assertSame(['app_users'], $connection->lastBindings);
        $this->assertStringContainsString('information_schema.columns', $connection->lastSql);
        $this->assertStringContainsString('DATABASE()', $connection->lastSql);
        $this->assertSame([
            'nullable' => false,
            'has_default' => false,
            'is_pk' => true,
            'type' => 'bigint',
        ], $columns['id']);
        $this->assertSame([
            'nullable' => true,
            'has_default' => true,
            'is_pk' => false,
            'type' => 'varchar',
        ], $columns['name']);
    }

    public function test_mariadb_uses_mysql_family_metadata_query(): void
    {
        $connection = new FakeMetadataConnection('mariadb', 'tenant_', [
            (object) [
                'column_name' => 'code',
                'nullable' => false,
                'has_default' => true,
                'is_pk' => true,
                'data_type' => 'CHAR',
            ],
        ]);

        $columns = (new TargetTableMetadataResolver(static fn (): object => $connection))->resolve('db_target', 'countries');

        $this->assertSame(['tenant_countries'], $connection->lastBindings);
        $this->assertStringContainsString('information_schema.columns', $connection->lastSql);
        $this->assertSame('char', $columns['code']['type']);
        $this->assertTrue($columns['code']['is_pk']);
    }

    public function test_sqlsrv_resolves_information_schema_rows_and_primary_key_metadata(): void
    {
        $connection = new FakeMetadataConnection('sqlsrv', 'crm_', [
            (object) [
                'COLUMN_NAME' => 'AccountId',
                'NULLABLE' => '0',
                'HAS_DEFAULT' => '0',
                'IS_PK' => '1',
                'DATA_TYPE' => 'UNIQUEIDENTIFIER',
            ],
            (object) [
                'COLUMN_NAME' => 'DisplayName',
                'NULLABLE' => '1',
                'HAS_DEFAULT' => '1',
                'IS_PK' => '0',
                'DATA_TYPE' => 'NVARCHAR',
            ],
        ]);

        $columns = (new TargetTableMetadataResolver(static fn (): object => $connection))->resolve('sqlsrv_target', 'accounts');

        $this->assertSame(['crm_accounts'], $connection->lastBindings);
        $this->assertStringContainsString('INFORMATION_SCHEMA.COLUMNS', $connection->lastSql);
        $this->assertStringContainsString('INFORMATION_SCHEMA.KEY_COLUMN_USAGE', $connection->lastSql);
        $this->assertSame([
            'nullable' => false,
            'has_default' => false,
            'is_pk' => true,
            'type' => 'uniqueidentifier',
        ], $columns['AccountId']);
        $this->assertSame([
            'nullable' => true,
            'has_default' => true,
            'is_pk' => false,
            'type' => 'nvarchar',
        ], $columns['DisplayName']);
    }
}

class FakeMetadataConnection
{
    public string $lastSql = '';

    /**
     * @var list<mixed>
     */
    public array $lastBindings = [];

    /**
     * @param  list<object>  $rows
     */
    public function __construct(
        private string $driver,
        private string $prefix,
        private array $rows,
    ) {}

    public function getDriverName(): string
    {
        return $this->driver;
    }

    public function getTablePrefix(): string
    {
        return $this->prefix;
    }

    /**
     * @param  list<mixed>  $bindings
     * @return list<object>
     */
    public function select(string $query, array $bindings = []): array
    {
        $this->lastSql = $query;
        $this->lastBindings = $bindings;

        return $this->rows;
    }
}
