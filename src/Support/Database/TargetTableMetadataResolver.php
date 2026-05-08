<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Facades\DB;

/**
 * Resolves target table column metadata for supported database drivers.
 */
class TargetTableMetadataResolver
{
    /**
     * @var callable(string): object
     */
    private mixed $connectionResolver;

    /**
     * @param  (callable(string): object)|null  $connectionResolver
     */
    public function __construct(?callable $connectionResolver = null)
    {
        $this->connectionResolver = $connectionResolver ?? static fn (string $connectionName): object => DB::connection($connectionName);
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public function resolve(string $connectionName, string $table): array
    {
        $connection = ($this->connectionResolver)($connectionName);
        $driver = (string) $connection->getDriverName();
        $physicalTable = (string) $connection->getTablePrefix().$table;

        if ($driver === 'sqlite') {
            return $this->resolveSqlite($connection, $physicalTable);
        }

        if ($driver === 'pgsql') {
            return $this->resolvePgsql($connection, $physicalTable);
        }

        if ($driver === 'mysql' || $driver === 'mariadb') {
            return $this->resolveMysqlFamily($connection, $physicalTable);
        }

        if ($driver === 'sqlsrv') {
            return $this->resolveSqlsrv($connection, $physicalTable);
        }

        return [];
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function resolveSqlite(object $connection, string $physicalTable): array
    {
        $rows = $connection->select(sprintf('PRAGMA table_info("%s")', str_replace('"', '""', $physicalTable)));
        $columns = [];
        foreach ($rows as $row) {
            $data = (array) $row;
            $name = (string) ($data['name'] ?? '');
            if ($name === '') {
                continue;
            }

            $columns[$name] = [
                'nullable' => ((int) ($data['notnull'] ?? 0)) === 0,
                'has_default' => array_key_exists('dflt_value', $data) && $data['dflt_value'] !== null,
                'is_pk' => ((int) ($data['pk'] ?? 0)) === 1,
                'type' => strtolower((string) ($data['type'] ?? '')),
            ];
        }

        return $columns;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function resolvePgsql(object $connection, string $physicalTable): array
    {
        $rows = $connection->select(
            <<<'SQL'
            SELECT
                c.column_name,
                (c.is_nullable = 'YES') AS nullable,
                (c.column_default IS NOT NULL) AS has_default,
                c.data_type,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = c.table_schema
                      AND tc.table_name = c.table_name
                      AND kcu.column_name = c.column_name
                ) AS is_pk
            FROM information_schema.columns c
            WHERE c.table_schema = ANY (current_schemas(false))
              AND c.table_name = ?
            SQL,
            [$physicalTable],
        );

        return $this->columnsFromInformationSchemaRows($rows);
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function resolveMysqlFamily(object $connection, string $physicalTable): array
    {
        $rows = $connection->select(
            <<<'SQL'
            SELECT
                c.column_name,
                (c.is_nullable = 'YES') AS nullable,
                (c.column_default IS NOT NULL) AS has_default,
                c.data_type,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_schema = kcu.constraint_schema
                        AND tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = c.table_schema
                      AND tc.table_name = c.table_name
                      AND kcu.column_name = c.column_name
                ) AS is_pk
            FROM information_schema.columns c
            WHERE c.table_schema = DATABASE()
              AND c.table_name = ?
            SQL,
            [$physicalTable],
        );

        return $this->columnsFromInformationSchemaRows($rows);
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function resolveSqlsrv(object $connection, string $physicalTable): array
    {
        $rows = $connection->select(
            <<<'SQL'
            SELECT
                c.COLUMN_NAME AS column_name,
                CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS nullable,
                CASE WHEN c.COLUMN_DEFAULT IS NOT NULL THEN 1 ELSE 0 END AS has_default,
                c.DATA_TYPE AS data_type,
                CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
            FROM INFORMATION_SCHEMA.COLUMNS AS c
            LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND tc.TABLE_NAME = c.TABLE_NAME
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND kcu.TABLE_NAME = c.TABLE_NAME
                AND kcu.COLUMN_NAME = c.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = SCHEMA_NAME()
              AND c.TABLE_NAME = ?
            SQL,
            [$physicalTable],
        );

        return $this->columnsFromInformationSchemaRows($rows);
    }

    /**
     * @param  list<object>  $rows
     * @return array<string, array<string, mixed>>
     */
    private function columnsFromInformationSchemaRows(array $rows): array
    {
        $columns = [];
        foreach ($rows as $row) {
            $name = (string) $this->rowValue($row, 'column_name', '');
            if ($name === '') {
                continue;
            }

            $columns[$name] = [
                'nullable' => $this->toBool($this->rowValue($row, 'nullable', false)),
                'has_default' => $this->toBool($this->rowValue($row, 'has_default', false)),
                'is_pk' => $this->toBool($this->rowValue($row, 'is_pk', false)),
                'type' => strtolower((string) $this->rowValue($row, 'data_type', '')),
            ];
        }

        return $columns;
    }

    private function rowValue(object $row, string $key, mixed $default): mixed
    {
        $data = (array) $row;
        if (array_key_exists($key, $data)) {
            return $data[$key];
        }

        foreach ($data as $rowKey => $value) {
            if (strcasecmp((string) $rowKey, $key) === 0) {
                return $value;
            }
        }

        return $default;
    }

    private function toBool(mixed $value): bool
    {
        if (is_bool($value)) {
            return $value;
        }

        if (is_int($value) || is_float($value)) {
            return $value !== 0 && $value !== 0.0;
        }

        if (is_string($value)) {
            return in_array(strtolower(trim($value)), ['1', 'true', 't', 'yes', 'y'], true);
        }

        return (bool) $value;
    }
}
