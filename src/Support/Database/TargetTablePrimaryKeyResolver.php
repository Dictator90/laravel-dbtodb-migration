<?php

declare(strict_types=1);

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Facades\DB;

/**
 * Resolves primary key column names for a physical target table (respects connection table prefix).
 */
final class TargetTablePrimaryKeyResolver
{
    /**
     * @return list<string> Ordered PK column names; empty if unsupported driver or no PK metadata.
     */
    public function resolve(string $connectionName, string $table): array
    {
        $connection = DB::connection($connectionName);
        $driver = $connection->getDriverName();
        $physicalTable = $connection->getTablePrefix().$table;

        if ($driver === 'sqlite') {
            return $this->resolveSqlite($connectionName, $physicalTable);
        }

        if ($driver === 'pgsql') {
            return $this->resolvePgsql($connectionName, $physicalTable);
        }

        if ($driver === 'mysql' || $driver === 'mariadb') {
            return $this->resolveMysqlFamily($connectionName, $physicalTable);
        }

        if ($driver === 'sqlsrv') {
            return $this->resolveSqlsrv($connectionName, $physicalTable);
        }

        return [];
    }

    /**
     * @return list<string>
     */
    private function resolveSqlite(string $connectionName, string $physicalTable): array
    {
        $rows = DB::connection($connectionName)->select(sprintf(
            'PRAGMA table_info("%s")',
            str_replace('"', '""', $physicalTable),
        ));

        $keys = [];
        foreach ($rows as $row) {
            $data = (array) $row;
            if ((int) ($data['pk'] ?? 0) === 1) {
                $keys[] = (string) ($data['name'] ?? '');
            }
        }

        return array_values(array_filter($keys, static fn (string $k): bool => $k !== ''));
    }

    /**
     * @return list<string>
     */
    private function resolvePgsql(string $connectionName, string $physicalTable): array
    {
        $rows = DB::connection($connectionName)->select(
            <<<'SQL'
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_schema = kcu.constraint_schema
                AND tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = ANY (current_schemas(true))
              AND tc.table_name = ?
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            SQL,
            [$physicalTable],
        );

        return array_values(array_map(static fn (object $row): string => (string) $row->column_name, $rows));
    }

    /**
     * @return list<string>
     */
    private function resolveMysqlFamily(string $connectionName, string $physicalTable): array
    {
        $rows = DB::connection($connectionName)->select(
            <<<'SQL'
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_schema = kcu.constraint_schema
                AND tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = DATABASE()
              AND tc.table_name = ?
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            SQL,
            [$physicalTable],
        );

        return array_values(array_map(static fn (object $row): string => (string) $row->column_name, $rows));
    }

    /**
     * @return list<string>
     */
    private function resolveSqlsrv(string $connectionName, string $physicalTable): array
    {
        $rows = DB::connection($connectionName)->select(
            <<<'SQL'
            SELECT kcu.COLUMN_NAME AS column_name
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = SCHEMA_NAME()
              AND tc.TABLE_NAME = ?
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            SQL,
            [$physicalTable],
        );

        return array_values(array_map(static fn (object $row): string => (string) $row->column_name, $rows));
    }
}
