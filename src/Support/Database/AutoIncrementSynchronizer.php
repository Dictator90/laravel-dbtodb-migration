<?php

declare(strict_types=1);

namespace MB\DbToDb\Support\Database;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use RuntimeException;
use Throwable;

/**
 * After bulk INSERT/UPSERT with explicit PK values, auto-increment counters / sequences
 * may lag behind MAX(pk). This service adjusts them per target DB driver.
 */
final class AutoIncrementSynchronizer
{
    private TargetTablePrimaryKeyResolver $primaryKeyResolver;

    public function __construct(?TargetTablePrimaryKeyResolver $primaryKeyResolver = null)
    {
        $this->primaryKeyResolver = $primaryKeyResolver ?? new TargetTablePrimaryKeyResolver;
    }

    /**
     * @param  list<array{table: string, column?: string|null}>  $definitions  Logical table name (connection prefix applied internally).
     * @param  callable(string): void|null  $onSkip  Invoked when a table is skipped (caller should gate on verbosity).
     */
    public function sync(string $connectionName, array $definitions, ?callable $onSkip = null): void
    {
        if ($definitions === []) {
            return;
        }

        $connection = DB::connection($connectionName);
        $driver = $connection->getDriverName();

        if (! in_array($driver, ['pgsql', 'mysql', 'mariadb', 'sqlite', 'sqlsrv'], true)) {
            if ($onSkip !== null) {
                $onSkip(sprintf(
                    'sync_serial_sequences: unsupported target driver "%s"; nothing to do.',
                    $driver,
                ));
            }

            return;
        }

        foreach ($definitions as $index => $definition) {
            if (! is_array($definition) || ! isset($definition['table'])) {
                throw new RuntimeException(sprintf(
                    'Each auto-increment sync entry must be an array with a "table" key (index %d).',
                    $index,
                ));
            }

            $table = (string) $definition['table'];
            $this->assertSafeSqlIdentifier($table, 'table');

            $column = $definition['column'] ?? null;
            if ($column === null || $column === '') {
                $pkColumns = $this->primaryKeyResolver->resolve($connectionName, $table);
                if (count($pkColumns) === 0) {
                    if ($onSkip !== null) {
                        $onSkip(sprintf(
                            'sync_serial_sequences: skip table "%s": no primary key columns resolved.',
                            $table,
                        ));
                    }

                    continue;
                }
                if (count($pkColumns) > 1) {
                    if ($onSkip !== null) {
                        $onSkip(sprintf(
                            'sync_serial_sequences: skip table "%s": composite primary key (%s) is not supported.',
                            $table,
                            implode(', ', $pkColumns),
                        ));
                    }

                    continue;
                }
                $column = $pkColumns[0];
            }

            $this->assertSafeSqlIdentifier($column, 'column');

            match ($driver) {
                'pgsql' => $this->syncPostgres($connection, $connectionName, $table, $column),
                'mysql', 'mariadb' => $this->syncMysqlFamily($connection, $table, $column, $onSkip),
                'sqlite' => $this->syncSqlite($connection, $table, $column, $onSkip),
                'sqlsrv' => $this->syncSqlsrv($connection, $table, $column, $onSkip),
            };
        }
    }

    private function syncPostgres(Connection $connection, string $connectionName, string $table, string $column): void
    {
        $prefixedTable = $connection->getTablePrefix().$table;

        $row = $connection->selectOne(
            'select pg_get_serial_sequence(?, ?) as sequence',
            [$prefixedTable, $column],
        );

        $sequenceName = $row !== null ? (string) ($row->sequence ?? '') : '';

        if ($sequenceName === '') {
            return;
        }

        $grammar = $connection->getQueryGrammar();
        $wrappedTable = $grammar->wrapTable($table);
        $wrappedColumn = $grammar->wrap($column);

        $pdo = $connection->getPdo();
        if ($pdo === null) {
            throw new RuntimeException(sprintf('Cannot obtain PDO for connection "%s".', $connectionName));
        }

        $sequenceLiteral = $pdo->quote($sequenceName);

        $connection->statement(
            'select setval('.$sequenceLiteral.', coalesce((select max('.$wrappedColumn.') from '.$wrappedTable.'), 1), true)',
        );
    }

    private function syncMysqlFamily(Connection $connection, string $table, string $column, ?callable $onSkip): void
    {
        $grammar = $connection->getQueryGrammar();
        $wrappedTable = $grammar->wrapTable($table);

        try {
            $max = $connection->table($table)->max($column);
            $next = $max === null ? 1 : (int) $max + 1;
            if ($next < 1) {
                $next = 1;
            }

            $connection->statement('ALTER TABLE '.$wrappedTable.' AUTO_INCREMENT = '.$next);
        } catch (Throwable $e) {
            if ($onSkip !== null) {
                $onSkip(sprintf(
                    'sync_serial_sequences: skip table "%s": MySQL/MariaDB AUTO_INCREMENT sync failed (%s).',
                    $table,
                    $e->getMessage(),
                ));
            }
        }
    }

    private function syncSqlite(
        Connection $connection,
        string $table,
        string $column,
        ?callable $onSkip,
    ): void {
        $physicalTable = $connection->getTablePrefix().$table;
        $grammar = $connection->getQueryGrammar();
        $wrappedTable = $grammar->wrapTable($table);
        $wrappedColumn = $grammar->wrap($column);

        $sequenceTableReady = $connection->selectOne(
            "select 1 as one from sqlite_master where type = 'table' and name = 'sqlite_sequence' limit 1",
        );

        if ($sequenceTableReady === null) {
            if ($onSkip !== null) {
                $onSkip(sprintf(
                    'sync_serial_sequences: skip table "%s": sqlite_sequence is not present yet (no AUTOINCREMENT table was ever created in this database file).',
                    $table,
                ));
            }

            return;
        }

        $exists = $connection->selectOne(
            'select 1 as one from sqlite_sequence where name = ? limit 1',
            [$physicalTable],
        );

        if ($exists === null) {
            if ($onSkip !== null) {
                $onSkip(sprintf(
                    'sync_serial_sequences: skip table "%s": not listed in sqlite_sequence (no AUTOINCREMENT on this table).',
                    $table,
                ));
            }

            return;
        }

        $maxRow = $connection->selectOne('select max('.$wrappedColumn.') as m from '.$wrappedTable);
        $max = $maxRow !== null && isset($maxRow->m) && $maxRow->m !== null ? (int) $maxRow->m : 0;
        if ($max < 1) {
            $max = 1;
        }

        $connection->update(
            'update sqlite_sequence set seq = ? where name = ?',
            [$max, $physicalTable],
        );
    }

    private function syncSqlsrv(
        Connection $connection,
        string $table,
        string $column,
        ?callable $onSkip,
    ): void {
        $physicalTable = $connection->getTablePrefix().$table;
        $max = $connection->table($table)->max($column);
        $reseed = $max === null ? 0 : (int) $max;

        $pdo = $connection->getPdo();
        if ($pdo === null) {
            throw new RuntimeException('Cannot obtain PDO for sqlsrv DBCC CHECKIDENT.');
        }

        $quoted = $pdo->quote($physicalTable);

        try {
            $connection->statement('DBCC CHECKIDENT ('.$quoted.', RESEED, '.$reseed.')');
        } catch (Throwable $e) {
            if ($onSkip !== null) {
                $onSkip(sprintf(
                    'sync_serial_sequences: skip table "%s": DBCC CHECKIDENT failed (%s).',
                    $table,
                    $e->getMessage(),
                ));
            }
        }
    }

    private function assertSafeSqlIdentifier(string $value, string $kind): void
    {
        if ($value === '' || ! preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $value)) {
            throw new RuntimeException(sprintf(
                'Invalid %s name "%s" for auto-increment sync (allowed: letters, digits, underscore; must not be empty).',
                $kind,
                $value,
            ));
        }
    }
}
