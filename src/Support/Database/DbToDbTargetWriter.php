<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Facades\DB;
use RuntimeException;

class DbToDbTargetWriter
{
    /**
     * @var array<string, list<string>>
     */
    private array $resolvedPrimaryKeyCache = [];

    public function resetPrimaryKeyCache(): void
    {
        $this->resolvedPrimaryKeyCache = [];
    }

    /**
     * @param  array<string, mixed>  $target
     * @param  list<array<string, mixed>>  $rows
     */
    public function write(array $target, array $rows): int
    {
        if ($rows === []) {
            return 0;
        }

        $operation = (string) ($target['operation'] ?? 'upsert');
        $connection = DB::connection((string) $target['connection']);
        $table = (string) $target['table'];
        $rowChunks = $this->chunkRowsForBindLimit($connection->getDriverName(), $rows);

        if ($operation === 'insert') {
            foreach ($rowChunks as $chunk) {
                $connection->table($table)->insert($chunk);
            }

            return count($rows);
        }

        if ($operation !== 'upsert') {
            throw new RuntimeException(sprintf('Unsupported target operation "%s".', $operation));
        }

        $keys = array_values((array) ($target['keys'] ?? []));
        if ($keys === []) {
            $keys = $this->cachedPrimaryKeys((string) $target['connection'], (string) $target['table']);
        }

        if ($keys === []) {
            throw new RuntimeException(sprintf(
                'Cannot resolve upsert keys for target "%s.%s".',
                (string) $target['connection'],
                (string) $target['table']
            ));
        }

        $rows = $this->dedupeRowsByUpsertKeys($rows, $keys);
        if ($rows === []) {
            return 0;
        }

        $rowChunks = $this->chunkRowsForBindLimit($connection->getDriverName(), $rows);

        $firstRow = $rows[0];
        $updateColumns = array_values(array_filter(
            array_keys($firstRow),
            static fn (string $column): bool => ! in_array($column, $keys, true),
        ));

        foreach ($rowChunks as $chunk) {
            $connection->table($table)->upsert($chunk, $keys, $updateColumns);
        }

        return count($rows);
    }

    /**
     * PostgreSQL rejects ON CONFLICT when the same conflict key appears twice in one INSERT.
     * Later rows win (match typical "last write" expectation for duplicate source rows).
     *
     * @param  list<array<string, mixed>>  $rows
     * @param  list<string>  $keys
     * @return list<array<string, mixed>>
     */
    private function dedupeRowsByUpsertKeys(array $rows, array $keys): array
    {
        if ($rows === [] || $keys === []) {
            return $rows;
        }

        $merged = [];
        foreach ($rows as $row) {
            $keyValues = [];
            foreach ($keys as $key) {
                if (! array_key_exists($key, $row)) {
                    throw new RuntimeException(sprintf(
                        'Upsert row missing key column "%s" (required for deduplication / conflict target).',
                        $key
                    ));
                }
                $keyValues[$key] = $row[$key];
            }
            ksort($keyValues);
            $signature = json_encode($keyValues, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
            $merged[$signature] = $row;
        }

        return array_values($merged);
    }

    /**
     * @param  list<array<string, mixed>>  $rows
     * @return list<list<array<string, mixed>>>
     */
    private function chunkRowsForBindLimit(string $driver, array $rows): array
    {
        if ($rows === []) {
            return [];
        }

        $columnCount = max(1, count($rows[0]));
        $maxBindParameters = $this->maxBindParametersForDriver($driver);
        $rowCap = max(1, (int) config('dbtodb_mapping.runtime.defaults.max_rows_per_upsert', 500));

        $rowsPerChunk = $maxBindParameters === PHP_INT_MAX
            ? $rowCap
            : max(1, min(intdiv($maxBindParameters, $columnCount), $rowCap));

        if ($rowsPerChunk >= count($rows)) {
            return [$rows];
        }

        /** @var list<list<array<string, mixed>>> $chunks */
        return array_values(array_chunk($rows, $rowsPerChunk));
    }

    private function maxBindParametersForDriver(string $driver): int
    {
        if ($driver === 'pgsql') {
            // PostgreSQL hard limit for bind parameters in one statement.
            return 65535;
        }

        return PHP_INT_MAX;
    }

    /**
     * @return list<string>
     */
    private function cachedPrimaryKeys(string $connection, string $table): array
    {
        $cacheKey = $connection."\0".$table;
        if (! array_key_exists($cacheKey, $this->resolvedPrimaryKeyCache)) {
            $this->resolvedPrimaryKeyCache[$cacheKey] = $this->resolvePrimaryKeys($connection, $table);
        }

        return $this->resolvedPrimaryKeyCache[$cacheKey];
    }

    /**
     * @return list<string>
     */
    private function resolvePrimaryKeys(string $connection, string $table): array
    {
        $driver = DB::connection($connection)->getDriverName();

        if ($driver === 'sqlite') {
            $rows = DB::connection($connection)->select(sprintf('PRAGMA table_info("%s")', str_replace('"', '""', $table)));

            $keys = [];
            foreach ($rows as $row) {
                $data = (array) $row;
                if ((int) ($data['pk'] ?? 0) === 1) {
                    $keys[] = (string) ($data['name'] ?? '');
                }
            }

            return array_values(array_filter($keys));
        }

        if ($driver === 'pgsql') {
            // Prefer information_schema + search_path (same idea as DbToDbRoutingExecutor::tableColumnsByName).
            // Raw pg_class/pg_index without schema can miss the table or match the wrong relation.
            $rows = DB::connection($connection)->select(
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
                [$table],
            );

            return array_values(array_map(static fn (object $row): string => (string) $row->column_name, $rows));
        }

        return [];
    }
}
