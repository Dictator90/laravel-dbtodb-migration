<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Facades\DB;
use RuntimeException;
use Throwable;

class DbToDbTargetWriter
{
    /**
     * @var array<string, list<string>>
     */
    private array $resolvedPrimaryKeyCache = [];

    /**
     * @var array<string, true>
     */
    private array $truncatedTargets = [];

    private TargetTablePrimaryKeyResolver $primaryKeyResolver;

    public function __construct(?TargetTablePrimaryKeyResolver $primaryKeyResolver = null)
    {
        $this->primaryKeyResolver = $primaryKeyResolver ?? new TargetTablePrimaryKeyResolver;
    }

    public function resetPrimaryKeyCache(): void
    {
        $this->resolvedPrimaryKeyCache = [];
    }

    public function resetRunState(): void
    {
        $this->resetPrimaryKeyCache();
        $this->truncatedTargets = [];
    }

    /**
     * @param  array<string, mixed>  $target
     * @param  list<array<string, mixed>>  $rows
     */
    public function write(array $target, array $rows): int
    {
        return $this->writeWithReport($target, $rows)['written'];
    }

    /**
     * @param  array<string, mixed>  $target
     * @param  list<array<string, mixed>>  $rows
     * @return array{written:int, skipped:int, errors:list<string>}
     */
    public function writeWithReport(array $target, array $rows): array
    {
        if ($rows === []) {
            return ['written' => 0, 'skipped' => 0, 'errors' => []];
        }

        if ($this->writeErrorMode($target) === 'skip_row') {
            try {
                $written = $this->writeFailFast($target, $rows);

                return ['written' => $written, 'skipped' => 0, 'errors' => []];
            } catch (Throwable $exception) {
                return $this->writeRowsSkippingFailures($target, $rows);
            }
        }

        return ['written' => $this->writeFailFast($target, $rows), 'skipped' => 0, 'errors' => []];
    }

    /**
     * @param  array<string, mixed>  $target
     * @param  list<array<string, mixed>>  $rows
     */
    private function writeFailFast(array $target, array $rows): int
    {
        if ($rows === []) {
            return 0;
        }

        $operation = (string) ($target['operation'] ?? 'upsert');
        $connection = DB::connection((string) $target['connection']);
        $table = (string) $target['table'];
        $rowChunks = $this->chunkRowsForBindLimit($connection->getDriverName(), $rows);

        if ($operation === 'truncate_insert') {
            $truncateKey = $this->truncateKey((string) $target['connection'], $table);
            if (! isset($this->truncatedTargets[$truncateKey])) {
                $this->truncateTable($connection, $table);
                $this->truncatedTargets[$truncateKey] = true;
            }
        }

        if ($operation === 'insert' || $operation === 'truncate_insert') {
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
     * @param  array<string, mixed>  $target
     * @param  list<array<string, mixed>>  $rows
     * @return array{written:int, skipped:int, errors:list<string>}
     */
    private function writeRowsSkippingFailures(array $target, array $rows): array
    {
        $written = 0;
        $skipped = 0;
        $errors = [];

        foreach ($rows as $row) {
            try {
                $written += $this->writeFailFast($target, [$row]);
            } catch (Throwable $exception) {
                $skipped++;
                $message = $this->sanitizeExceptionMessage($exception->getMessage());
                if (! in_array($message, $errors, true)) {
                    $errors[] = $message;
                }
            }
        }

        return ['written' => $written, 'skipped' => $skipped, 'errors' => $errors];
    }

    /**
     * @param  array<string, mixed>  $target
     */
    private function writeErrorMode(array $target): string
    {
        $mode = (string) ($target['on_row_error'] ?? $target['write_error_mode'] ?? 'fail');

        return $mode === 'skip_row' ? 'skip_row' : 'fail';
    }

    private function sanitizeExceptionMessage(string $message): string
    {
        $trimmed = preg_replace('/\s+\(Connection:.*$/s', '', $message);
        if (! is_string($trimmed) || $trimmed === '') {
            $trimmed = $message;
        }

        return substr($trimmed, 0, 1000);
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

    private function truncateTable(object $connection, string $table): void
    {
        if ((string) $connection->getDriverName() === 'sqlite') {
            $connection->table($table)->delete();

            return;
        }

        $connection->table($table)->truncate();
    }

    private function truncateKey(string $connection, string $table): string
    {
        return $connection."\0".$table;
    }

    /**
     * @return list<string>
     */
    private function cachedPrimaryKeys(string $connection, string $table): array
    {
        $cacheKey = $connection."\0".$table;
        if (! array_key_exists($cacheKey, $this->resolvedPrimaryKeyCache)) {
            $this->resolvedPrimaryKeyCache[$cacheKey] = $this->primaryKeyResolver->resolve($connection, $table);
        }

        return $this->resolvedPrimaryKeyCache[$cacheKey];
    }
}
