<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Arr;
use MB\DbToDb\Support\ProfileLoggingChannel;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use RuntimeException;
use Throwable;

class DbToDbRoutingExecutor
{
    /**
     * @var array<string, array<string, array<string, mixed>>>
     */
    private array $targetTableColumnsCache = [];

    private bool $profileLogging = false;

    private float $profileSlowThresholdSeconds = 5.0;

    /**
     * Set while writing a chunk (batch or atomic) for per-target timing logs.
     *
     * @var array{pipeline:string,source_table:string,chunk_index:int}|null
     */
    private ?array $profileWriteChunkContext = null;

    /**
     * @var (callable(array<string, mixed>, string, int, int): void)|null
     */
    private mixed $pipelineProgressCallback = null;

    private int $memoryLogEveryChunks = 0;

    private int $forceGcEveryChunks = 0;

    public function __construct(
        private DbToDbMappingValidator $validator,
        private DbToDbSourceReader $sourceReader,
        private DbToDbTargetWriter $targetWriter,
    ) {}

    private function profileLogChannel(): string
    {
        return ProfileLoggingChannel::resolve();
    }

    /**
     * @param  list<array<string, mixed>>  $pipelines
     * @param  (callable(array<string, mixed>, int, int): void)|null  $beforePipeline  ($pipeline, $index, $total)
     * @param  (callable(): void)|null  $afterPipeline
     * @param  bool  $profileLog  When true, write timing events to the `db_to_db` log channel (no row payloads).
     * @param  (callable(array<string, mixed>, string, int, int): void)|null  $onPipelineProgress  ($pipeline, $phase, $current, $totalRows). $phase is "read" or "write".
     * @return list<array<string, mixed>>
     */
    public function run(
        array $pipelines,
        bool $dryRun,
        bool $continueOnError,
        ?callable $beforePipeline = null,
        ?callable $afterPipeline = null,
        bool $profileLog = false,
        ?callable $onPipelineProgress = null,
    ): array {
        $reports = [];
        $this->runEach(
            $pipelines,
            $dryRun,
            $continueOnError,
            function (array $report) use (&$reports): void {
                $reports[] = $report;
            },
            $beforePipeline,
            $afterPipeline,
            $profileLog,
            $onPipelineProgress,
        );

        return $reports;
    }

    /**
     * @param  list<array<string, mixed>>  $pipelines
     * @param  callable(array<string, mixed>): void  $onPipelineReport
     * @param  (callable(array<string, mixed>, int, int): void)|null  $beforePipeline  ($pipeline, $index, $total)
     * @param  (callable(): void)|null  $afterPipeline
     * @param  bool  $profileLog  When true, write timing events to the `db_to_db` log channel (no row payloads).
     * @param  (callable(array<string, mixed>, string, int, int): void)|null  $onPipelineProgress  ($pipeline, $phase, $current, $totalRows). $phase is "read" or "write".
     */
    public function runEach(
        array $pipelines,
        bool $dryRun,
        bool $continueOnError,
        callable $onPipelineReport,
        ?callable $beforePipeline = null,
        ?callable $afterPipeline = null,
        bool $profileLog = false,
        ?callable $onPipelineProgress = null,
    ): void {
        $this->profileLogging = $profileLog;
        $this->profileSlowThresholdSeconds = (float) config('dbtodb_mapping.runtime.profile_slow_chunk_seconds', 5.0);
        $this->pipelineProgressCallback = $onPipelineProgress;
        $this->memoryLogEveryChunks = max(0, (int) config('dbtodb_mapping.runtime.memory.memory_log_every_chunks', 0));
        $this->forceGcEveryChunks = max(0, (int) config('dbtodb_mapping.runtime.memory.force_gc_every_chunks', 0));
        $total = count($pipelines);
        $this->targetWriter->resetPrimaryKeyCache();

        try {
            foreach ($pipelines as $index => $pipeline) {
                if (is_callable($beforePipeline)) {
                    $beforePipeline($pipeline, $index, $total);
                }

                $pipelineStartedAt = microtime(true);
                try {
                    $report = $this->runPipeline($pipeline, $dryRun);
                } catch (Throwable $exception) {
                    $durationMs = (int) round((microtime(true) - $pipelineStartedAt) * 1000);
                    $safeReason = $this->sanitizeExceptionMessage($exception->getMessage());
                    $report = [
                        'pipeline' => (string) ($pipeline['name'] ?? 'unknown'),
                        'source' => [
                            'connection' => (string) Arr::get($pipeline, 'source.connection', ''),
                            'table' => (string) Arr::get($pipeline, 'source.table', ''),
                        ],
                        'status' => 'failed',
                        'read' => 0,
                        'written' => 0,
                        'targets' => [],
                        'reason' => $safeReason,
                        'duration_ms' => $durationMs,
                    ];

                    if ($this->profileLogging) {
                        Log::channel($this->profileLogChannel())->warning('db_to_db.pipeline.failed', [
                            'pipeline' => $report['pipeline'],
                            'source_table' => (string) Arr::get($pipeline, 'source.table', ''),
                            'duration_ms' => $durationMs,
                            'reason' => $safeReason,
                        ]);
                    }

                    $onPipelineReport($report);

                    if (! $continueOnError) {
                        if (is_callable($afterPipeline)) {
                            $afterPipeline();
                        }
                        throw new RuntimeException($safeReason, (int) $exception->getCode(), $exception);
                    }

                    if (is_callable($afterPipeline)) {
                        $afterPipeline();
                    }

                    continue;
                }

                $onPipelineReport($report);
                if (is_callable($afterPipeline)) {
                    $afterPipeline();
                }
            }
        } finally {
            $this->profileLogging = false;
            $this->profileWriteChunkContext = null;
            $this->pipelineProgressCallback = null;
            $this->memoryLogEveryChunks = 0;
            $this->forceGcEveryChunks = 0;
        }
    }

    /**
     * @param  array<string, mixed>  $pipeline
     * @return array<string, mixed>
     */
    public function runPipeline(array $pipeline, bool $dryRun): array
    {
        $pipelineStartedAt = microtime(true);
        $this->validator->validatePipeline($pipeline);
        $this->targetTableColumnsCache = [];

        $strict = (bool) ($pipeline['strict'] ?? true);
        $chunkSize = max(1, (int) Arr::get($pipeline, 'source.chunk', 1000));
        $transactionMode = (string) ($pipeline['transaction_mode'] ?? 'batch');
        $targets = array_values((array) ($pipeline['targets'] ?? []));
        $pipelineName = (string) ($pipeline['name'] ?? '');
        $sourceTable = (string) Arr::get($pipeline, 'source.table', '');
        $keysetColumn = trim((string) Arr::get($pipeline, 'source.keyset_column', ''));

        if ($this->profileLogging) {
            Log::channel($this->profileLogChannel())->info('db_to_db.pipeline.start', [
                'pipeline' => $pipelineName,
                'source_table' => $sourceTable,
                'chunk' => $chunkSize,
                'keyset_column' => $keysetColumn !== '' ? $keysetColumn : null,
                'transaction_mode' => $transactionMode,
            ]);
        }

        $metaStartedAt = microtime(true);
        $targetMetadata = $this->loadTargetMetadata($targets);
        if ($this->profileLogging) {
            $this->profileLogTimed('pipeline.metadata', [
                'pipeline' => $pipelineName,
                'source_table' => $sourceTable,
                'targets' => array_map(static fn (array $target): array => [
                    'connection' => (string) $target['connection'],
                    'table' => (string) $target['table'],
                ], $targets),
            ], microtime(true) - $metaStartedAt);
        }

        $query = $this->sourceReader->buildQuery((array) $pipeline['source']);
        if ($keysetColumn !== '') {
            $query->orderBy($keysetColumn);
        }

        $sourceTotalRows = (int) (clone $query)->count();
        $this->emitPipelineProgress($pipeline, 'read', 0, $sourceTotalRows);

        $totalRead = 0;
        $totalWritten = 0;
        $targetWritten = array_fill_keys(array_map(fn (array $target): string => $this->targetKey($target), $targets), 0);

        if ($keysetColumn !== '') {
            $lastKey = null;
            $chunkIndex = 0;
            while (true) {
                $readStartedAt = microtime(true);
                $page = clone $query;
                if ($lastKey !== null) {
                    $page->where($keysetColumn, '>', $lastKey);
                }
                $batch = [];
                foreach ($page->limit($chunkSize)->cursor() as $row) {
                    $batch[] = (array) $row;
                }
                $readSeconds = microtime(true) - $readStartedAt;
                if ($batch === []) {
                    break;
                }

                if ($this->profileLogging) {
                    $this->profileLogTimed('chunk.read', [
                        'pipeline' => $pipelineName,
                        'source_table' => $sourceTable,
                        'chunk_index' => $chunkIndex,
                        'rows' => count($batch),
                        'last_key' => $lastKey,
                    ], $readSeconds);
                }

                $totalRead += count($batch);
                $this->emitPipelineProgress($pipeline, 'read', $totalRead, $sourceTotalRows);

                if (! $dryRun) {
                    $this->profileWriteChunkContext = [
                        'pipeline' => $pipelineName,
                        'source_table' => $sourceTable,
                        'chunk_index' => $chunkIndex,
                    ];
                    try {
                        $writeStartedAt = microtime(true);
                        if ($transactionMode === 'atomic') {
                            $written = $this->writeAtomicBatch($batch, $targets, $targetMetadata, $strict, $targetWritten);
                        } else {
                            $written = $this->writeBatchMode($batch, $targets, $targetMetadata, $strict, $targetWritten);
                        }
                        $writeSeconds = microtime(true) - $writeStartedAt;
                        if ($this->profileLogging) {
                            $this->profileLogTimed('chunk.write', [
                                'pipeline' => $pipelineName,
                                'source_table' => $sourceTable,
                                'chunk_index' => $chunkIndex,
                                'written_total' => $written,
                                'transaction_mode' => $transactionMode,
                            ], $writeSeconds);
                        }
                        $totalWritten += $written;
                        $this->emitPipelineProgress($pipeline, 'write', $totalWritten, $sourceTotalRows);
                    } finally {
                        $this->profileWriteChunkContext = null;
                    }
                }

                $lastRow = $batch[array_key_last($batch)];
                if (! array_key_exists($keysetColumn, $lastRow)) {
                    throw new RuntimeException(sprintf(
                        'Keyset pagination: last row is missing column "%s". Check source.keyset_column and SELECT list.',
                        $keysetColumn
                    ));
                }
                $lastKey = $lastRow[$keysetColumn];
                $chunkIndex++;
                $this->afterChunk($pipelineName, $sourceTable, $chunkIndex);
                unset($batch, $lastRow);
            }
        } else {
            $offset = 0;
            $chunkIndex = 0;
            while (true) {
                $readStartedAt = microtime(true);
                $batch = [];
                foreach ((clone $query)->offset($offset)->limit($chunkSize)->cursor() as $row) {
                    $batch[] = (array) $row;
                }
                $readSeconds = microtime(true) - $readStartedAt;
                if ($batch === []) {
                    break;
                }

                if ($this->profileLogging) {
                    $this->profileLogTimed('chunk.read', [
                        'pipeline' => $pipelineName,
                        'source_table' => $sourceTable,
                        'chunk_index' => $chunkIndex,
                        'rows' => count($batch),
                        'offset' => $offset,
                    ], $readSeconds);
                }

                $totalRead += count($batch);
                $this->emitPipelineProgress($pipeline, 'read', $totalRead, $sourceTotalRows);

                if (! $dryRun) {
                    $this->profileWriteChunkContext = [
                        'pipeline' => $pipelineName,
                        'source_table' => $sourceTable,
                        'chunk_index' => $chunkIndex,
                    ];
                    try {
                        $writeStartedAt = microtime(true);
                        if ($transactionMode === 'atomic') {
                            $written = $this->writeAtomicBatch($batch, $targets, $targetMetadata, $strict, $targetWritten);
                        } else {
                            $written = $this->writeBatchMode($batch, $targets, $targetMetadata, $strict, $targetWritten);
                        }
                        $writeSeconds = microtime(true) - $writeStartedAt;
                        if ($this->profileLogging) {
                            $this->profileLogTimed('chunk.write', [
                                'pipeline' => $pipelineName,
                                'source_table' => $sourceTable,
                                'chunk_index' => $chunkIndex,
                                'written_total' => $written,
                                'transaction_mode' => $transactionMode,
                            ], $writeSeconds);
                        }
                        $totalWritten += $written;
                        $this->emitPipelineProgress($pipeline, 'write', $totalWritten, $sourceTotalRows);
                    } finally {
                        $this->profileWriteChunkContext = null;
                    }
                }

                $offset += $chunkSize;
                $chunkIndex++;
                $this->afterChunk($pipelineName, $sourceTable, $chunkIndex);
                unset($batch);
            }
        }

        if ($this->profileLogging) {
            $this->profileLogTimed('pipeline.end', [
                'pipeline' => $pipelineName,
                'source_table' => $sourceTable,
                'read' => $totalRead,
                'written' => $dryRun ? 0 : $totalWritten,
            ], microtime(true) - $pipelineStartedAt);
        }

        return [
            'pipeline' => (string) $pipeline['name'],
            'source' => [
                'connection' => (string) Arr::get($pipeline, 'source.connection'),
                'table' => (string) Arr::get($pipeline, 'source.table'),
            ],
            'status' => 'ok',
            'read' => $totalRead,
            'written' => $dryRun ? 0 : $totalWritten,
            'duration_ms' => (int) round((microtime(true) - $pipelineStartedAt) * 1000),
            'targets' => array_map(function (array $target) use ($targetWritten): array {
                $key = $this->targetKey($target);

                return [
                    'connection' => (string) $target['connection'],
                    'table' => (string) $target['table'],
                    'operation' => (string) ($target['operation'] ?? 'upsert'),
                    'written' => (int) ($targetWritten[$key] ?? 0),
                ];
            }, $targets),
        ];
    }

    /**
     * @param  array<string, mixed>  $context
     */
    private function profileLogTimed(string $event, array $context, float $durationSeconds): void
    {
        if (! $this->profileLogging) {
            return;
        }

        $context['duration_ms'] = (int) round($durationSeconds * 1000);
        $channel = Log::channel($this->profileLogChannel());
        $message = 'db_to_db.'.$event;

        if ($durationSeconds >= $this->profileSlowThresholdSeconds) {
            $context['slow'] = true;
            $channel->warning($message, $context);
        } else {
            $channel->info($message, $context);
        }
    }

    /**
     * @param  'read'|'write'  $phase
     */
    private function emitPipelineProgress(array $pipeline, string $phase, int $current, int $totalRows): void
    {
        $callback = $this->pipelineProgressCallback;
        if (! is_callable($callback)) {
            return;
        }

        $callback($pipeline, $phase, $current, $totalRows);
    }

    /**
     * @param  list<array<string, mixed>>  $batch
     * @param  list<array<string, mixed>>  $targets
     * @param  array<string, array<string, mixed>>  $targetMetadata
     * @param  array<string, int>  $targetWritten
     */
    private function writeBatchMode(array $batch, array $targets, array $targetMetadata, bool $strict, array &$targetWritten): int
    {
        $written = 0;
        foreach ($targets as $target) {
            $key = $this->targetKey($target);
            $rows = [];
            foreach ($batch as $sourceRow) {
                $payload = $this->mapRowToTarget($sourceRow, $target, $targetMetadata[$key], $strict);
                if ($payload !== []) {
                    $rows[] = $payload;
                }
            }
            if ($rows === []) {
                continue;
            }

            $writeStartedAt = microtime(true);
            $count = $this->targetWriter->write($target, $rows);
            if ($this->profileLogging && $this->profileWriteChunkContext !== null) {
                $this->profileLogTimed('chunk.write_target', array_merge($this->profileWriteChunkContext, [
                    'target_connection' => (string) $target['connection'],
                    'target_table' => (string) $target['table'],
                    'rows' => count($rows),
                ]), microtime(true) - $writeStartedAt);
            }
            $targetWritten[$key] += $count;
            $written += $count;
            unset($rows);
        }

        return $written;
    }

    private function afterChunk(string $pipelineName, string $sourceTable, int $chunkIndex): void
    {
        if ($this->profileLogging && $this->memoryLogEveryChunks > 0 && $chunkIndex % $this->memoryLogEveryChunks === 0) {
            Log::channel($this->profileLogChannel())->info('db_to_db.chunk.memory', [
                'pipeline' => $pipelineName,
                'source_table' => $sourceTable,
                'chunk_index' => $chunkIndex,
                'memory_usage' => memory_get_usage(true),
                'memory_peak' => memory_get_peak_usage(true),
            ]);
        }

        if ($this->forceGcEveryChunks > 0 && $chunkIndex % $this->forceGcEveryChunks === 0) {
            gc_collect_cycles();
        }
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
     * @param  list<array<string, mixed>>  $batch
     * @param  list<array<string, mixed>>  $targets
     * @param  array<string, array<string, mixed>>  $targetMetadata
     * @param  array<string, int>  $targetWritten
     */
    private function writeAtomicBatch(array $batch, array $targets, array $targetMetadata, bool $strict, array &$targetWritten): int
    {
        $connections = array_values(array_unique(array_map(static fn (array $target): string => (string) $target['connection'], $targets)));
        if (count($connections) !== 1) {
            throw new RuntimeException('transaction_mode=atomic supports targets on one connection only.');
        }

        $connection = DB::connection($connections[0]);
        $written = 0;

        $connection->transaction(function () use ($batch, $targets, $targetMetadata, $strict, &$targetWritten, &$written): void {
            foreach ($batch as $sourceRow) {
                foreach ($targets as $target) {
                    $payload = $this->mapRowToTarget($sourceRow, $target, $targetMetadata[$this->targetKey($target)], $strict);
                    if ($payload === []) {
                        continue;
                    }

                    $writeStartedAt = microtime(true);
                    $count = $this->targetWriter->write($target, [$payload]);
                    if ($this->profileLogging && $this->profileWriteChunkContext !== null) {
                        $this->profileLogTimed('chunk.write_target', array_merge($this->profileWriteChunkContext, [
                            'target_connection' => (string) $target['connection'],
                            'target_table' => (string) $target['table'],
                            'rows' => 1,
                        ]), microtime(true) - $writeStartedAt);
                    }
                    $targetWritten[$this->targetKey($target)] += $count;
                    $written += $count;
                }
            }
        });

        return $written;
    }

    /**
     * @param  list<array<string, mixed>>  $targets
     * @return array<string, array<string, mixed>>
     */
    private function loadTargetMetadata(array $targets): array
    {
        $metadata = [];

        foreach ($targets as $target) {
            $key = $this->targetKey($target);
            $connection = (string) $target['connection'];
            $table = (string) $target['table'];

            $columns = $this->cachedTableColumnsByName($connection, $table);
            if ($columns === []) {
                throw new RuntimeException(sprintf('Target table not found or has no columns: "%s.%s".', $connection, $table));
            }

            $metadata[$key] = [
                'table' => $table,
                'columns' => $columns,
                'required' => $this->requiredColumns($columns),
            ];
        }

        return $metadata;
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     * @param  array<string, mixed>  $target
     * @param  array<string, mixed>  $metadata
     * @return array<string, mixed>
     */
    private function mapRowToTarget(array $sourceRow, array $target, array $metadata, bool $strict): array
    {
        if (! $this->passesFilters($sourceRow, (array) ($target['filters'] ?? []))) {
            return [];
        }

        /** @var array<string, string> $map */
        $map = (array) ($target['map'] ?? []);
        /** @var array<string, mixed> $transforms */
        $transforms = (array) ($target['transforms'] ?? []);
        /** @var array<string, mixed> $values */
        $values = (array) ($target['values'] ?? []);

        $payload = [];
        if ($map === []) {
            foreach ($sourceRow as $sourceColumn => $value) {
                $targetColumn = (string) $sourceColumn;

                if ($strict && ! array_key_exists($targetColumn, (array) $metadata['columns'])) {
                    throw new RuntimeException(sprintf(
                        'Strict mapping failed for target "%s": target column "%s" (from source "%s") does not exist.',
                        $this->targetKey($target),
                        $targetColumn,
                        $sourceColumn
                    ));
                }

                $payload[$targetColumn] = $this->normalizeByTargetType(
                    $this->applyTransforms($value, $transforms[$sourceColumn] ?? null, $sourceRow),
                    $targetColumn,
                    $metadata,
                );
            }
        } else {
            foreach ($map as $sourceColumn => $targetColumn) {
                if (! is_string($targetColumn) || trim($targetColumn) === '') {
                    throw new RuntimeException(sprintf(
                        'Invalid mapping for target "%s": source column "%s" must map to a non-empty target column.',
                        $this->targetKey($target),
                        $sourceColumn
                    ));
                }

                if ($strict && ! array_key_exists($targetColumn, (array) $metadata['columns'])) {
                    throw new RuntimeException(sprintf(
                        'Strict mapping failed for target "%s": target column "%s" (from source "%s") does not exist.',
                        $this->targetKey($target),
                        $targetColumn,
                        $sourceColumn
                    ));
                }

                if (! array_key_exists($sourceColumn, $sourceRow)) {
                    continue;
                }

                $value = $sourceRow[$sourceColumn];
                $payload[$targetColumn] = $this->normalizeByTargetType(
                    $this->applyTransforms($value, $transforms[$sourceColumn] ?? null, $sourceRow),
                    $targetColumn,
                    $metadata,
                );
            }
        }

        foreach ($values as $targetColumn => $value) {
            if (! is_string($targetColumn) || trim($targetColumn) === '') {
                throw new RuntimeException(sprintf(
                    'Invalid static value for target "%s": target column must be a non-empty string.',
                    $this->targetKey($target)
                ));
            }

            if ($strict && ! array_key_exists($targetColumn, (array) $metadata['columns'])) {
                throw new RuntimeException(sprintf(
                    'Strict mapping failed for target "%s": static target column "%s" does not exist.',
                    $this->targetKey($target),
                    $targetColumn
                ));
            }

            $payload[$targetColumn] = $this->normalizeByTargetType($this->normalizeValue($value), $targetColumn, $metadata);
        }

        if ($strict) {
            $required = (array) ($target['required'] ?? $metadata['required'] ?? []);
            $missing = array_values(array_diff($required, array_keys($payload)));
            if ($missing !== []) {
                throw new RuntimeException(sprintf(
                    'Strict mapping failed for target "%s": missing required target columns: %s.',
                    $this->targetKey($target),
                    implode(', ', $missing)
                ));
            }
        }

        return $payload;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     */
    private function passesFilters(array $row, array $filters): bool
    {
        foreach ($this->normalizeFilters($filters) as $filter) {
            if (! $this->matchesFilter($row, $filter)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<array{column:string, operator:string, value:mixed}>
     */
    private function normalizeFilters(array $filters): array
    {
        if ($filters === []) {
            return [];
        }

        $first = reset($filters);
        if (is_array($first) && array_key_exists('column', $first)) {
            /** @var list<array{column:string, operator:string, value:mixed}> $filters */
            return array_map(static function (array $filter): array {
                $operator = strtolower((string) ($filter['operator'] ?? '='));
                $value = $filter['value'] ?? null;
                if (in_array($operator, ['in', 'not_in'], true) && $value === null && array_key_exists('values', $filter)) {
                    $value = $filter['values'];
                }

                return [
                    'column' => (string) ($filter['column'] ?? ''),
                    'operator' => $operator,
                    'value' => $value,
                ];
            }, $filters);
        }

        $normalized = [];
        foreach ($filters as $column => $value) {
            $normalized[] = [
                'column' => (string) $column,
                'operator' => '=',
                'value' => $value,
            ];
        }

        return $normalized;
    }

    /**
     * @param  array{column:string, operator:string, value:mixed}  $filter
     */
    private function matchesFilter(array $row, array $filter): bool
    {
        $column = $filter['column'];
        $operator = $filter['operator'];
        $value = $filter['value'];

        $actual = array_key_exists($column, $row) ? $row[$column] : null;

        return match ($operator) {
            '=' => $actual == $value,
            '!=' => $actual != $value,
            '>' => $actual > $value,
            '>=' => $actual >= $value,
            '<' => $actual < $value,
            '<=' => $actual <= $value,
            'in' => is_array($value) && in_array($actual, $value, true),
            'not_in' => is_array($value) && ! in_array($actual, $value, true),
            'like' => is_string($actual) && $this->matchLike($actual, (string) $value),
            'not_like' => is_string($actual) && ! $this->matchLike($actual, (string) $value),
            'null' => $actual === null,
            'not_null' => $actual !== null,
            'between' => is_array($value) && count($value) === 2 && $actual >= $value[0] && $actual <= $value[1],
            'not_between' => is_array($value) && count($value) === 2 && ($actual < $value[0] || $actual > $value[1]),
            'exists_in' => throw new RuntimeException(
                'Target filter operator "exists_in" is not supported. Use exists_in in source filters (config filters on source table) only.'
            ),
            default => throw new RuntimeException(sprintf('Unsupported target filter operator "%s".', $operator)),
        };
    }

    private function matchLike(string $actual, string $pattern): bool
    {
        $regex = '/^'.str_replace(['%', '_'], ['.*', '.'], preg_quote($pattern, '/')).'$/ui';

        return preg_match($regex, $actual) === 1;
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyTransforms(mixed $value, mixed $transformDefinition, array $sourceRow = []): mixed
    {
        if ($transformDefinition === null) {
            return $this->normalizeValue($value);
        }

        $transforms = is_array($transformDefinition) ? $transformDefinition : [$transformDefinition];
        $current = $value;

        foreach ($transforms as $transform) {
            $current = $this->applyTransformRule($current, $transform, $sourceRow);
        }

        return $this->normalizeValue($current);
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyTransformRule(mixed $current, mixed $transform, array $sourceRow = []): mixed
    {
        if ($transform instanceof \Closure) {
            return $transform($current, $sourceRow);
        }

        if (is_string($transform)) {
            if ($transform === 'trim') {
                return is_string($current) ? trim($current) : $current;
            }

            if ($transform === 'null_if_empty') {
                return is_string($current) && trim($current) === '' ? null : $current;
            }

            if ($transform === 'zero_date_to_null') {
                return $this->applyZeroDateToNullTransform($current);
            }

            throw new RuntimeException(sprintf('Unsupported transform rule "%s".', $transform));
        }

        if (is_array($transform)) {
            $ruleName = (string) ($transform['rule'] ?? '');
            if ($ruleName === 'if_eq') {
                if (! array_key_exists('value', $transform)) {
                    throw new RuntimeException('Invalid transform rule "if_eq": missing "value".');
                }
                if (! array_key_exists('then', $transform)) {
                    throw new RuntimeException('Invalid transform rule "if_eq": missing "then".');
                }

                return $current == $transform['value'] ? $transform['then'] : $current;
            }

            if ($ruleName === 'multiply') {
                if ($current === null) {
                    return null;
                }
                if (! array_key_exists('by', $transform)) {
                    throw new RuntimeException('Invalid transform rule "multiply": missing "by".');
                }

                return (float) $current * (float) $transform['by'];
            }

            if ($ruleName === 'round_precision') {
                if ($current === null) {
                    return null;
                }
                $precision = (int) ($transform['precision'] ?? 0);
                if (isset($transform['when']) && is_array($transform['when']) && $sourceRow !== []) {
                    $whenCol = (string) ($transform['when']['column'] ?? '');
                    $whenIn = $transform['when']['in'] ?? null;
                    if ($whenCol !== '' && is_array($whenIn)) {
                        $cell = $sourceRow[$whenCol] ?? null;
                        if (in_array($cell, $whenIn, true)) {
                            $precision = (int) ($transform['then_precision'] ?? $precision);
                        }
                    }
                }

                return round((float) $current, $precision);
            }

            if ($ruleName === 'invoke') {
                $using = $transform['using'] ?? null;
                if (! is_array($using) || count($using) !== 2) {
                    throw new RuntimeException('Invalid transform rule "invoke": "using" must be a [class, method] array.');
                }
                $callable = $using;
                if (! is_callable($callable)) {
                    throw new RuntimeException('Invalid transform rule "invoke": "using" is not callable.');
                }

                return $callable($current, $sourceRow);
            }

            throw new RuntimeException(sprintf('Unsupported transform rule "%s".', $ruleName));
        }

        throw new RuntimeException(sprintf(
            'Unsupported transform definition type "%s".',
            get_debug_type($transform)
        ));
    }

    /**
     * MySQL-style zero dates (0000-00-00); leave any other value unchanged (do not treat as unsupported).
     */
    private function applyZeroDateToNullTransform(mixed $current): mixed
    {
        if (is_string($current) && preg_match('/^0{4}-0{2}-0{2}/', $current) === 1) {
            return null;
        }

        if ($current instanceof \DateTimeInterface) {
            $year = (int) $current->format('Y');
            if ($year <= 0) {
                return null;
            }
        }

        return $current;
    }

    private function normalizeValue(mixed $value): mixed
    {
        if (is_string($value) && preg_match('/^0{4}-0{2}-0{2}/', $value) === 1) {
            return null;
        }

        return $value;
    }

    /**
     * @param  array<string, mixed>  $metadata
     */
    private function normalizeByTargetType(mixed $value, string $targetColumn, array $metadata): mixed
    {
        if (! (bool) config('dbtodb_mapping.auto_transforms.enabled', true)) {
            return $value;
        }

        $columnMeta = (array) ($metadata['columns'][$targetColumn] ?? []);
        $type = strtolower((string) ($columnMeta['type'] ?? ''));

        $forcedBoolColumns = (array) config('dbtodb_mapping.auto_transforms.bool_columns.'.((string) ($metadata['table'] ?? '')), []);
        $isForcedBoolColumn = in_array($targetColumn, $forcedBoolColumns, true);

        if ((bool) config('dbtodb_mapping.auto_transforms.bool', true) && ($this->isBooleanType($type) || $isForcedBoolColumn)) {
            return $this->castToBoolLike($value);
        }

        return $value;
    }

    private function isBooleanType(string $type): bool
    {
        if ($type === '') {
            return false;
        }

        return str_contains($type, 'bool');
    }

    private function castToBoolLike(mixed $value): mixed
    {
        if ($value === null || is_bool($value)) {
            return $value;
        }

        if (is_int($value) || is_float($value)) {
            return $value != 0;
        }

        if (is_string($value)) {
            $normalized = strtolower(trim($value));
            if (in_array($normalized, ['1', 'true', 't', 'yes', 'y', 'on'], true)) {
                return true;
            }
            if (in_array($normalized, ['0', 'false', 'f', 'no', 'n', 'off'], true)) {
                return false;
            }
            if (is_numeric($normalized)) {
                return (float) $normalized != 0.0;
            }
        }

        return $value;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function cachedTableColumnsByName(string $connection, string $table): array
    {
        $cacheKey = $connection."\0".$table;
        if (! array_key_exists($cacheKey, $this->targetTableColumnsCache)) {
            $this->targetTableColumnsCache[$cacheKey] = $this->tableColumnsByName($connection, $table);
        }

        return $this->targetTableColumnsCache[$cacheKey];
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function tableColumnsByName(string $connection, string $table): array
    {
        $driver = DB::connection($connection)->getDriverName();

        if ($driver === 'sqlite') {
            $rows = DB::connection($connection)->select(sprintf('PRAGMA table_info("%s")', str_replace('"', '""', $table)));
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

        if ($driver === 'pgsql') {
            $rows = DB::connection($connection)->select(
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
                [$table],
            );

            $columns = [];
            foreach ($rows as $row) {
                $columns[(string) $row->column_name] = [
                    'nullable' => (bool) $row->nullable,
                    'has_default' => (bool) $row->has_default,
                    'is_pk' => (bool) $row->is_pk,
                    'type' => strtolower((string) $row->data_type),
                ];
            }

            return $columns;
        }

        return [];
    }

    /**
     * @param  array<string, array<string, mixed>>  $columns
     * @return list<string>
     */
    private function requiredColumns(array $columns): array
    {
        $required = [];
        foreach ($columns as $name => $meta) {
            if (($meta['nullable'] ?? true) === false && ($meta['has_default'] ?? false) === false && ($meta['is_pk'] ?? false) === false) {
                $required[] = $name;
            }
        }

        return $required;
    }

    /**
     * @param  array<string, mixed>  $target
     */
    private function targetKey(array $target): string
    {
        return sprintf('%s.%s', (string) $target['connection'], (string) $target['table']);
    }
}
