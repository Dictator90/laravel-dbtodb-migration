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

    private DbToDbTransformEngine $transformEngine;

    public function __construct(
        private DbToDbMappingValidator $validator,
        private DbToDbSourceReader $sourceReader,
        private DbToDbTargetWriter $targetWriter,
        private ?TargetTableMetadataResolver $metadataResolver = null,
        private ?DbToDbFilterEngine $filterEngine = null,
    ) {
        $this->metadataResolver ??= new TargetTableMetadataResolver;
        $this->filterEngine ??= new DbToDbFilterEngine;
        $this->transformEngine = new DbToDbTransformEngine;
    }

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
        $this->targetWriter->resetRunState();

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
        $targetKeys = array_map(fn (array $target): string => $this->targetKey($target), $targets);
        $targetWritten = array_fill_keys($targetKeys, 0);
        $targetSkipped = array_fill_keys($targetKeys, 0);
        $targetDeduplicated = array_fill_keys($targetKeys, 0);
        $targetErrors = array_fill_keys($targetKeys, []);

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
                            $written = $this->writeAtomicBatch($batch, $targets, $targetMetadata, $strict, $targetWritten, $targetSkipped, $targetDeduplicated, $targetErrors);
                        } else {
                            $written = $this->writeBatchMode($batch, $targets, $targetMetadata, $strict, $targetWritten, $targetSkipped, $targetDeduplicated, $targetErrors);
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
                            $written = $this->writeAtomicBatch($batch, $targets, $targetMetadata, $strict, $targetWritten, $targetSkipped, $targetDeduplicated, $targetErrors);
                        } else {
                            $written = $this->writeBatchMode($batch, $targets, $targetMetadata, $strict, $targetWritten, $targetSkipped, $targetDeduplicated, $targetErrors);
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
            'targets' => array_map(function (array $target) use ($targetWritten, $targetSkipped, $targetDeduplicated, $targetErrors): array {
                $key = $this->targetKey($target);

                return [
                    'connection' => (string) $target['connection'],
                    'table' => (string) $target['table'],
                    'operation' => (string) ($target['operation'] ?? 'upsert'),
                    'written' => (int) ($targetWritten[$key] ?? 0),
                    'skipped' => (int) ($targetSkipped[$key] ?? 0),
                    'deduplicated' => (int) ($targetDeduplicated[$key] ?? 0),
                    'errors' => array_values((array) ($targetErrors[$key] ?? [])),
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
    private function writeBatchMode(
        array $batch,
        array $targets,
        array $targetMetadata,
        bool $strict,
        array &$targetWritten,
        array &$targetSkipped,
        array &$targetDeduplicated,
        array &$targetErrors,
    ): int
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

            $beforeDedupe = count($rows);
            $rows = $this->deduplicateRowsForTarget($rows, $target);
            $targetDeduplicated[$key] += $beforeDedupe - count($rows);
            if ($rows === []) {
                continue;
            }

            $writeStartedAt = microtime(true);
            $result = $this->targetWriter->writeWithReport($target, $rows);
            if ($this->profileLogging && $this->profileWriteChunkContext !== null) {
                $this->profileLogTimed('chunk.write_target', array_merge($this->profileWriteChunkContext, [
                    'target_connection' => (string) $target['connection'],
                    'target_table' => (string) $target['table'],
                    'rows' => count($rows),
                    'skipped' => $result['skipped'],
                    'deduplicated' => $beforeDedupe - count($rows),
                ]), microtime(true) - $writeStartedAt);
            }
            $targetWritten[$key] += $result['written'];
            $targetSkipped[$key] += $result['skipped'];
            $targetErrors[$key] = $this->mergeErrorLists((array) $targetErrors[$key], $result['errors']);
            $written += $result['written'];
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
    private function writeAtomicBatch(
        array $batch,
        array $targets,
        array $targetMetadata,
        bool $strict,
        array &$targetWritten,
        array &$targetSkipped,
        array &$targetDeduplicated,
        array &$targetErrors,
    ): int
    {
        $connections = array_values(array_unique(array_map(static fn (array $target): string => (string) $target['connection'], $targets)));
        if (count($connections) !== 1) {
            throw new RuntimeException('transaction_mode=atomic supports targets on one connection only.');
        }

        $connection = DB::connection($connections[0]);
        $written = 0;

        $connection->transaction(function () use ($batch, $targets, $targetMetadata, $strict, &$targetWritten, &$targetSkipped, &$targetDeduplicated, &$targetErrors, &$written): void {
            foreach ($targets as $target) {
                $key = $this->targetKey($target);
                $rows = [];
                foreach ($batch as $sourceRow) {
                    $payload = $this->mapRowToTarget($sourceRow, $target, $targetMetadata[$key], $strict);
                    if ($payload !== []) {
                        $rows[] = $payload;
                    }
                }

                $beforeDedupe = count($rows);
                $rows = $this->deduplicateRowsForTarget($rows, $target);
                $targetDeduplicated[$key] += $beforeDedupe - count($rows);

                foreach ($rows as $payload) {
                    $writeStartedAt = microtime(true);
                    $result = $this->targetWriter->writeWithReport($target, [$payload]);
                    if ($this->profileLogging && $this->profileWriteChunkContext !== null) {
                        $this->profileLogTimed('chunk.write_target', array_merge($this->profileWriteChunkContext, [
                            'target_connection' => (string) $target['connection'],
                            'target_table' => (string) $target['table'],
                            'rows' => 1,
                            'skipped' => $result['skipped'],
                        ]), microtime(true) - $writeStartedAt);
                    }
                    $targetWritten[$key] += $result['written'];
                    $targetSkipped[$key] += $result['skipped'];
                    $targetErrors[$key] = $this->mergeErrorLists((array) $targetErrors[$key], $result['errors']);
                    $written += $result['written'];
                }
            }
        });

        return $written;
    }

    /**
     * @param  list<array<string, mixed>>  $rows
     * @param  array<string, mixed>  $target
     * @return list<array<string, mixed>>
     */
    private function deduplicateRowsForTarget(array $rows, array $target): array
    {
        if ($rows === []) {
            return $rows;
        }

        $deduplicate = $target['deduplicate'] ?? false;
        $enabled = is_array($deduplicate) ? (bool) ($deduplicate['enabled'] ?? true) : (bool) $deduplicate;
        if (! $enabled) {
            return $rows;
        }

        $keys = is_array($deduplicate) ? array_values((array) ($deduplicate['keys'] ?? [])) : [];
        if ($keys === []) {
            $keys = array_values((array) ($target['keys'] ?? []));
        }
        $keys = array_values(array_filter(array_map(static fn (mixed $key): string => is_string($key) ? trim($key) : '', $keys)));
        if ($keys === []) {
            throw new RuntimeException(sprintf('Target "%s": deduplicate.keys must be a non-empty list (or upsert_keys must be set).', $this->targetKey($target)));
        }

        $strategy = is_array($deduplicate) ? (string) ($deduplicate['strategy'] ?? 'last') : 'last';
        if (! in_array($strategy, ['first', 'last'], true)) {
            throw new RuntimeException(sprintf('Target "%s": deduplicate.strategy must be first or last.', $this->targetKey($target)));
        }

        $deduped = [];
        foreach ($rows as $row) {
            $keyValues = [];
            foreach ($keys as $key) {
                if (! array_key_exists($key, $row)) {
                    throw new RuntimeException(sprintf('Target "%s": deduplicate row missing key column "%s".', $this->targetKey($target), $key));
                }
                $keyValues[$key] = $row[$key];
            }
            ksort($keyValues);
            $signature = json_encode($keyValues, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
            if ($strategy === 'first' && array_key_exists($signature, $deduped)) {
                continue;
            }
            $deduped[$signature] = $row;
        }

        return array_values($deduped);
    }

    /**
     * @param  list<string>  $current
     * @param  list<string>  $incoming
     * @return list<string>
     */
    private function mergeErrorLists(array $current, array $incoming): array
    {
        foreach ($incoming as $message) {
            if (is_string($message) && $message !== '' && ! in_array($message, $current, true)) {
                $current[] = $message;
            }
        }

        return $current;
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
                    $this->applyTransforms($value, $this->resolveTransformDefinition($transforms, (string) $sourceColumn, $targetColumn), $sourceRow, (string) $sourceColumn, $targetColumn, (string) $target['table']),
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
                    $this->applyTransforms($value, $this->resolveTransformDefinition($transforms, (string) $sourceColumn, $targetColumn), $sourceRow, (string) $sourceColumn, $targetColumn, (string) $target['table']),
                    $targetColumn,
                    $metadata,
                );
            }
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
        return $this->filterEngine->passesRow($row, $filters);
    }

    /**
     * @param  array<string, mixed>  $transforms
     */
    private function resolveTransformDefinition(array $transforms, string $sourceColumn, string $targetColumn): mixed
    {
        if (array_key_exists($targetColumn, $transforms)) {
            return $transforms[$targetColumn];
        }

        if (array_key_exists($sourceColumn, $transforms)) {
            return $transforms[$sourceColumn];
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyTransforms(
        mixed $value,
        mixed $transformDefinition,
        array $sourceRow = [],
        ?string $sourceColumn = null,
        ?string $targetColumn = null,
        ?string $targetTable = null,
    ): mixed {
        return $this->transformEngine->apply(
            $value,
            $transformDefinition,
            $sourceRow,
            $sourceColumn,
            $targetColumn,
            $targetTable,
        );
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
        $nullable = (bool) ($columnMeta['nullable'] ?? true);

        if ($value === '' && $nullable && (bool) config('dbtodb_mapping.auto_transforms.empty_string_to_null', true)) {
            return null;
        }

        $forcedBoolColumns = (array) config('dbtodb_mapping.auto_transforms.bool_columns.'.((string) ($metadata['table'] ?? '')), []);
        $isForcedBoolColumn = in_array($targetColumn, $forcedBoolColumns, true);

        if ((bool) config('dbtodb_mapping.auto_transforms.bool', true) && ($this->isBooleanType($type) || $isForcedBoolColumn)) {
            return $this->castToBoolLike($value);
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.integer', true) && $this->isIntegerType($type)) {
            return $this->castToIntegerLike($value);
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.float', true) && $this->isFloatType($type)) {
            return $this->castToFloatLike($value);
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.json', true) && $this->isJsonType($type)) {
            return $this->castToJsonLike($value);
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.date', true) && $this->isDateType($type)) {
            return $this->castToDateLike($value, 'Y-m-d');
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.datetime', true) && $this->isDateTimeType($type)) {
            return $this->castToDateLike($value, 'Y-m-d H:i:s');
        }

        if ((bool) config('dbtodb_mapping.auto_transforms.string', false) && $this->isStringType($type) && $value !== null && ! is_string($value)) {
            return is_scalar($value) ? (string) $value : $this->castToJsonLike($value);
        }

        return $value;
    }

    private function isBooleanType(string $type): bool
    {
        return $type !== '' && str_contains($type, 'bool');
    }

    private function isIntegerType(string $type): bool
    {
        if ($type === '') {
            return false;
        }

        return str_contains($type, 'int') || str_contains($type, 'serial');
    }

    private function isFloatType(string $type): bool
    {
        foreach (['float', 'double', 'real', 'numeric', 'decimal'] as $needle) {
            if (str_contains($type, $needle)) {
                return true;
            }
        }

        return false;
    }

    private function isJsonType(string $type): bool
    {
        return str_contains($type, 'json');
    }

    private function isDateType(string $type): bool
    {
        return $type === 'date';
    }

    private function isDateTimeType(string $type): bool
    {
        foreach (['datetime', 'timestamp', 'time with time zone', 'time without time zone'] as $needle) {
            if (str_contains($type, $needle)) {
                return true;
            }
        }

        return $type === 'time';
    }

    private function isStringType(string $type): bool
    {
        foreach (['char', 'text', 'uuid', 'string', 'varchar'] as $needle) {
            if (str_contains($type, $needle)) {
                return true;
            }
        }

        return false;
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

    private function castToIntegerLike(mixed $value): mixed
    {
        if ($value === null || is_int($value)) {
            return $value;
        }

        return is_numeric($value) ? (int) $value : $value;
    }

    private function castToFloatLike(mixed $value): mixed
    {
        if ($value === null || is_float($value)) {
            return $value;
        }

        return is_numeric($value) ? (float) $value : $value;
    }

    private function castToJsonLike(mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value)) {
            json_decode($value, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return $value;
            }

            return match ((string) config('dbtodb_mapping.auto_transforms.json_invalid', 'keep')) {
                'null' => null,
                'fail' => throw new RuntimeException('Auto transform failed: invalid JSON string.'),
                default => $value,
            };
        }

        $encoded = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($encoded === false) {
            throw new RuntimeException('Auto transform failed: JSON encoding failed.');
        }

        return $encoded;
    }

    private function castToDateLike(mixed $value, string $format): mixed
    {
        if ($value === null || $value instanceof \DateTimeInterface) {
            return $value instanceof \DateTimeInterface ? $value->format($format) : null;
        }

        if (! is_scalar($value)) {
            return $value;
        }

        try {
            return (new \DateTimeImmutable((string) $value))->format($format);
        } catch (\Throwable) {
            return $value;
        }
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
        return $this->metadataResolver?->resolve($connection, $table) ?? [];
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
