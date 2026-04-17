<?php

namespace MB\DbToDb\Console;

use MB\DbToDb\Support\Database\AutoIncrementSynchronizer;
use MB\DbToDb\Support\Database\DbToDbRoutingExecutor;
use MB\DbToDb\Support\ProfileLoggingChannel;
use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use RuntimeException;
use Symfony\Component\Console\Helper\ProgressBar;
use Throwable;

class DbToDbCommand extends Command
{
    protected $signature = 'db:to-db
        {--tables= : Comma-separated source tables from config/dbtodb_mapping.php}
        {--dry-run : Validate and read data without writing}
        {--continue-on-error : Continue processing on per-pipeline failure}
        {--report-file= : Write JSON report file}
        {--source=source : Source database connection}
        {--target=target : Target database connection}
        {--step= : When runtime.steps_in_tables is true, run only this step key (omit to run all steps in order)}
        {--profile : Log per-pipeline and per-chunk timings to the log channel named in dbtodb_mapping.profile_logging}';

    protected $description = 'Universal database-to-database data routing command (1 source -> N targets).';

    public function __construct(
        private DbToDbRoutingExecutor $executor,
        private AutoIncrementSynchronizer $autoIncrementSynchronizer,
    ) {
        parent::__construct();
    }

    public function handle(): int
    {
        try {
            $this->applyDbToDbCliMemoryLimit();

            $dryRun = (bool) $this->option('dry-run');
            $continueOnError = (bool) $this->option('continue-on-error');
            $source = (string) $this->option('source');
            $target = (string) $this->option('target');
            $reportFileOption = (string) ($this->option('report-file') ?? '');

            $pipelines = $this->resolvePipelines($source, $target);
            if ($pipelines === []) {
                $this->warn('No pipelines selected for execution.');

                return self::SUCCESS;
            }

            $profile = (bool) $this->option('profile');
            if ($profile) {
                $sourceTables = array_values(array_unique(array_map(
                    static fn (array $p): string => (string) Arr::get($p, 'source.table', ''),
                    $pipelines
                )));
                $sourceTables = array_values(array_filter($sourceTables, static fn (string $t): bool => $t !== ''));

                Log::channel(ProfileLoggingChannel::resolve())->info('db_to_db.command.start', [
                    'source' => $source,
                    'target' => $target,
                    'pipelines_count' => count($pipelines),
                    'dry_run' => $dryRun,
                    'continue_on_error' => $continueOnError,
                    'source_table_count' => count($sourceTables),
                    'source_tables' => $sourceTables,
                ]);
                $this->line(sprintf(
                    'Profiling: timings go to the "%s" log channel (see config/logging.php).',
                    ProfileLoggingChannel::resolve()
                ));
            }

            $stepLine = '';
            if (filter_var(config('dbtodb_mapping.runtime.steps_in_tables', false), FILTER_VALIDATE_BOOLEAN)) {
                $stepOpt = trim((string) ($this->option('step') ?? ''));
                $stepLine = $stepOpt !== ''
                    ? sprintf(' step=%s', $stepOpt)
                    : ' steps=all';
            }

            $this->info(sprintf(
                'Starting db-to-db routing: pipelines=%d dry-run=%s continue-on-error=%s%s',
                count($pipelines),
                $dryRun ? 'yes' : 'no',
                $continueOnError ? 'yes' : 'no',
                $stepLine
            ));

            $progressBar = $this->createTableProgressBar(count($pipelines));
            $reportPath = $this->resolveReportFilePath($reportFileOption);
            $reportStream = $this->openReportStream($reportPath, $dryRun, $continueOnError);
            $tableRows = [];
            $failedCount = 0;

            try {
                $this->executor->runEach(
                    $pipelines,
                    $dryRun,
                    $continueOnError,
                    function (array $report) use (&$tableRows, &$failedCount, &$reportStream): void {
                        $this->appendReportStream($reportStream, $report);
                        $row = [
                            (string) Arr::get($report, 'pipeline'),
                            (string) Arr::get($report, 'status'),
                            (string) Arr::get($report, 'source.table'),
                            $this->formatTargetTablesFromReport($report),
                            (string) Arr::get($report, 'read'),
                            (string) Arr::get($report, 'written'),
                            (string) (Arr::get($report, 'reason') ?? ''),
                        ];
                        if ($this->output->isVerbose()) {
                            $ms = Arr::get($report, 'duration_ms');
                            $row[] = $ms !== null && $ms !== '' ? (string) $ms : '';
                        }
                        $tableRows[] = $row;

                        if ((string) Arr::get($report, 'status') === 'failed') {
                            $failedCount++;
                        }
                    },
                    $progressBar === null ? null : function (array $pipeline, int $index, int $total) use ($progressBar): void {
                        $table = (string) Arr::get($pipeline, 'source.table', '');
                        $label = $table !== '' ? $table : (string) ($pipeline['name'] ?? '');
                        $progressBar->setMessage($label !== '' ? $label : sprintf('pipeline %d/%d', $index + 1, $total));
                    },
                    $progressBar === null ? null : static function () use ($progressBar): void {
                        $progressBar->advance();
                    },
                    $profile,
                    $progressBar === null ? null : function (array $pipeline, string $phase, int $current, int $totalRows) use ($progressBar): void {
                        $table = (string) Arr::get($pipeline, 'source.table', '');
                        $label = $table !== '' ? $table : (string) ($pipeline['name'] ?? '');
                        $base = $label !== '' ? $label : 'pipeline';
                        $suffix = match ($phase) {
                            'read' => sprintf(' (read: %d/%d rows)', $current, $totalRows),
                            'write' => sprintf(' (write: %d/%d)', $current, $totalRows),
                            default => '',
                        };
                        $progressBar->setMessage($base.$suffix);
                        $progressBar->display();
                    },
                );
            } finally {
                $this->closeReportStream($reportStream);
                if ($progressBar instanceof ProgressBar) {
                    $progressBar->finish();
                    $this->newLine(2);
                }
            }

            $syncTablesRaw = config('dbtodb_mapping.sync_serial_sequence_tables', []);
            if (! is_array($syncTablesRaw)) {
                $syncTablesRaw = [];
            }
            $syncSerial = filter_var(config('dbtodb_mapping.sync_serial_sequences', false), FILTER_VALIDATE_BOOLEAN);

            $syncDefinitions = [];
            if ($syncSerial && ! $dryRun && $failedCount === 0) {
                if ($syncTablesRaw === []) {
                    $syncDefinitions = $this->resolveAutoIncrementSyncFromPipelines($pipelines, $target);
                } else {
                    $syncDefinitions = $this->normalizeSyncSerialSequenceTablesConfig($syncTablesRaw);
                }
            }

            if ($syncSerial && ! $dryRun && $syncDefinitions !== [] && $failedCount === 0) {
                $driver = DB::connection($target)->getDriverName();
                $supportedDrivers = ['pgsql', 'mysql', 'mariadb', 'sqlite', 'sqlsrv'];
                if (in_array($driver, $supportedDrivers, true)) {
                    $onSkip = $this->output->isVerbose()
                        ? function (string $message): void {
                            $this->comment($message);
                        }
                        : null;
                    $this->autoIncrementSynchronizer->sync($target, $syncDefinitions, $onSkip);
                    $this->info(sprintf(
                        'Auto-increment / sequences synchronized for %d table(s) on connection "%s".',
                        count($syncDefinitions),
                        $target,
                    ));
                } elseif ($this->output->isVerbose()) {
                    $this->comment(sprintf(
                        'sync_serial_sequences is enabled but target driver "%s" is not supported; skipped.',
                        $driver,
                    ));
                }
            } elseif ($syncSerial && ! $dryRun && $syncTablesRaw === [] && $failedCount === 0 && $syncDefinitions === [] && $this->output->isVerbose()) {
                $this->comment('sync_serial_sequences is enabled but no target tables were resolved for this run (check pipelines / --target).');
            }

            $headers = ['pipeline', 'status', 'source_table', 'target_table', 'read', 'written', 'reason'];
            if ($this->output->isVerbose()) {
                $headers[] = 'ms';
            }

            $this->table($headers, $tableRows);
            $this->line('Report file: '.$reportPath);

            if ($failedCount > 0) {
                $this->warn(sprintf('Completed with %d failed pipeline(s).', $failedCount));
            }

            return self::SUCCESS;
        } catch (RuntimeException $exception) {
            $this->error('RuntimeException: '.$this->sanitizeExceptionMessage($exception->getMessage(), 450));

            return self::FAILURE;
        } catch (Throwable $exception) {
            $this->error('Migration failed: '.$this->sanitizeExceptionMessage($exception->getMessage()));

            return self::FAILURE;
        }
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function resolvePipelines(string $source, string $target): array
    {
        return $this->resolvePipelinesFromConfig($source, $target);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function resolvePipelinesFromConfig(string $source, string $target): array
    {
        $mappingConfig = (array) config('dbtodb_mapping');
        $tablesRoot = $this->resolveTablesRootForPipelines($mappingConfig);
        $mappingConfig['tables'] = $tablesRoot;

        $allowedSourceTables = array_keys($tablesRoot);
        $tables = $this->resolveTables($allowedSourceTables, (string) ($this->option('tables') ?? ''));

        $strict = (bool) config('dbtodb_mapping.strict', true);
        $defaultChunk = (int) Arr::get($mappingConfig, 'runtime.defaults.chunk', 1000);
        if ($defaultChunk < 1) {
            throw new RuntimeException('Invalid dbtodb_mapping.runtime.defaults.chunk, expected integer >= 1.');
        }

        $defaultTransactionMode = (string) Arr::get($mappingConfig, 'runtime.defaults.transaction_mode', 'batch');
        if (! in_array($defaultTransactionMode, ['atomic', 'batch'], true)) {
            throw new RuntimeException('Invalid dbtodb_mapping.runtime.defaults.transaction_mode, expected "atomic" or "batch".');
        }

        $pipelines = [];
        foreach ($tables as $table) {
            $targetTables = $this->normalizeTargetTables($mappingConfig, $table);

            $targets = [];
            $filterResolution = $this->resolveFiltersForTable($mappingConfig, $table, $targetTables);
            foreach ($targetTables as $targetTable) {
                $map = Arr::get($mappingConfig, sprintf('columns.%s.%s', $table, $targetTable), []);
                if (! is_array($map)) {
                    throw new RuntimeException(sprintf(
                        'Invalid columns mapping for "%s" -> "%s". Expected columns.%s.%s array.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable
                    ));
                }
                if (count($targetTables) > 1 && $map === []) {
                    throw new RuntimeException(sprintf(
                        'Missing columns mapping for "%s" -> "%s". Expected columns.%s.%s array for multi-target routing.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable
                    ));
                }

                $transforms = $this->resolveTransformsForTarget($mappingConfig, $table, $targetTable, $targetTables);
                if (! is_array($transforms)) {
                    throw new RuntimeException(sprintf(
                        'Invalid transforms mapping for "%s" -> "%s". Expected transforms.%s.%s array.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable
                    ));
                }

                $targetDef = [
                    'connection' => $target,
                    'table' => $targetTable,
                    'operation' => 'upsert',
                    'map' => $map,
                    'transforms' => $transforms,
                    'filters' => $filterResolution['targets'][$targetTable] ?? [],
                ];

                $upsertKeys = $this->resolveUpsertKeysForTarget($mappingConfig, $table, $targetTable);
                if ($upsertKeys !== []) {
                    $targetDef['keys'] = $upsertKeys;
                }

                $targets[] = $targetDef;
            }

            $tableChunk = (int) Arr::get($mappingConfig, sprintf('runtime.tables.%s.chunk', $table), $defaultChunk);
            if ($tableChunk < 1) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.chunk, expected integer >= 1.',
                    $table
                ));
            }

            $tableTransactionMode = (string) Arr::get(
                $mappingConfig,
                sprintf('runtime.tables.%s.transaction_mode', $table),
                $defaultTransactionMode
            );
            if (! in_array($tableTransactionMode, ['atomic', 'batch'], true)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.transaction_mode, expected "atomic" or "batch".',
                    $table
                ));
            }

            $keysetColumn = trim((string) Arr::get($mappingConfig, sprintf('runtime.tables.%s.keyset_column', $table), ''));
            if ($keysetColumn !== '' && ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $keysetColumn)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.keyset_column, expected identifier pattern.',
                    $table
                ));
            }

            $sourceSelect = $this->resolveSourceSelectFromMapping(
                $targets,
                $filterResolution['source'] ?? [],
                $keysetColumn,
            );

            $sourceDef = [
                'connection' => $source,
                'table' => $table,
                'chunk' => $tableChunk,
                'filters' => $filterResolution['source'],
            ];
            if ($keysetColumn !== '') {
                $sourceDef['keyset_column'] = $keysetColumn;
            }
            if ($sourceSelect !== null) {
                $sourceDef['select'] = $sourceSelect;
            }

            $pipelines[] = [
                'name' => 'source:'.$table,
                'strict' => $strict,
                'transaction_mode' => $tableTransactionMode,
                'source' => $sourceDef,
                'targets' => $targets,
            ];
        }

        return $pipelines;
    }

    /**
     * Flatten {@see $mappingConfig} `tables` for the current CLI `--step` and `runtime.steps_in_tables`.
     *
     * @param  array<string, mixed>  $mappingConfig
     * @return array<string, mixed>
     */
    private function resolveTablesRootForPipelines(array $mappingConfig): array
    {
        $step = trim((string) ($this->option('step') ?? ''));
        $stepsInTables = filter_var(
            Arr::get($mappingConfig, 'runtime.steps_in_tables', false),
            FILTER_VALIDATE_BOOLEAN
        );

        $raw = $mappingConfig['tables'] ?? [];
        if (! is_array($raw)) {
            throw new RuntimeException('Invalid dbtodb_mapping.tables: expected array.');
        }

        if (! $stepsInTables) {
            if ($step !== '') {
                throw new RuntimeException(
                    'The --step option is only valid when dbtodb_mapping.runtime.steps_in_tables is true.'
                );
            }

            return $raw;
        }

        if ($raw === []) {
            throw new RuntimeException(
                'Invalid dbtodb_mapping.tables: expected non-empty array when runtime.steps_in_tables is true.'
            );
        }

        if (array_is_list($raw)) {
            throw new RuntimeException(
                'When dbtodb_mapping.runtime.steps_in_tables is true, tables must be a map of step keys (e.g. first, second), not a list.'
            );
        }

        if ($step !== '') {
            if (! array_key_exists($step, $raw)) {
                throw new RuntimeException(sprintf(
                    'Unknown step "%s". Available steps: %s.',
                    $step,
                    implode(', ', array_keys($raw))
                ));
            }

            $inner = $raw[$step];
            if (! is_array($inner) || $inner === []) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": expected non-empty array of source => target mappings.',
                    $step
                ));
            }
            if (array_is_list($inner)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": inner map must use source table names as keys, not a list.',
                    $step
                ));
            }

            return $this->validateFlatTablesLeaf($inner, $step);
        }

        $merged = [];
        foreach ($raw as $stepName => $inner) {
            if (! is_string($stepName) || trim($stepName) === '') {
                throw new RuntimeException(
                    'When dbtodb_mapping.runtime.steps_in_tables is true, every step key must be a non-empty string.'
                );
            }
            if (! is_array($inner) || $inner === []) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": expected non-empty array of source => target mappings.',
                    $stepName
                ));
            }
            if (array_is_list($inner)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": inner map must use source table names as keys, not a list.',
                    $stepName
                ));
            }
            $inner = $this->validateFlatTablesLeaf($inner, $stepName);
            foreach ($inner as $sourceTable => $targets) {
                if (array_key_exists($sourceTable, $merged)) {
                    throw new RuntimeException(sprintf(
                        'Duplicate source table "%s" under step "%s" and an earlier step. Use --step to run one step, or remove the duplicate.',
                        $sourceTable,
                        $stepName
                    ));
                }
                $merged[$sourceTable] = $targets;
            }
        }

        return $merged;
    }

    /**
     * @param  array<string, mixed>  $leaf
     * @return array<string, mixed>
     */
    private function validateFlatTablesLeaf(array $leaf, string $stepLabel): array
    {
        foreach ($leaf as $sourceTable => $targets) {
            if (! is_string($sourceTable) || trim($sourceTable) === '') {
                throw new RuntimeException(sprintf(
                    'Invalid source table key under step "%s": expected non-empty string.',
                    $stepLabel
                ));
            }
        }

        return $leaf;
    }

    /**
     * Narrow source SELECT from non-empty per-target column maps in config (source keys only).
     * Returns null for SELECT * when every target map is empty (single target: copy all columns).
     *
     * @param  list<array<string, mixed>>  $targets
     * @param  array<int|string, mixed>  $sourceFilters
     * @return list<string>|null
     */
    private function resolveSourceSelectFromMapping(
        array $targets,
        array $sourceFilters,
        string $keysetColumn,
    ): ?array {
        $merged = [];
        $hasNonEmptyMap = false;
        foreach ($targets as $target) {
            if (! is_array($target)) {
                continue;
            }
            $map = $target['map'] ?? [];
            if ($map !== []) {
                $hasNonEmptyMap = true;
            }
            foreach (array_keys($map) as $k) {
                if (! is_string($k)) {
                    continue;
                }
                $sk = trim($k);
                if ($sk !== '' && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $sk)) {
                    $merged[$sk] = true;
                }
            }
        }

        if (! $hasNonEmptyMap) {
            return null;
        }

        foreach ($this->collectFilterColumnNamesFromRules($sourceFilters) as $c) {
            $merged[$c] = true;
        }
        foreach ($targets as $target) {
            if (! is_array($target)) {
                continue;
            }
            foreach ($this->collectFilterColumnNamesFromRules($target['filters'] ?? []) as $c) {
                $merged[$c] = true;
            }
        }

        if ($keysetColumn !== '') {
            $merged[$keysetColumn] = true;
        }

        $keys = array_keys($merged);
        sort($keys, SORT_STRING);

        return $keys;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<string>
     */
    private function collectFilterColumnNamesFromRules(array $filters): array
    {
        $names = [];
        foreach ($this->normalizeFilterRulesForColumnHarvest($filters) as $filter) {
            if (! is_array($filter)) {
                continue;
            }
            $col = trim((string) ($filter['column'] ?? ''));
            if ($col === '' || ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $col)) {
                continue;
            }
            $names[$col] = true;
        }

        return array_keys($names);
    }

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<array<string, mixed>>
     */
    private function normalizeFilterRulesForColumnHarvest(array $filters): array
    {
        if ($filters === []) {
            return [];
        }

        $first = reset($filters);
        if (is_array($first) && array_key_exists('column', $first)) {
            /** @var list<array<string, mixed>> $filters */
            return $filters;
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
     * Optional upsert conflict columns for targets that lack a discoverable PK or need an override.
     *
     * Formats:
     * - upsert_keys[source] = ['col1', 'col2'] — same keys for every target of this source
     * - upsert_keys[source][target] = ['col1'] — per target (multi-target pipelines)
     *
     * @param  array<string, mixed>  $mappingConfig
     * @return list<string>
     */
    private function resolveUpsertKeysForTarget(array $mappingConfig, string $sourceTable, string $targetTable): array
    {
        $node = Arr::get($mappingConfig, sprintf('upsert_keys.%s', $sourceTable));
        if (! is_array($node) || $node === []) {
            return [];
        }

        if (array_is_list($node)) {
            return array_values(array_filter(
                array_map(static fn (mixed $v): string => is_string($v) ? trim($v) : '', $node),
                static fn (string $s): bool => $s !== '',
            ));
        }

        $specific = $node[$targetTable] ?? null;
        if (! is_array($specific)) {
            return [];
        }

        return array_values(array_filter(
            array_map(static fn (mixed $v): string => is_string($v) ? trim($v) : '', $specific),
            static fn (string $s): bool => $s !== '',
        ));
    }

    private function normalizeTargetTables(array $mappingConfig, string $table): array
    {
        $raw = Arr::get($mappingConfig, 'tables.'.$table);

        if (is_string($raw) && trim($raw) !== '') {
            return [$raw];
        }

        if (! is_array($raw) || $raw === []) {
            throw new RuntimeException(sprintf(
                'Invalid dbtodb_mapping.tables for "%s": expected non-empty string or array of target tables.',
                $table
            ));
        }

        $normalized = [];
        foreach ($raw as $targetTable) {
            if (! is_string($targetTable) || trim($targetTable) === '') {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables for "%s": every target table must be non-empty string.',
                    $table
                ));
            }
            $normalized[] = $targetTable;
        }

        return $normalized;
    }

    /**
     * @param  list<string>  $allowedTables
     * @return list<string>
     */
    private function resolveTables(array $allowedTables, string $tablesOption): array
    {
        if (trim($tablesOption) === '') {
            return $allowedTables;
        }

        $requestedTables = $this->parseListOption($tablesOption);
        $unknown = array_values(array_diff($requestedTables, $allowedTables));
        if ($unknown !== []) {
            throw new RuntimeException('Unknown or disallowed tables: '.implode(', ', $unknown));
        }

        return $requestedTables;
    }

    /**
     * @return list<string>
     */
    private function parseListOption(string $value): array
    {
        return array_values(array_filter(array_map(
            static fn (string $entry): string => trim($entry),
            explode(',', $value),
        )));
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     * @param  list<string>  $targetTables
     * @return array{
     *   source: array<int|string, mixed>,
     *   targets: array<string, array<int|string, mixed>>
     * }
     */
    private function resolveFiltersForTable(array $mappingConfig, string $table, array $targetTables): array
    {
        $raw = Arr::get($mappingConfig, 'filters.'.$table, []);
        if (! is_array($raw)) {
            throw new RuntimeException(sprintf('Invalid filters for "%s": expected array.', $table));
        }

        $targetTableSet = array_fill_keys($targetTables, true);
        $usesPerTargetShape = array_key_exists('default', $raw);
        if (! $usesPerTargetShape) {
            foreach (array_keys($raw) as $key) {
                if (is_string($key) && array_key_exists($key, $targetTableSet)) {
                    $usesPerTargetShape = true;
                    break;
                }
            }
        }

        if (! $usesPerTargetShape) {
            return [
                'source' => $raw,
                'targets' => [],
            ];
        }

        $defaultRules = $raw['default'] ?? [];
        if (! is_array($defaultRules)) {
            throw new RuntimeException(sprintf(
                'Invalid filters.%s.default: expected rules array.',
                $table
            ));
        }

        $targetFilters = [];
        foreach ($targetTables as $targetTable) {
            $rules = $raw[$targetTable] ?? $defaultRules;
            if (! is_array($rules)) {
                throw new RuntimeException(sprintf(
                    'Invalid filters.%s.%s: expected rules array.',
                    $table,
                    $targetTable
                ));
            }
            $targetFilters[$targetTable] = $rules;
        }

        return [
            'source' => [],
            'targets' => $targetFilters,
        ];
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     * @return array<string, mixed>
     */
    private function resolveTransformsForTarget(array $mappingConfig, string $sourceTable, string $targetTable, array $knownTargetTables): array
    {
        $tableTransforms = Arr::get($mappingConfig, sprintf('transforms.%s', $sourceTable), []);
        if (! is_array($tableTransforms)) {
            throw new RuntimeException(sprintf(
                'Invalid transforms for "%s": expected array.',
                $sourceTable
            ));
        }

        $targetTransforms = Arr::get($tableTransforms, $targetTable, []);
        if ($targetTransforms !== [] && ! is_array($targetTransforms)) {
            throw new RuntimeException(sprintf(
                'Invalid transforms mapping for "%s" -> "%s". Expected transforms.%s.%s array.',
                $sourceTable,
                $targetTable,
                $sourceTable,
                $targetTable
            ));
        }

        $knownTargets = array_fill_keys($knownTargetTables, true);
        $tableLevelTransforms = [];
        foreach ($tableTransforms as $key => $value) {
            if (is_string($key) && array_key_exists($key, $knownTargets)) {
                continue;
            }
            $tableLevelTransforms[$key] = $value;
        }

        return array_merge($tableLevelTransforms, is_array($targetTransforms) ? $targetTransforms : []);
    }

    /**
     * @param  array<string, mixed>  $report
     */
    /**
     * @param  array<string, mixed>  $report
     */
    private function formatTargetTablesFromReport(array $report): string
    {
        $targets = (array) Arr::get($report, 'targets', []);
        $names = [];
        foreach ($targets as $target) {
            if (! is_array($target)) {
                continue;
            }
            $table = trim((string) ($target['table'] ?? ''));
            if ($table !== '') {
                $names[] = $table;
            }
        }

        $names = array_values(array_unique($names));

        return $names === [] ? '' : implode(', ', $names);
    }

    private function writeReportFile(string $path, array $report): void
    {
        File::ensureDirectoryExists(dirname($path));
        File::put($path, json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE).PHP_EOL);
    }

    /**
     * @return array{path:string, handle:resource, first:bool}
     */
    private function openReportStream(string $path, bool $dryRun, bool $continueOnError): array
    {
        File::ensureDirectoryExists(dirname($path));
        $handle = fopen($path, 'wb');
        if ($handle === false) {
            throw new RuntimeException(sprintf('Cannot open report file "%s" for writing.', $path));
        }

        fwrite($handle, '{"dry_run":'.($dryRun ? 'true' : 'false').',"continue_on_error":'.($continueOnError ? 'true' : 'false').',"pipelines":[');

        return [
            'path' => $path,
            'handle' => $handle,
            'first' => true,
        ];
    }

    /**
     * @param  array{path:string, handle:resource, first:bool}  $stream
     * @param  array<string, mixed>  $report
     */
    private function appendReportStream(array &$stream, array $report): void
    {
        $json = json_encode($report, JSON_UNESCAPED_UNICODE);
        if (! is_string($json)) {
            throw new RuntimeException('Failed to JSON-encode pipeline report.');
        }

        if (! $stream['first']) {
            fwrite($stream['handle'], ',');
        }
        fwrite($stream['handle'], $json);
        $stream['first'] = false;
    }

    /**
     * @param  array{path:string, handle:resource, first:bool}  $stream
     */
    private function closeReportStream(array $stream): void
    {
        fwrite($stream['handle'], ']}'.PHP_EOL);
        fclose($stream['handle']);
    }

    private function resolveReportFilePath(string $optionValue): string
    {
        $trimmed = trim($optionValue);
        if ($trimmed !== '') {
            return $trimmed;
        }

        return storage_path('logs/db-to-db-'.now()->format('Ymd-His').'.json');
    }

    private function createTableProgressBar(int $pipelineCount): ?ProgressBar
    {
        if ($pipelineCount < 1 || $this->output->isQuiet()) {
            return null;
        }

        $bar = $this->output->createProgressBar($pipelineCount);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% %message%');
        $bar->start();

        return $bar;
    }

    private function applyDbToDbCliMemoryLimit(): void
    {
        $limit = config('dbtodb_mapping.runtime.cli_memory_limit');
        if (! is_string($limit)) {
            return;
        }

        $limit = trim($limit);
        if ($limit === '') {
            return;
        }

        @ini_set('memory_limit', $limit);
    }

    /**
     * @param  list<mixed>  $raw
     * @return list<array{table: string, column: string|null}>
     */
    private function normalizeSyncSerialSequenceTablesConfig(array $raw): array
    {
        $out = [];
        foreach ($raw as $entry) {
            if (is_string($entry)) {
                $table = trim($entry);
                if ($table !== '') {
                    $out[] = ['table' => $table, 'column' => 'id'];
                }

                continue;
            }

            if (! is_array($entry) || ! isset($entry['table'])) {
                continue;
            }

            $table = trim((string) $entry['table']);
            if ($table === '') {
                continue;
            }

            $col = $entry['column'] ?? null;
            $column = is_string($col) && trim($col) !== '' ? trim($col) : null;
            $out[] = ['table' => $table, 'column' => $column];
        }

        return $out;
    }

    /**
     * @param  list<array<string, mixed>>  $pipelines
     * @return list<array{table: string, column: string|null}>
     */
    private function resolveAutoIncrementSyncFromPipelines(array $pipelines, string $targetConnection): array
    {
        $seen = [];
        $out = [];
        foreach ($pipelines as $pipeline) {
            foreach ((array) ($pipeline['targets'] ?? []) as $target) {
                if (! is_array($target)) {
                    continue;
                }
                if ((string) ($target['connection'] ?? '') !== $targetConnection) {
                    continue;
                }
                $table = trim((string) ($target['table'] ?? ''));
                if ($table === '') {
                    continue;
                }
                if (array_key_exists($table, $seen)) {
                    continue;
                }
                $seen[$table] = true;

                $keys = array_values(array_filter(
                    array_map(
                        static fn (mixed $k): string => is_string($k) ? trim($k) : '',
                        array_values((array) ($target['keys'] ?? [])),
                    ),
                    static fn (string $s): bool => $s !== '',
                ));
                $column = count($keys) === 1 ? $keys[0] : null;
                $out[] = ['table' => $table, 'column' => $column];
            }
        }

        return $out;
    }

    private function sanitizeExceptionMessage(string $message, int $maxLength = 1000): string
    {
        $trimmed = preg_replace('/\s+\(Connection:.*$/s', '', $message);
        if (! is_string($trimmed) || $trimmed === '') {
            $trimmed = $message;
        }

        return substr($trimmed, 0, max(1, $maxLength));
    }
}
