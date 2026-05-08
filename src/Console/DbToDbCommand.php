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
        {--m|migration= : Named migration key from dbtodb_mapping.migrations (default: default)}
        {--tables= : Comma-separated source tables from the selected migration}
        {--dry-run : Validate and read data without writing}
        {--continue-on-error : Continue processing on per-pipeline failure}
        {--report-file= : Write JSON report file}
        {--step= : Run only this step key from the selected migration steps (omit to run all steps in order)}
        {--profile : Log per-pipeline and per-chunk timings to the log channel named in dbtodb_mapping.profile_logging}';

    protected $description = 'Universal database-to-database data routing command (1 source -> N targets).';

    public function __construct(
        private DbToDbRoutingExecutor $executor,
        private AutoIncrementSynchronizer $autoIncrementSynchronizer,
    ) {
        parent::__construct();
    }

    private function stringOptionOrNull(string $name): ?string
    {
        $value = $this->option($name);
        if ($value === null) {
            return null;
        }

        $value = trim((string) $value);

        return $value === '' ? null : $value;
    }

    public function handle(): int
    {
        try {
            $this->applyDbToDbCliMemoryLimit();

            $dryRun = (bool) $this->option('dry-run');
            $continueOnError = (bool) $this->option('continue-on-error');
            $reportFileOption = (string) ($this->option('report-file') ?? '');

            $pipelines = $this->resolvePipelines();
            if ($pipelines === []) {
                $this->warn('No pipelines selected for execution.');

                return self::SUCCESS;
            }

            $resolvedSource = (string) Arr::get($pipelines[0], 'source.connection', '');
            $resolvedTarget = (string) Arr::get($pipelines[0], 'targets.0.connection', '');
            $migrationName = $this->resolveMigrationName();

            $profile = (bool) $this->option('profile');
            if ($profile) {
                $sourceTables = array_values(array_unique(array_map(
                    static fn (array $p): string => (string) Arr::get($p, 'source.table', ''),
                    $pipelines
                )));
                $sourceTables = array_values(array_filter($sourceTables, static fn (string $t): bool => $t !== ''));

                Log::channel(ProfileLoggingChannel::resolve())->info('db_to_db.command.start', [
                    'source' => $resolvedSource,
                    'target' => $resolvedTarget,
                    'migration' => $migrationName,
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

            $stepOpt = trim((string) ($this->option('step') ?? ''));
            $stepLine = $stepOpt !== '' ? sprintf(' step=%s', $stepOpt) : '';

            $this->info(sprintf(
                'Starting db-to-db routing: migration=%s source=%s target=%s pipelines=%d dry-run=%s continue-on-error=%s%s',
                $migrationName,
                $resolvedSource,
                $resolvedTarget,
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
                    $syncDefinitions = $this->resolveAutoIncrementSyncFromPipelines($pipelines, $resolvedTarget);
                } else {
                    $syncDefinitions = $this->normalizeSyncSerialSequenceTablesConfig($syncTablesRaw);
                }
            }

            if ($syncSerial && ! $dryRun && $syncDefinitions !== [] && $failedCount === 0) {
                $driver = DB::connection($resolvedTarget)->getDriverName();
                $supportedDrivers = ['pgsql', 'mysql', 'mariadb', 'sqlite', 'sqlsrv'];
                if (in_array($driver, $supportedDrivers, true)) {
                    $onSkip = $this->output->isVerbose()
                        ? function (string $message): void {
                            $this->comment($message);
                        }
                        : null;
                    $this->autoIncrementSynchronizer->sync($resolvedTarget, $syncDefinitions, $onSkip);
                    $this->info(sprintf(
                        'Auto-increment / sequences synchronized for %d table(s) on connection "%s".',
                        count($syncDefinitions),
                        $resolvedTarget,
                    ));
                } elseif ($this->output->isVerbose()) {
                    $this->comment(sprintf(
                        'sync_serial_sequences is enabled but target driver "%s" is not supported; skipped.',
                        $driver,
                    ));
                }
            } elseif ($syncSerial && ! $dryRun && $syncTablesRaw === [] && $failedCount === 0 && $syncDefinitions === [] && $this->output->isVerbose()) {
                $this->comment('sync_serial_sequences is enabled but no target tables were resolved for this run (check pipelines / selected migration target).');
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
    public function resolvePipelines(): array
    {
        return $this->resolvePipelinesFromConfig();
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function resolvePipelinesFromConfig(): array
    {
        $mappingConfig = (array) config('dbtodb_mapping');
        $migrationName = $this->resolveMigrationName();
        $migrationConfig = $this->resolveMigrationConfig($mappingConfig, $migrationName);
        $tablesRoot = $this->resolveTablesRootForPipelines($migrationConfig, $migrationName);

        $source = trim((string) Arr::get($migrationConfig, 'source', ''));
        $target = trim((string) Arr::get($migrationConfig, 'target', ''));
        if ($source === '') {
            throw new RuntimeException(sprintf(
                'Migration "%s": source connection is required (set dbtodb_mapping.migrations.%s.source).',
                $migrationName,
                $migrationName,
            ));
        }
        if ($target === '') {
            throw new RuntimeException(sprintf(
                'Migration "%s": target connection is required (set dbtodb_mapping.migrations.%s.target).',
                $migrationName,
                $migrationName,
            ));
        }

        $allowedSourceTables = array_keys($tablesRoot);
        $tables = $this->resolveTables($allowedSourceTables, (string) ($this->option('tables') ?? ''));

        $strict = (bool) Arr::get($migrationConfig, 'strict', config('dbtodb_mapping.strict', true));
        $defaultChunk = (int) Arr::get($migrationConfig, 'runtime.defaults.chunk', Arr::get($mappingConfig, 'runtime.defaults.chunk', 1000));
        if ($defaultChunk < 1) {
            throw new RuntimeException('Invalid runtime.defaults.chunk, expected integer >= 1.');
        }

        $defaultTransactionMode = (string) Arr::get(
            $migrationConfig,
            'runtime.defaults.transaction_mode',
            Arr::get($mappingConfig, 'runtime.defaults.transaction_mode', 'batch')
        );
        if (! in_array($defaultTransactionMode, ['atomic', 'batch'], true)) {
            throw new RuntimeException('Invalid runtime.defaults.transaction_mode, expected "atomic" or "batch".');
        }

        $pipelines = [];
        foreach ($tables as $table) {
            $tableDefinition = $this->normalizeMigrationTableDefinition($tablesRoot[$table] ?? null, $table);
            $targetTables = array_keys($tableDefinition['targets']);
            $sourceFilters = (array) ($tableDefinition['source']['filters'] ?? []);

            $targets = [];
            foreach ($targetTables as $targetTable) {
                $targetConfig = $tableDefinition['targets'][$targetTable];
                $map = $targetConfig['columns'] ?? [];
                if (! is_array($map)) {
                    throw new RuntimeException(sprintf(
                        'Invalid columns mapping for "%s" -> "%s". Expected columns array.',
                        $table,
                        $targetTable,
                    ));
                }
                if (count($targetTables) > 1 && $map === []) {
                    throw new RuntimeException(sprintf(
                        'Missing columns mapping for "%s" -> "%s". Expected non-empty columns array for multi-target routing.',
                        $table,
                        $targetTable,
                    ));
                }

                $transforms = $targetConfig['transforms'] ?? [];
                if (! is_array($transforms)) {
                    throw new RuntimeException(sprintf(
                        'Invalid transforms mapping for "%s" -> "%s". Expected transforms array.',
                        $table,
                        $targetTable,
                    ));
                }

                $values = $targetConfig['values'] ?? [];
                if (! is_array($values)) {
                    throw new RuntimeException(sprintf(
                        'Invalid static values mapping for "%s" -> "%s". Expected values array.',
                        $table,
                        $targetTable,
                    ));
                }

                $targetDef = [
                    'connection' => $target,
                    'table' => $targetTable,
                    'operation' => (string) ($targetConfig['operation'] ?? 'upsert'),
                    'map' => $map,
                    'transforms' => $transforms,
                    'filters' => $targetConfig['filters'] ?? [],
                    'values' => $values,
                ];

                $upsertKeys = $this->normalizeStringList($targetConfig['upsert_keys'] ?? []);
                if ($upsertKeys !== []) {
                    $targetDef['keys'] = $upsertKeys;
                }

                $targets[] = $targetDef;
            }

            $tableRuntime = (array) ($tableDefinition['source']['runtime'] ?? []);
            $tableChunk = (int) Arr::get($tableRuntime, 'chunk', $defaultChunk);
            if ($tableChunk < 1) {
                throw new RuntimeException(sprintf(
                    'Invalid runtime chunk for source table "%s", expected integer >= 1.',
                    $table
                ));
            }

            $tableTransactionMode = (string) Arr::get($tableRuntime, 'transaction_mode', $defaultTransactionMode);
            if (! in_array($tableTransactionMode, ['atomic', 'batch'], true)) {
                throw new RuntimeException(sprintf(
                    'Invalid runtime transaction_mode for source table "%s", expected "atomic" or "batch".',
                    $table
                ));
            }

            $keysetColumn = trim((string) Arr::get($tableRuntime, 'keyset_column', ''));
            if ($keysetColumn !== '' && ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $keysetColumn)) {
                throw new RuntimeException(sprintf(
                    'Invalid runtime keyset_column for source table "%s", expected identifier pattern.',
                    $table
                ));
            }

            $sourceSelect = $this->resolveSourceSelectFromMapping(
                $targets,
                $sourceFilters,
                $keysetColumn,
            );

            $sourceDef = [
                'connection' => $source,
                'table' => $table,
                'chunk' => $tableChunk,
                'filters' => $sourceFilters,
            ];
            if ($keysetColumn !== '') {
                $sourceDef['keyset_column'] = $keysetColumn;
            }
            if ($sourceSelect !== null) {
                $sourceDef['select'] = $sourceSelect;
            }

            $pipelines[] = [
                'name' => $migrationName.':source:'.$table,
                'strict' => $strict,
                'transaction_mode' => $tableTransactionMode,
                'source' => $sourceDef,
                'targets' => $targets,
            ];
        }

        return $pipelines;
    }

    private function resolveMigrationName(): string
    {
        $name = $this->stringOptionOrNull('migration') ?? 'default';

        return $name === '' ? 'default' : $name;
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     * @return array<string, mixed>
     */
    private function resolveMigrationConfig(array $mappingConfig, string $migrationName): array
    {
        $migrations = $mappingConfig['migrations'] ?? null;
        if (! is_array($migrations) || $migrations === []) {
            throw new RuntimeException('Invalid dbtodb_mapping.migrations: expected non-empty array with at least the "default" migration.');
        }

        if (! array_key_exists($migrationName, $migrations)) {
            throw new RuntimeException(sprintf(
                'Unknown migration "%s". Available migrations: %s.',
                $migrationName,
                implode(', ', array_keys($migrations)),
            ));
        }

        $migration = $migrations[$migrationName];
        if (! is_array($migration)) {
            throw new RuntimeException(sprintf('Invalid dbtodb_mapping.migrations.%s: expected array.', $migrationName));
        }

        return $migration;
    }

    /**
     * @param  array<string, mixed>  $migrationConfig
     * @return array<string, mixed>
     */
    private function resolveTablesRootForPipelines(array $migrationConfig, string $migrationName): array
    {
        $step = trim((string) ($this->option('step') ?? ''));

        if (array_key_exists('steps', $migrationConfig)) {
            $steps = $migrationConfig['steps'];
            if (! is_array($steps) || $steps === [] || array_is_list($steps)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.migrations.%s.steps: expected non-empty map of step names.',
                    $migrationName,
                ));
            }

            if ($step !== '') {
                if (! array_key_exists($step, $steps)) {
                    throw new RuntimeException(sprintf(
                        'Unknown step "%s" for migration "%s". Available steps: %s.',
                        $step,
                        $migrationName,
                        implode(', ', array_keys($steps)),
                    ));
                }

                return $this->extractStepTables($steps[$step], $migrationName, $step);
            }

            $merged = [];
            foreach ($steps as $stepName => $stepConfig) {
                if (! is_string($stepName) || trim($stepName) === '') {
                    throw new RuntimeException(sprintf(
                        'Invalid dbtodb_mapping.migrations.%s.steps: every step key must be a non-empty string.',
                        $migrationName,
                    ));
                }

                foreach ($this->extractStepTables($stepConfig, $migrationName, $stepName) as $sourceTable => $definition) {
                    if (array_key_exists($sourceTable, $merged)) {
                        throw new RuntimeException(sprintf(
                            'Duplicate source table "%s" in migration "%s". Use --step to run one step, or remove the duplicate.',
                            $sourceTable,
                            $migrationName,
                        ));
                    }
                    $merged[$sourceTable] = $definition;
                }
            }

            return $merged;
        }

        if ($step !== '') {
            throw new RuntimeException(sprintf(
                'The --step option requires dbtodb_mapping.migrations.%s.steps.',
                $migrationName,
            ));
        }

        $tables = $migrationConfig['tables'] ?? [];
        if (! is_array($tables) || $tables === [] || array_is_list($tables)) {
            throw new RuntimeException(sprintf(
                'Invalid dbtodb_mapping.migrations.%s.tables: expected non-empty map of source tables.',
                $migrationName,
            ));
        }

        return $this->validateSourceTableMap($tables, sprintf('migrations.%s.tables', $migrationName));
    }

    /**
     * @return array<string, mixed>
     */
    private function extractStepTables(mixed $stepConfig, string $migrationName, string $stepName): array
    {
        if (! is_array($stepConfig)) {
            throw new RuntimeException(sprintf(
                'Invalid dbtodb_mapping.migrations.%s.steps.%s: expected array.',
                $migrationName,
                $stepName,
            ));
        }

        $tables = $stepConfig['tables'] ?? $stepConfig;
        if (! is_array($tables) || $tables === [] || array_is_list($tables)) {
            throw new RuntimeException(sprintf(
                'Invalid dbtodb_mapping.migrations.%s.steps.%s.tables: expected non-empty map of source tables.',
                $migrationName,
                $stepName,
            ));
        }

        return $this->validateSourceTableMap($tables, sprintf('migrations.%s.steps.%s.tables', $migrationName, $stepName));
    }

    /**
     * @param  array<string, mixed>  $tables
     * @return array<string, mixed>
     */
    private function validateSourceTableMap(array $tables, string $path): array
    {
        foreach ($tables as $sourceTable => $_definition) {
            if (! is_string($sourceTable) || trim($sourceTable) === '') {
                throw new RuntimeException(sprintf('Invalid dbtodb_mapping.%s: every source table key must be a non-empty string.', $path));
            }
        }

        return $tables;
    }

    /**
     * @return array{source: array<string, mixed>, targets: array<string, array<string, mixed>>}
     */
    private function normalizeMigrationTableDefinition(mixed $definition, string $sourceTable): array
    {
        if (! is_array($definition) || $definition === [] || array_is_list($definition)) {
            throw new RuntimeException(sprintf(
                'Invalid table definition for source "%s": expected target map or full table definition array.',
                $sourceTable,
            ));
        }

        $source = [
            'filters' => [],
            'runtime' => [],
        ];

        if (isset($definition['source'])) {
            if (! is_array($definition['source'])) {
                throw new RuntimeException(sprintf('Invalid source definition for table "%s": expected array.', $sourceTable));
            }
            $source['filters'] = $definition['source']['filters'] ?? [];
            $source['runtime'] = $this->normalizeRuntimeDefinition($definition['source']);
        }

        if (array_key_exists('filters', $definition)) {
            $source['filters'] = $definition['filters'];
        }
        if (array_key_exists('runtime', $definition) && is_array($definition['runtime'])) {
            $source['runtime'] = array_replace($source['runtime'], $definition['runtime']);
        }
        foreach (['chunk', 'transaction_mode', 'keyset_column'] as $runtimeKey) {
            if (array_key_exists($runtimeKey, $definition)) {
                $source['runtime'][$runtimeKey] = $definition[$runtimeKey];
            }
        }

        $targetsNode = $definition['targets'] ?? null;
        if ($targetsNode === null) {
            $targetsNode = array_diff_key($definition, array_flip([
                'source',
                'filters',
                'runtime',
                'chunk',
                'transaction_mode',
                'keyset_column',
            ]));
        }

        if (! is_array($targetsNode) || $targetsNode === [] || array_is_list($targetsNode)) {
            throw new RuntimeException(sprintf('Invalid targets for source "%s": expected non-empty target map.', $sourceTable));
        }

        $targets = [];
        foreach ($targetsNode as $targetTable => $targetDefinition) {
            if (! is_string($targetTable) || trim($targetTable) === '') {
                throw new RuntimeException(sprintf('Invalid target key for source "%s": expected non-empty string.', $sourceTable));
            }
            $targets[trim($targetTable)] = $this->normalizeTargetDefinition($targetDefinition, $sourceTable, trim($targetTable));
        }

        return ['source' => $source, 'targets' => $targets];
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeRuntimeDefinition(array $definition): array
    {
        $runtime = [];
        if (isset($definition['runtime']) && is_array($definition['runtime'])) {
            $runtime = $definition['runtime'];
        }
        foreach (['chunk', 'transaction_mode', 'keyset_column'] as $runtimeKey) {
            if (array_key_exists($runtimeKey, $definition)) {
                $runtime[$runtimeKey] = $definition[$runtimeKey];
            }
        }

        return $runtime;
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeTargetDefinition(mixed $definition, string $sourceTable, string $targetTable): array
    {
        if ($definition === null) {
            return ['columns' => []];
        }

        if (! is_array($definition)) {
            throw new RuntimeException(sprintf(
                'Invalid target definition for "%s" -> "%s": expected array.',
                $sourceTable,
                $targetTable,
            ));
        }

        $reserved = ['columns', 'transforms', 'filters', 'upsert_keys', 'operation', 'values'];
        $hasFullShape = false;
        foreach ($reserved as $key) {
            if (array_key_exists($key, $definition)) {
                $hasFullShape = true;
                break;
            }
        }

        if (! $hasFullShape) {
            return ['columns' => $definition];
        }

        $target = $definition;
        $target['columns'] = $definition['columns'] ?? [];

        return $target;
    }

    /**
     * @return list<string>
     */
    private function normalizeStringList(mixed $value): array
    {
        if (! is_array($value)) {
            return [];
        }

        return array_values(array_filter(
            array_map(static fn (mixed $item): string => is_string($item) ? trim($item) : '', $value),
            static fn (string $item): bool => $item !== '',
        ));
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
            $map = $target['map'] ?? [];
            if ($map !== []) {
                $hasNonEmptyMap = true;
            }
            foreach (array_keys((array) $map) as $key) {
                if (! is_string($key)) {
                    continue;
                }
                $sourceColumn = trim($key);
                if ($sourceColumn !== '' && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $sourceColumn)) {
                    $merged[$sourceColumn] = true;
                }
            }
        }

        if (! $hasNonEmptyMap) {
            return null;
        }

        foreach ($this->collectFilterColumnNamesFromRules($sourceFilters) as $column) {
            $merged[$column] = true;
        }
        foreach ($targets as $target) {
            foreach ($this->collectFilterColumnNamesFromRules($target['filters'] ?? []) as $column) {
                $merged[$column] = true;
            }
        }

        if ($keysetColumn !== '') {
            $merged[$keysetColumn] = true;
        }

        $columns = array_keys($merged);
        sort($columns, SORT_STRING);

        return $columns;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<string>
     */
    private function collectFilterColumnNamesFromRules(array $filters): array
    {
        $names = [];
        foreach ($this->normalizeFilterRulesForColumnHarvest($filters) as $filter) {
            $column = trim((string) ($filter['column'] ?? ''));
            if ($column !== '' && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                $names[$column] = true;
            }
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
