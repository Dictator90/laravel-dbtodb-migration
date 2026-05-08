<?php

namespace MB\DbToDb\Console;

use MB\DbToDb\Support\Database\AutoIncrementSynchronizer;
use MB\DbToDb\Support\Database\DbToDbFilterEngine;
use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
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
        {--migration= : Named migration key from dbtodb_mapping.migrations}
        {--tables= : Comma-separated source tables from the selected migration or legacy config}
        {--dry-run : Validate and read data without writing}
        {--continue-on-error : Continue processing on per-pipeline failure}
        {--report-file= : Write JSON report file}
        {--source= : Override source database connection from the selected migration or legacy config}
        {--target= : Override target database connection from the selected migration or legacy config}
        {--step= : Run only this step key from selected migration steps or legacy runtime.steps_in_tables}
        {--profile : Log per-pipeline and per-chunk timings to the log channel named in dbtodb_mapping.profile_logging}';

    protected $description = 'Universal database-to-database data routing command (1 source -> N targets).';

    public function __construct(
        private DbToDbRoutingExecutor $executor,
        private AutoIncrementSynchronizer $autoIncrementSynchronizer,
        private DbToDbMappingConfigResolver $mappingConfigResolver,
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
            $source = $this->stringOptionOrNull('source');
            $target = $this->stringOptionOrNull('target');
            $reportFileOption = (string) ($this->option('report-file') ?? '');

            $pipelines = $this->resolvePipelines($source, $target);
            if ($pipelines === []) {
                $this->warn('No pipelines selected for execution.');

                return self::SUCCESS;
            }

            $resolvedSource = (string) Arr::get($pipelines[0], 'source.connection', $source ?? '');
            $resolvedTarget = (string) Arr::get($pipelines[0], 'targets.0.connection', $target ?? '');
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
    public function resolvePipelines(?string $source = null, ?string $target = null): array
    {
        return $this->resolvePipelinesFromConfig($source, $target);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function resolvePipelinesFromConfig(?string $sourceOverride = null, ?string $targetOverride = null): array
    {
        $mappingConfig = (array) config('dbtodb_mapping');
        $migrationOption = $this->stringOptionOrNull('migration');

        if ($migrationOption === null && $this->hasLegacyTablesConfig($mappingConfig)) {
            return $this->resolveLegacyPipelinesFromConfig(
                $mappingConfig,
                $sourceOverride ?: 'source',
                $targetOverride ?: 'target',
            );
        }

        $migrationName = $migrationOption ?? 'default';
        $migrationConfig = $this->resolveMigrationConfig($mappingConfig, $migrationName);
        $this->assertNoConflictingLegacyTableModes($mappingConfig, $migrationConfig, $migrationName, $migrationOption !== null);
        $tablesRoot = $this->resolveTablesRootForPipelines($migrationConfig, $migrationName);

        $source = $sourceOverride ?: trim((string) Arr::get($migrationConfig, 'source', ''));
        $target = $targetOverride ?: trim((string) Arr::get($migrationConfig, 'target', ''));
        if ($source === '') {
            throw new RuntimeException(sprintf(
                'Migration "%s": source connection is required (set dbtodb_mapping.migrations.%s.source or pass --source).',
                $migrationName,
                $migrationName,
            ));
        }
        if ($target === '') {
            throw new RuntimeException(sprintf(
                'Migration "%s": target connection is required (set dbtodb_mapping.migrations.%s.target or pass --target).',
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
            $tableDefinition = $this->mappingConfigResolver->normalizeMigrationTableDefinition($tablesRoot[$table] ?? null, $table);
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

                $targetDef = [
                    'connection' => $target,
                    'table' => $targetTable,
                    'operation' => (string) ($targetConfig['operation'] ?? 'upsert'),
                    'map' => $map,
                    'transforms' => $transforms,
                    'filters' => $targetConfig['filters'] ?? [],
                ];

                $upsertKeys = $this->mappingConfigResolver->normalizeStringList($targetConfig['upsert_keys'] ?? []);
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
        $name = $this->stringOptionOrNull('migration');
        if ($name !== null) {
            return $name;
        }

        return $this->hasLegacyTablesConfig((array) config('dbtodb_mapping')) ? 'legacy' : 'default';
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     */
    private function hasLegacyTablesConfig(array $mappingConfig): bool
    {
        return array_key_exists('tables', $mappingConfig);
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     * @param  array<string, mixed>  $migrationConfig
     */
    private function assertNoConflictingLegacyTableModes(
        array $mappingConfig,
        array $migrationConfig,
        string $migrationName,
        bool $migrationWasExplicitlySelected,
    ): void {
        if (! $migrationWasExplicitlySelected) {
            return;
        }

        $legacyStepsInTables = filter_var(
            Arr::get($mappingConfig, 'runtime.steps_in_tables', false),
            FILTER_VALIDATE_BOOLEAN,
        );
        if (! $legacyStepsInTables || ! array_key_exists('tables', $migrationConfig)) {
            return;
        }

        $step = trim((string) ($this->option('step') ?? ''));
        if ($step === '') {
            return;
        }

        throw new RuntimeException(sprintf(
            'Cannot combine --migration=%s with legacy dbtodb_mapping.runtime.steps_in_tables and --step. Define ordered steps under dbtodb_mapping.migrations.%s.steps, or remove --step. Use --tables as an additional filter inside the selected migration.',
            $migrationName,
            $migrationName,
        ));
    }

    /**
     * Build pipelines from the pre-migrations top-level config shape:
     * tables, columns, transforms, filters, upsert_keys, and runtime.tables.
     *
     * @param  array<string, mixed>  $mappingConfig
     * @return list<array<string, mixed>>
     */
    private function resolveLegacyPipelinesFromConfig(array $mappingConfig, string $source, string $target): array
    {
        $tablesRoot = $this->resolveLegacyTablesRootForPipelines($mappingConfig);
        $mappingConfig['tables'] = $tablesRoot;

        $allowedSourceTables = array_keys($tablesRoot);
        $tables = $this->resolveTables($allowedSourceTables, (string) ($this->option('tables') ?? ''));

        $strict = (bool) Arr::get($mappingConfig, 'strict', true);
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
            $tableDefinition = $this->mappingConfigResolver->normalizeLegacyTableDefinition($mappingConfig, $table);
            $targetTables = array_keys($tableDefinition['targets']);
            $filterResolution = [
                'source' => $tableDefinition['source']['filters'],
                'targets' => array_map(
                    static fn (array $target): array => (array) ($target['filters'] ?? []),
                    $tableDefinition['targets'],
                ),
            ];

            $targets = [];
            foreach ($targetTables as $targetTable) {
                $targetConfig = $tableDefinition['targets'][$targetTable];
                $map = $targetConfig['columns'] ?? [];
                if (! is_array($map)) {
                    throw new RuntimeException(sprintf(
                        'Invalid columns mapping for "%s" -> "%s". Expected columns.%s.%s array.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable,
                    ));
                }
                if (count($targetTables) > 1 && $map === []) {
                    throw new RuntimeException(sprintf(
                        'Missing columns mapping for "%s" -> "%s". Expected columns.%s.%s array for multi-target routing.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable,
                    ));
                }

                $transforms = $targetConfig['transforms'] ?? [];
                if (! is_array($transforms)) {
                    throw new RuntimeException(sprintf(
                        'Invalid transforms mapping for "%s" -> "%s". Expected transforms.%s.%s array.',
                        $table,
                        $targetTable,
                        $table,
                        $targetTable,
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

                $upsertKeys = $this->mappingConfigResolver->normalizeStringList($targetConfig['upsert_keys'] ?? []);
                if ($upsertKeys !== []) {
                    $targetDef['keys'] = $upsertKeys;
                }

                $targets[] = $targetDef;
            }

            $tableChunk = (int) Arr::get($mappingConfig, sprintf('runtime.tables.%s.chunk', $table), $defaultChunk);
            if ($tableChunk < 1) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.chunk, expected integer >= 1.',
                    $table,
                ));
            }

            $tableTransactionMode = (string) Arr::get(
                $mappingConfig,
                sprintf('runtime.tables.%s.transaction_mode', $table),
                $defaultTransactionMode,
            );
            if (! in_array($tableTransactionMode, ['atomic', 'batch'], true)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.transaction_mode, expected "atomic" or "batch".',
                    $table,
                ));
            }

            $keysetColumn = trim((string) Arr::get($mappingConfig, sprintf('runtime.tables.%s.keyset_column', $table), ''));
            if ($keysetColumn !== '' && ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $keysetColumn)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.runtime.tables.%s.keyset_column, expected identifier pattern.',
                    $table,
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
                'name' => 'legacy:source:'.$table,
                'strict' => $strict,
                'transaction_mode' => $tableTransactionMode,
                'source' => $sourceDef,
                'targets' => $targets,
            ];
        }

        return $pipelines;
    }

    /**
     * @param  array<string, mixed>  $mappingConfig
     * @return array<string, mixed>
     */
    private function resolveLegacyTablesRootForPipelines(array $mappingConfig): array
    {
        $step = trim((string) ($this->option('step') ?? ''));
        $stepsInTables = filter_var(
            Arr::get($mappingConfig, 'runtime.steps_in_tables', false),
            FILTER_VALIDATE_BOOLEAN,
        );

        $raw = $mappingConfig['tables'] ?? [];
        if (! is_array($raw)) {
            throw new RuntimeException('Invalid dbtodb_mapping.tables: expected array.');
        }

        if (! $stepsInTables) {
            if ($step !== '') {
                throw new RuntimeException(
                    'The --step option is only valid when dbtodb_mapping.runtime.steps_in_tables is true or the selected migration defines steps.'
                );
            }

            return $this->validateSourceTableMap($raw, 'tables');
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
                    implode(', ', array_keys($raw)),
                ));
            }

            $inner = $raw[$step];
            if (! is_array($inner) || $inner === []) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": expected non-empty array of source => target mappings.',
                    $step,
                ));
            }
            if (array_is_list($inner)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": inner map must use source table names as keys, not a list.',
                    $step,
                ));
            }

            return $this->validateSourceTableMap($inner, sprintf('tables.%s', $step));
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
                    $stepName,
                ));
            }
            if (array_is_list($inner)) {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables step "%s": inner map must use source table names as keys, not a list.',
                    $stepName,
                ));
            }
            $inner = $this->validateSourceTableMap($inner, sprintf('tables.%s', $stepName));
            foreach ($inner as $sourceTable => $targets) {
                if (array_key_exists($sourceTable, $merged)) {
                    throw new RuntimeException(sprintf(
                        'Duplicate source table "%s" under step "%s" and an earlier step. Use --step to run one step, or remove the duplicate.',
                        $sourceTable,
                        $stepName,
                    ));
                }
                $merged[$sourceTable] = $targets;
            }
        }

        return $merged;
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
            foreach ($this->collectTransformColumnNames((array) ($target['transforms'] ?? [])) as $column) {
                $merged[$column] = true;
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
        return (new DbToDbFilterEngine)->collectColumnNames($filters);
    }

    /**
     * @param  array<string, mixed>  $transforms
     * @return list<string>
     */
    private function collectTransformColumnNames(array $transforms): array
    {
        $columns = [];
        foreach ($transforms as $definition) {
            foreach ($this->normalizeTransformDefinitionsForColumnCollection($definition) as $rule) {
                if (! is_array($rule)) {
                    continue;
                }

                foreach ((array) ($rule['columns'] ?? []) as $column) {
                    if (is_string($column) && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                        $columns[$column] = true;
                    }
                }

                foreach ([$rule, (array) ($rule['source'] ?? []), (array) ($rule['target'] ?? [])] as $columnSource) {
                    foreach (['from_column', 'column'] as $key) {
                        $column = $columnSource[$key] ?? null;
                        if (is_string($column) && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                            $columns[$column] = true;
                        }
                    }
                }

                foreach (['items', 'values'] as $key) {
                    foreach ((array) ($rule[$key] ?? []) as $item) {
                        if (is_array($item)) {
                            $column = $item['column'] ?? null;
                            if (is_string($column) && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
                                $columns[$column] = true;
                            }
                        }
                    }
                }
            }
        }

        $names = array_keys($columns);
        sort($names, SORT_STRING);

        return $names;
    }

    /**
     * @return list<mixed>
     */
    private function normalizeTransformDefinitionsForColumnCollection(mixed $definition): array
    {
        if (! is_array($definition)) {
            return [$definition];
        }

        if (array_key_exists('rule', $definition)) {
            return [$definition];
        }

        return array_is_list($definition) ? $definition : [$definition];
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
