<?php

namespace MB\DbToDb\Console;

use MB\DbToDb\Support\Database\AutoIncrementSynchronizer;
use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
use MB\DbToDb\Support\Database\DbToDbReportWriter;
use MB\DbToDb\Support\Database\DbToDbRoutingExecutor;
use MB\DbToDb\Support\ProfileLoggingChannel;
use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
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
        private DbToDbReportWriter $reportWriter,
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
            $reportStream = $this->reportWriter->open($reportPath, $dryRun, $continueOnError);
            $tableRows = [];
            $failedCount = 0;

            try {
                $this->executor->runEach(
                    $pipelines,
                    $dryRun,
                    $continueOnError,
                    function (array $report) use (&$tableRows, &$failedCount, &$reportStream): void {
                        $this->reportWriter->append($reportStream, $report);
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
                $this->reportWriter->close($reportStream);
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
        return $this->mappingConfigResolver->resolvePipelinesFromConfig(
            (array) config('dbtodb_mapping'),
            $this->stringOptionOrNull('migration'),
            (string) ($this->option('tables') ?? ''),
            (string) ($this->option('step') ?? ''),
            $sourceOverride,
            $targetOverride,
        );
    }

    private function resolveMigrationName(): string
    {
        $name = $this->stringOptionOrNull('migration');
        if ($name !== null) {
            return $name;
        }

        return array_key_exists('tables', (array) config('dbtodb_mapping')) ? 'legacy' : 'default';
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
