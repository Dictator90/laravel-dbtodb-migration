<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Arr;
use RuntimeException;

class DbToDbMappingConfigResolver
{
    /**
     * @param  array<string, mixed>  $mappingConfig
     * @return list<array<string, mixed>>
     */
    public function resolvePipelinesFromConfig(
        array $mappingConfig,
        ?string $migrationOption = null,
        string $tablesOption = '',
        string $stepOption = '',
        ?string $sourceOverride = null,
        ?string $targetOverride = null,
    ): array {
        $migrationOption = $this->blankStringToNull($migrationOption);
        $sourceOverride = $this->blankStringToNull($sourceOverride);
        $targetOverride = $this->blankStringToNull($targetOverride);

        (new DbToDbConfigValidator)->validate($mappingConfig);

        $migrationName = $migrationOption ?? 'default';
        $migrationConfig = $this->resolveMigrationConfig($mappingConfig, $migrationName);
        $tablesRoot = $this->resolveTablesRootForPipelines($migrationConfig, $migrationName, $stepOption);

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

        $tables = $this->resolveTables(array_keys($tablesRoot), $tablesOption);

        $strict = (bool) Arr::get($migrationConfig, 'strict', Arr::get($mappingConfig, 'strict', true));
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
            $sourceFilters = $tableDefinition['source']['filters'] ?? [];

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

                foreach (['deduplicate', 'on_row_error', 'write_error_mode'] as $optionKey) {
                    if (array_key_exists($optionKey, $targetConfig)) {
                        $targetDef[$optionKey] = $targetConfig[$optionKey];
                    }
                }

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

            $sourceSelect = $this->resolveSourceSelectFromMapping($targets, is_array($sourceFilters) && ! is_callable($sourceFilters) ? $sourceFilters : [], $keysetColumn);

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

    /**
     * Normalize the current migration table syntax. Supports:
     * - legacy source => target string;
     * - source => [target => column map];
     * - source => [target => full target definition];
     * - source => [targets => [...], source/runtime/filter metadata].
     *
     * @return array{source: array<string, mixed>, targets: array<string, array<string, mixed>>}
     */
    public function normalizeMigrationTableDefinition(mixed $definition, string $sourceTable): array
    {
        if (is_string($definition) && trim($definition) !== '') {
            return [
                'source' => ['filters' => [], 'runtime' => []],
                'targets' => [trim($definition) => ['columns' => []]],
            ];
        }

        if (! is_array($definition) || $definition === []) {
            throw new RuntimeException(sprintf(
                'Invalid table definition for source "%s": expected target map or table definition array.',
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

        return [
            'source' => $source,
            'targets' => $this->normalizeTargetsNode($targetsNode, $sourceTable),
        ];
    }

    /** @return list<string> */
    public function normalizeStringList(mixed $value): array
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
     * @param  array<string, mixed>  $migrationConfig
     * @return array<string, mixed>
     */
    public function resolveTablesRootForPipelines(array $migrationConfig, string $migrationName, string $stepOption = ''): array
    {
        $step = trim($stepOption);

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
     * Narrow source SELECT from non-empty per-target column maps in config (source keys only).
     * Returns null for SELECT * when every target map is empty (single target: copy all columns).
     *
     * @param  list<array<string, mixed>>  $targets
     * @param  array<int|string, mixed>  $sourceFilters
     * @return list<string>|null
     */
    public function resolveSourceSelectFromMapping(array $targets, array $sourceFilters, string $keysetColumn): ?array
    {
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

    /** @return array<string, mixed> */
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

    /** @return array<string, array<string, mixed>> */
    private function normalizeTargetsNode(mixed $targetsNode, string $sourceTable): array
    {
        if (is_array($targetsNode) && array_is_list($targetsNode)) {
            $targets = [];
            foreach ($targetsNode as $targetName) {
                if (! is_string($targetName) || trim($targetName) === '') {
                    throw new RuntimeException(sprintf('Invalid target list for source "%s": target names must be non-empty strings.', $sourceTable));
                }
                $targets[trim($targetName)] = ['columns' => []];
            }

            return $targets;
        }

        if (! is_array($targetsNode) || $targetsNode === []) {
            throw new RuntimeException(sprintf('Invalid targets for source "%s": expected non-empty target map.', $sourceTable));
        }

        $targets = [];
        foreach ($targetsNode as $targetTable => $targetDefinition) {
            if (! is_string($targetTable) || trim($targetTable) === '') {
                throw new RuntimeException(sprintf('Invalid target key for source "%s": expected non-empty string.', $sourceTable));
            }
            $targets[trim($targetTable)] = $this->normalizeTargetDefinition($targetDefinition, $sourceTable, trim($targetTable));
        }

        return $targets;
    }

    /** @return array<string, mixed> */
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

        $reserved = ['columns', 'transforms', 'filters', 'upsert_keys', 'operation', 'deduplicate', 'on_row_error', 'write_error_mode'];
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

    /** @return array<string, mixed> */
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

    /** @return list<string> */
    private function parseListOption(string $value): array
    {
        return array_values(array_filter(array_map(
            static fn (string $entry): string => trim($entry),
            explode(',', $value),
        )));
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

    /** @return list<mixed> */
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

    private function blankStringToNull(?string $value): ?string
    {
        if ($value === null) {
            return null;
        }

        $value = trim($value);

        return $value === '' ? null : $value;
    }
}
