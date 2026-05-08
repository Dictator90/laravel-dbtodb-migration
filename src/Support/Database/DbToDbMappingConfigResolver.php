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

        if ($migrationOption === null && $this->hasLegacyTablesConfig($mappingConfig)) {
            return $this->resolveLegacyPipelinesFromConfig(
                $mappingConfig,
                $sourceOverride ?: 'source',
                $targetOverride ?: 'target',
                $tablesOption,
                $stepOption,
            );
        }

        $migrationName = $migrationOption ?? 'default';
        $migrationConfig = $this->resolveMigrationConfig($mappingConfig, $migrationName);
        $this->assertNoConflictingLegacyTableModes($mappingConfig, $migrationConfig, $migrationName, $migrationOption !== null, $stepOption);
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

            $sourceSelect = $this->resolveSourceSelectFromMapping($targets, $sourceFilters, $keysetColumn);

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

    /**
     * Normalize the historical top-level config shape.
     *
     * @param  array<string, mixed>  $mappingConfig
     * @return array{source: array<string, mixed>, targets: array<string, array<string, mixed>>}
     */
    public function normalizeLegacyTableDefinition(array $mappingConfig, string $sourceTable): array
    {
        $targetTables = $this->normalizeTargetTables($mappingConfig, $sourceTable);
        $filterResolution = $this->resolveFiltersForTable($mappingConfig, $sourceTable, $targetTables);

        $targets = [];
        foreach ($targetTables as $targetTable) {
            $map = Arr::get($mappingConfig, sprintf('columns.%s.%s', $sourceTable, $targetTable), []);
            if (! is_array($map)) {
                throw new RuntimeException(sprintf(
                    'Invalid columns mapping for "%s" -> "%s". Expected columns.%s.%s array.',
                    $sourceTable,
                    $targetTable,
                    $sourceTable,
                    $targetTable,
                ));
            }

            $targets[$targetTable] = [
                'columns' => $map,
                'transforms' => $this->resolveTransformsForTarget($mappingConfig, $sourceTable, $targetTable, $targetTables),
                'filters' => $filterResolution['targets'][$targetTable] ?? [],
                'upsert_keys' => $this->resolveUpsertKeysForTarget($mappingConfig, $sourceTable, $targetTable),
                'operation' => 'upsert',
            ];
        }

        return [
            'source' => [
                'filters' => $filterResolution['source'],
                'runtime' => (array) Arr::get($mappingConfig, sprintf('runtime.tables.%s', $sourceTable), []),
            ],
            'targets' => $targets,
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

    /** @return list<string> */
    public function normalizeTargetTables(array $mappingConfig, string $table): array
    {
        $raw = Arr::get($mappingConfig, 'tables.'.$table);

        if (is_string($raw) && trim($raw) !== '') {
            return [trim($raw)];
        }

        if (! is_array($raw) || $raw === []) {
            throw new RuntimeException(sprintf(
                'Invalid dbtodb_mapping.tables for "%s": expected non-empty string or array of target tables.',
                $table,
            ));
        }

        $normalized = [];
        foreach ($raw as $targetTable) {
            if (! is_string($targetTable) || trim($targetTable) === '') {
                throw new RuntimeException(sprintf(
                    'Invalid dbtodb_mapping.tables for "%s": every target table must be non-empty string.',
                    $table,
                ));
            }
            $normalized[] = trim($targetTable);
        }

        return $normalized;
    }

    /**
     * @param  list<string>  $targetTables
     * @return array{source: array<int|string, mixed>, targets: array<string, array<int|string, mixed>>}
     */
    public function resolveFiltersForTable(array $mappingConfig, string $table, array $targetTables): array
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
                $table,
            ));
        }

        $targetFilters = [];
        foreach ($targetTables as $targetTable) {
            $rules = $raw[$targetTable] ?? $defaultRules;
            if (! is_array($rules)) {
                throw new RuntimeException(sprintf(
                    'Invalid filters.%s.%s: expected rules array.',
                    $table,
                    $targetTable,
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
     * @param  list<string>  $knownTargetTables
     * @return array<string, mixed>
     */
    public function resolveTransformsForTarget(
        array $mappingConfig,
        string $sourceTable,
        string $targetTable,
        array $knownTargetTables,
    ): array {
        $tableTransforms = Arr::get($mappingConfig, sprintf('transforms.%s', $sourceTable), []);
        if (! is_array($tableTransforms)) {
            throw new RuntimeException(sprintf(
                'Invalid transforms for "%s": expected array.',
                $sourceTable,
            ));
        }

        $targetTransforms = Arr::get($tableTransforms, $targetTable, []);
        if ($targetTransforms !== [] && ! is_array($targetTransforms)) {
            throw new RuntimeException(sprintf(
                'Invalid transforms mapping for "%s" -> "%s". Expected transforms.%s.%s array.',
                $sourceTable,
                $targetTable,
                $sourceTable,
                $targetTable,
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

    /** @return list<string> */
    public function resolveUpsertKeysForTarget(array $mappingConfig, string $sourceTable, string $targetTable): array
    {
        $node = Arr::get($mappingConfig, sprintf('upsert_keys.%s', $sourceTable));
        if (! is_array($node) || $node === []) {
            return [];
        }

        if (array_is_list($node)) {
            return $this->normalizeStringList($node);
        }

        $specific = $node[$targetTable] ?? null;
        if (! is_array($specific)) {
            return [];
        }

        return $this->normalizeStringList($specific);
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
    private function resolveLegacyTablesRootForPipelines(array $mappingConfig, string $stepOption = ''): array
    {
        $step = trim($stepOption);
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

    private function resolveLegacyPipelinesFromConfig(
        array $mappingConfig,
        string $source,
        string $target,
        string $tablesOption,
        string $stepOption,
    ): array {
        $tablesRoot = $this->resolveLegacyTablesRootForPipelines($mappingConfig, $stepOption);
        $mappingConfig['tables'] = $tablesRoot;

        $tables = $this->resolveTables(array_keys($tablesRoot), $tablesOption);

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
            $tableDefinition = $this->normalizeLegacyTableDefinition($mappingConfig, $table);
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

                $upsertKeys = $this->normalizeStringList($targetConfig['upsert_keys'] ?? []);
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

    private function hasLegacyTablesConfig(array $mappingConfig): bool
    {
        return array_key_exists('tables', $mappingConfig);
    }

    /**
     * @param  array<string, mixed>  $migrationConfig
     */
    private function assertNoConflictingLegacyTableModes(
        array $mappingConfig,
        array $migrationConfig,
        string $migrationName,
        bool $migrationWasExplicitlySelected,
        string $stepOption,
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

        if (trim($stepOption) === '') {
            return;
        }

        throw new RuntimeException(sprintf(
            'Cannot combine --migration=%s with legacy dbtodb_mapping.runtime.steps_in_tables and --step. Define ordered steps under dbtodb_mapping.migrations.%s.steps, or remove --step. Use --tables as an additional filter inside the selected migration.',
            $migrationName,
            $migrationName,
        ));
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

        $reserved = ['columns', 'transforms', 'filters', 'upsert_keys', 'operation'];
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
