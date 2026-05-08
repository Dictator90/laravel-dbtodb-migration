<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Arr;
use RuntimeException;

class DbToDbMappingConfigResolver
{
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
     * Normalize the historical top-level config shape:
     * tables[source] = target(s), columns[source][target] = map,
     * transforms/filters/upsert_keys separated by source and optional target.
     *
     * @param  array<string, mixed>  $mappingConfig
     * @return array{source: array<string, mixed>, targets: array<string, array<string, mixed>>}
     */
    public function normalizeLegacyTableDefinition(array $mappingConfig, string $sourceTable): array
    {
        $targetTables = $this->normalizeLegacyTargetTables($mappingConfig, $sourceTable);
        $filterResolution = $this->resolveLegacyFiltersForTable($mappingConfig, $sourceTable, $targetTables);

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
                'transforms' => $this->resolveLegacyTransformsForTarget($mappingConfig, $sourceTable, $targetTable, $targetTables),
                'filters' => $filterResolution['targets'][$targetTable] ?? [],
                'upsert_keys' => $this->resolveLegacyUpsertKeysForTarget($mappingConfig, $sourceTable, $targetTable),
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

    /**
     * @return list<string>
     */
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
     * @return array<string, array<string, mixed>>
     */
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

    /**
     * @return list<string>
     */
    private function normalizeLegacyTargetTables(array $mappingConfig, string $table): array
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
    private function resolveLegacyFiltersForTable(array $mappingConfig, string $table, array $targetTables): array
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
    private function resolveLegacyTransformsForTarget(
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

    /**
     * @return list<string>
     */
    private function resolveLegacyUpsertKeysForTarget(array $mappingConfig, string $sourceTable, string $targetTable): array
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
}
