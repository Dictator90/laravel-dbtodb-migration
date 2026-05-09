<?php

namespace MB\DbToDb\Support\Database;

use RuntimeException;

class DbToDbConfigValidator
{
    private const OPERATION_VALUES = ['upsert', 'insert', 'truncate_insert'];

    /**
     * @param  array<string, mixed>  $mappingConfig
     */
    public function validate(array $mappingConfig): void
    {
        $migrations = $mappingConfig['migrations'] ?? null;
        $this->assertMap($migrations, 'dbtodb_mapping.migrations');

        foreach ($migrations as $migrationName => $migrationConfig) {
            if (! is_string($migrationName) || trim($migrationName) === '') {
                throw new RuntimeException('Invalid dbtodb_mapping.migrations: every migration name must be a non-empty string.');
            }

            $migrationPath = 'dbtodb_mapping.migrations.'.$migrationName;
            $this->assertMap($migrationConfig, $migrationPath);

            if (array_key_exists('runtime', $migrationConfig)) {
                $this->validateRuntime($migrationConfig['runtime'], $migrationPath.'.runtime', true);
            }

            if (array_key_exists('steps', $migrationConfig)) {
                $this->validateSteps($migrationConfig['steps'], $migrationPath.'.steps');
                continue;
            }

            $this->validateTables($migrationConfig['tables'] ?? null, $migrationPath.'.tables');
        }
    }

    private function validateSteps(mixed $steps, string $path): void
    {
        $this->assertMap($steps, $path);

        foreach ($steps as $stepName => $stepConfig) {
            if (! is_string($stepName) || trim($stepName) === '') {
                throw new RuntimeException(sprintf('Invalid %s: every step name must be a non-empty string.', $path));
            }

            $stepPath = $path.'.'.$stepName;
            if (! is_array($stepConfig)) {
                throw new RuntimeException(sprintf('Invalid %s: expected map.', $stepPath));
            }

            $tables = array_key_exists('tables', $stepConfig) ? $stepConfig['tables'] : $stepConfig;
            $tablesPath = array_key_exists('tables', $stepConfig) ? $stepPath.'.tables' : $stepPath;
            $this->validateTables($tables, $tablesPath);
        }
    }

    private function validateTables(mixed $tables, string $path): void
    {
        $this->assertMap($tables, $path);

        foreach ($tables as $sourceTable => $definition) {
            if (! is_string($sourceTable) || trim($sourceTable) === '') {
                throw new RuntimeException(sprintf('Invalid %s: every source table name must be a non-empty string.', $path));
            }

            $this->validateTableDefinition($definition, $path.'.'.$sourceTable);
        }
    }

    private function validateTableDefinition(mixed $definition, string $path): void
    {
        if (is_string($definition)) {
            if (trim($definition) === '') {
                throw new RuntimeException(sprintf('Invalid %s: target table name must be a non-empty string.', $path));
            }

            return;
        }

        if (! is_array($definition) || $definition === []) {
            throw new RuntimeException(sprintf('Invalid %s: expected non-empty target map or table definition.', $path));
        }

        if (array_key_exists('source', $definition)) {
            $this->validateSourceDefinition($definition['source'], $path.'.source');
        }

        if (array_key_exists('filters', $definition)) {
            $this->validateFilters($definition['filters'], $path.'.filters', true);
        }

        if (array_key_exists('runtime', $definition)) {
            $this->validateRuntime($definition['runtime'], $path.'.runtime');
        }

        foreach (['chunk', 'transaction_mode', 'keyset_column'] as $runtimeKey) {
            if (array_key_exists($runtimeKey, $definition)) {
                $this->validateRuntimeValue($runtimeKey, $definition[$runtimeKey], $path.'.'.$runtimeKey);
            }
        }

        if (array_key_exists('targets', $definition)) {
            $this->validateTargets($definition['targets'], $path.'.targets');

            return;
        }

        $targets = array_diff_key($definition, array_flip([
            'source',
            'filters',
            'runtime',
            'chunk',
            'transaction_mode',
            'keyset_column',
        ]));
        $this->validateTargets($targets, $path);
    }

    private function validateSourceDefinition(mixed $source, string $path): void
    {
        if (! is_array($source)) {
            throw new RuntimeException(sprintf('Invalid %s: expected map.', $path));
        }

        if (array_key_exists('filters', $source)) {
            $this->validateFilters($source['filters'], $path.'.filters', true);
        }

        if (array_key_exists('runtime', $source)) {
            $this->validateRuntime($source['runtime'], $path.'.runtime');
        }

        foreach (['chunk', 'transaction_mode', 'keyset_column'] as $runtimeKey) {
            if (array_key_exists($runtimeKey, $source)) {
                $this->validateRuntimeValue($runtimeKey, $source[$runtimeKey], $path.'.'.$runtimeKey);
            }
        }
    }

    private function validateTargets(mixed $targets, string $path): void
    {
        if (is_array($targets) && array_is_list($targets)) {
            if ($targets === []) {
                throw new RuntimeException(sprintf('Invalid %s: expected non-empty target map.', $path));
            }

            foreach ($targets as $index => $targetName) {
                if (! is_string($targetName) || trim($targetName) === '') {
                    throw new RuntimeException(sprintf('Invalid %s.%s: target table name must be a non-empty string.', $path, $index));
                }
            }

            return;
        }

        $this->assertMap($targets, $path);

        foreach ($targets as $targetTable => $targetDefinition) {
            if (! is_string($targetTable) || trim($targetTable) === '') {
                throw new RuntimeException(sprintf('Invalid %s: every target table name must be a non-empty string.', $path));
            }

            $this->validateTargetDefinition($targetDefinition, $path.'.'.$targetTable);
        }
    }

    private function validateTargetDefinition(mixed $definition, string $path): void
    {
        if ($definition === null) {
            return;
        }

        if (! is_array($definition)) {
            throw new RuntimeException(sprintf('Invalid %s: expected target definition map.', $path));
        }

        $reserved = ['columns', 'filters', 'transforms', 'operation', 'upsert_keys', 'runtime', 'deduplicate', 'on_row_error', 'write_error_mode'];
        $hasFullShape = false;
        foreach ($reserved as $key) {
            if (array_key_exists($key, $definition)) {
                $hasFullShape = true;
                break;
            }
        }

        if (! $hasFullShape) {
            $this->validateColumns($definition, $path.'.columns');

            return;
        }

        if (array_key_exists('columns', $definition)) {
            $this->validateColumns($definition['columns'], $path.'.columns');
        }

        if (array_key_exists('filters', $definition)) {
            $this->validateFilters($definition['filters'], $path.'.filters');
        }

        if (array_key_exists('transforms', $definition)) {
            $this->validateTransforms($definition['transforms'], $path.'.transforms');
        }

        if (array_key_exists('operation', $definition)) {
            $this->validateOperation($definition['operation'], $path.'.operation');
        }

        if (array_key_exists('upsert_keys', $definition)) {
            $this->validateUpsertKeys($definition['upsert_keys'], $path.'.upsert_keys');
        }

        if (array_key_exists('runtime', $definition)) {
            $this->validateRuntime($definition['runtime'], $path.'.runtime');
        }

        if (array_key_exists('deduplicate', $definition)) {
            $this->validateDeduplicate($definition['deduplicate'], $path.'.deduplicate');
        }

        foreach (['on_row_error', 'write_error_mode'] as $errorModeKey) {
            if (array_key_exists($errorModeKey, $definition)) {
                $this->validateWriteErrorMode($definition[$errorModeKey], $path.'.'.$errorModeKey);
            }
        }
    }

    private function validateColumns(mixed $columns, string $path): void
    {
        $this->assertMap($columns, $path, allowEmpty: true);

        foreach ($columns as $sourceColumn => $targetColumn) {
            if (! is_string($sourceColumn) || trim($sourceColumn) === '') {
                throw new RuntimeException(sprintf('Invalid %s: every source column must be a non-empty string.', $path));
            }

            if (! is_string($targetColumn) || trim($targetColumn) === '') {
                throw new RuntimeException(sprintf('Invalid %s.%s: target column must be a non-empty string.', $path, $sourceColumn));
            }
        }
    }

    private function validateFilters(mixed $filters, string $path, bool $allowCallable = false): void
    {
        if (! is_array($filters) && (! $allowCallable || ! is_callable($filters))) {
            $expected = $allowCallable ? 'filters array or callable' : 'filters array';
            throw new RuntimeException(sprintf('Invalid %s: expected %s.', $path, $expected));
        }
    }

    private function validateTransforms(mixed $transforms, string $path): void
    {
        if (! is_array($transforms)) {
            throw new RuntimeException(sprintf('Invalid %s: expected transforms array.', $path));
        }
    }

    private function validateDeduplicate(mixed $deduplicate, string $path): void
    {
        if (is_bool($deduplicate)) {
            return;
        }

        if (! is_array($deduplicate)) {
            throw new RuntimeException(sprintf('Invalid %s: expected bool or map.', $path));
        }

        if (array_key_exists('keys', $deduplicate)) {
            $this->validateUpsertKeys($deduplicate['keys'], $path.'.keys');
        }

        if (array_key_exists('strategy', $deduplicate) && (! is_string($deduplicate['strategy']) || ! in_array($deduplicate['strategy'], ['first', 'last'], true))) {
            throw new RuntimeException(sprintf('Invalid %s.strategy: expected first or last.', $path));
        }
    }

    private function validateWriteErrorMode(mixed $mode, string $path): void
    {
        if (! is_string($mode) || ! in_array($mode, ['fail', 'skip_row'], true)) {
            throw new RuntimeException(sprintf('Invalid %s: expected fail or skip_row.', $path));
        }
    }

    private function validateOperation(mixed $operation, string $path): void
    {
        if (! is_string($operation) || ! in_array($operation, self::OPERATION_VALUES, true)) {
            throw new RuntimeException(sprintf(
                'Invalid %s: expected one of %s.',
                $path,
                implode(', ', self::OPERATION_VALUES),
            ));
        }
    }

    private function validateUpsertKeys(mixed $upsertKeys, string $path): void
    {
        if (! is_array($upsertKeys) || ! array_is_list($upsertKeys)) {
            throw new RuntimeException(sprintf('Invalid %s: expected list of non-empty strings.', $path));
        }

        foreach ($upsertKeys as $index => $key) {
            if (! is_string($key) || trim($key) === '') {
                throw new RuntimeException(sprintf('Invalid %s.%s: expected non-empty string.', $path, $index));
            }
        }
    }

    private function validateRuntime(mixed $runtime, string $path, bool $allowDefaults = false): void
    {
        if (! is_array($runtime)) {
            throw new RuntimeException(sprintf('Invalid %s: expected runtime map.', $path));
        }

        foreach (['chunk', 'transaction_mode', 'keyset_column'] as $runtimeKey) {
            if (array_key_exists($runtimeKey, $runtime)) {
                $this->validateRuntimeValue($runtimeKey, $runtime[$runtimeKey], $path.'.'.$runtimeKey);
            }
        }

        if ($allowDefaults && array_key_exists('defaults', $runtime)) {
            $this->validateRuntime($runtime['defaults'], $path.'.defaults');
        }
    }

    private function validateRuntimeValue(string $key, mixed $value, string $path): void
    {
        if ($key === 'chunk') {
            if (! is_int($value) && ! (is_string($value) && ctype_digit($value))) {
                throw new RuntimeException(sprintf('Invalid %s: expected integer >= 1.', $path));
            }

            if ((int) $value < 1) {
                throw new RuntimeException(sprintf('Invalid %s: expected integer >= 1.', $path));
            }

            return;
        }

        if ($key === 'transaction_mode') {
            if (! is_string($value) || ! in_array($value, ['atomic', 'batch'], true)) {
                throw new RuntimeException(sprintf('Invalid %s: expected atomic or batch.', $path));
            }

            return;
        }

        if ($key === 'keyset_column') {
            if (! is_string($value) || trim($value) === '' || ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', trim($value))) {
                throw new RuntimeException(sprintf('Invalid %s: expected identifier pattern.', $path));
            }
        }
    }

    private function assertMap(mixed $value, string $path, bool $allowEmpty = false): void
    {
        if (! is_array($value) || array_is_list($value) || (! $allowEmpty && $value === [])) {
            throw new RuntimeException(sprintf('Invalid %s: expected non-empty map.', $path));
        }
    }
}
