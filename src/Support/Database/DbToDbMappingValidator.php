<?php

namespace MB\DbToDb\Support\Database;

use RuntimeException;

class DbToDbMappingValidator
{
    /**
     * @param  array<string, mixed>  $pipeline
     */
    public function validatePipeline(array $pipeline): void
    {
        $name = (string) ($pipeline['name'] ?? '');
        $source = $pipeline['source'] ?? null;
        $targets = $pipeline['targets'] ?? null;
        $mode = (string) ($pipeline['transaction_mode'] ?? 'batch');

        if ($name === '') {
            throw new RuntimeException('Pipeline "name" is required.');
        }

        if (! is_array($source)) {
            throw new RuntimeException(sprintf('Pipeline "%s": source definition is required.', $name));
        }

        if (! is_string($source['connection'] ?? null) || ($source['connection'] ?? '') === '') {
            throw new RuntimeException(sprintf('Pipeline "%s": source.connection is required.', $name));
        }

        if (! is_string($source['table'] ?? null) || ($source['table'] ?? '') === '') {
            throw new RuntimeException(sprintf('Pipeline "%s": source.table is required.', $name));
        }

        $keysetColumn = trim((string) ($source['keyset_column'] ?? ''));
        if ($keysetColumn !== '') {
            if (! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $keysetColumn)) {
                throw new RuntimeException(sprintf(
                    'Pipeline "%s": source.keyset_column must match /^[a-zA-Z_][a-zA-Z0-9_]*$/ (got "%s").',
                    $name,
                    $keysetColumn
                ));
            }
        }

        $select = $source['select'] ?? null;
        if ($select !== null) {
            if (! is_array($select) || $select === []) {
                throw new RuntimeException(sprintf(
                    'Pipeline "%s": source.select must be a non-empty array of column identifiers when present.',
                    $name
                ));
            }
            foreach ($select as $index => $column) {
                if (! is_string($column)) {
                    throw new RuntimeException(sprintf(
                        'Pipeline "%s": source.select[%s] must be a string.',
                        $name,
                        is_string($index) ? $index : (string) $index
                    ));
                }
                $trimmed = trim($column);
                if ($trimmed === '' || ! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $trimmed)) {
                    throw new RuntimeException(sprintf(
                        'Pipeline "%s": source.select entries must be non-empty identifiers matching /^[a-zA-Z_][a-zA-Z0-9_]*$/ (got "%s").',
                        $name,
                        $column
                    ));
                }
            }
        }

        if (! in_array($mode, ['atomic', 'batch'], true)) {
            throw new RuntimeException(sprintf('Pipeline "%s": transaction_mode must be atomic or batch.', $name));
        }

        if (! is_array($targets) || $targets === []) {
            throw new RuntimeException(sprintf('Pipeline "%s": at least one target is required.', $name));
        }

        foreach ($targets as $index => $target) {
            if (! is_array($target)) {
                throw new RuntimeException(sprintf('Pipeline "%s": target[%d] must be object.', $name, $index));
            }

            if (! is_string($target['connection'] ?? null) || ($target['connection'] ?? '') === '') {
                throw new RuntimeException(sprintf('Pipeline "%s": target[%d].connection is required.', $name, $index));
            }

            if (! is_string($target['table'] ?? null) || ($target['table'] ?? '') === '') {
                throw new RuntimeException(sprintf('Pipeline "%s": target[%d].table is required.', $name, $index));
            }

            $operation = (string) ($target['operation'] ?? 'upsert');
            if (! in_array($operation, ['upsert', 'insert', 'truncate_insert'], true)) {
                throw new RuntimeException(sprintf('Pipeline "%s": target[%d].operation must be upsert, insert, or truncate_insert.', $name, $index));
            }
        }
    }
}
