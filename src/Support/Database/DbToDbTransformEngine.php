<?php

namespace MB\DbToDb\Support\Database;

use DateTimeImmutable;
use DateTimeInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use RuntimeException;

class DbToDbTransformEngine
{
    /**
     * @var array<string, mixed>
     */
    private array $lookupCache = [];

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    public function apply(
        mixed $value,
        mixed $transformDefinition,
        array $sourceRow = [],
        ?string $sourceColumn = null,
        ?string $targetColumn = null,
        ?string $targetTable = null,
    ): mixed {
        if ($transformDefinition === null) {
            return $this->normalizeValue($value);
        }

        $current = $value;
        foreach ($this->normalizeTransformList($transformDefinition) as $transform) {
            $current = $this->applyTransformRule($current, $transform, $sourceRow, $sourceColumn, $targetColumn, $targetTable);
        }

        return $this->normalizeValue($current);
    }

    /**
     * @return list<mixed>
     */
    private function normalizeTransformList(mixed $definition): array
    {
        if (! is_array($definition)) {
            return [$definition];
        }

        if (array_key_exists('rule', $definition) || $definition instanceof \Closure) {
            return [$definition];
        }

        return array_is_list($definition) ? $definition : [$definition];
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyTransformRule(
        mixed $current,
        mixed $transform,
        array $sourceRow,
        ?string $sourceColumn,
        ?string $targetColumn,
        ?string $targetTable,
    ): mixed {
        if ($transform instanceof \Closure) {
            return $transform($current, $sourceRow, $sourceColumn, $targetColumn, $targetTable);
        }

        if (is_string($transform)) {
            return $this->applyStringRule($current, $transform);
        }

        if (! is_array($transform)) {
            throw new RuntimeException(sprintf(
                'Unsupported transform definition type "%s".',
                get_debug_type($transform)
            ));
        }

        $ruleName = (string) ($transform['rule'] ?? '');

        return match ($ruleName) {
            'default' => $current === null ? $this->requireRuleValue($transform, 'default') : $current,
            'static' => $this->requireRuleValue($transform, 'static'),
            'map', 'enum' => $this->applyMapRule($current, $transform),
            'replace' => $this->applyReplaceRule($current, $transform),
            'regex_replace' => $this->applyRegexReplaceRule($current, $transform),
            'substr' => $this->applySubstrRule($current, $transform),
            'lower' => $current === null ? null : mb_strtolower((string) $current),
            'upper' => $current === null ? null : mb_strtoupper((string) $current),
            'slug' => $current === null ? null : Str::slug((string) $current, (string) ($transform['separator'] ?? '-')),
            'date_format' => $this->applyDateFormatRule($current, $transform),
            'cast' => $this->applyCastRule($current, $transform),
            'concat' => $this->applyConcatRule($current, $transform, $sourceRow),
            'coalesce' => $this->applyCoalesceRule($current, $transform, $sourceRow),
            'from_columns' => $this->applyFromColumnsRule($transform, $sourceRow),
            'lookup' => $this->applyLookupRule($current, $transform, $sourceRow),
            'if_eq' => $this->applyIfEqRule($current, $transform),
            'multiply' => $this->applyMultiplyRule($current, $transform),
            'round_precision' => $this->applyRoundPrecisionRule($current, $transform, $sourceRow),
            'invoke' => $this->applyInvokeRule($current, $transform, $sourceRow, $sourceColumn, $targetColumn, $targetTable),
            default => throw new RuntimeException(sprintf('Unsupported transform rule "%s".', $ruleName)),
        };
    }

    private function applyStringRule(mixed $current, string $rule): mixed
    {
        return match ($rule) {
            'trim' => is_string($current) ? trim($current) : $current,
            'null_if_empty' => is_string($current) && trim($current) === '' ? null : $current,
            'zero_date_to_null' => $this->applyZeroDateToNullTransform($current),
            'lower' => $current === null ? null : mb_strtolower((string) $current),
            'upper' => $current === null ? null : mb_strtoupper((string) $current),
            'slug' => $current === null ? null : Str::slug((string) $current),
            default => throw new RuntimeException(sprintf('Unsupported transform rule "%s".', $rule)),
        };
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function requireRuleValue(array $transform, string $ruleName): mixed
    {
        if (! array_key_exists('value', $transform)) {
            throw new RuntimeException(sprintf('Invalid transform rule "%s": missing "value".', $ruleName));
        }

        return $transform['value'];
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyMapRule(mixed $current, array $transform): mixed
    {
        $map = $transform['map'] ?? $transform['values'] ?? null;
        if (! is_array($map)) {
            throw new RuntimeException('Invalid transform rule "map": missing "map" array.');
        }

        if ((is_int($current) || is_string($current)) && array_key_exists($current, $map)) {
            return $map[$current];
        }

        if (is_bool($current)) {
            $key = $current ? '1' : '0';
            if (array_key_exists($key, $map)) {
                return $map[$key];
            }
        }

        return array_key_exists('default', $transform) ? $transform['default'] : $current;
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyReplaceRule(mixed $current, array $transform): mixed
    {
        if ($current === null) {
            return null;
        }
        if (! array_key_exists('search', $transform) || ! array_key_exists('replace', $transform)) {
            throw new RuntimeException('Invalid transform rule "replace": missing "search" or "replace".');
        }

        return str_replace($transform['search'], $transform['replace'], (string) $current);
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyRegexReplaceRule(mixed $current, array $transform): mixed
    {
        if ($current === null) {
            return null;
        }
        $pattern = $transform['pattern'] ?? null;
        if (! is_string($pattern) || $pattern === '' || ! array_key_exists('replace', $transform)) {
            throw new RuntimeException('Invalid transform rule "regex_replace": missing "pattern" or "replace".');
        }

        $result = preg_replace($pattern, (string) $transform['replace'], (string) $current);
        if ($result === null) {
            throw new RuntimeException('Invalid transform rule "regex_replace": preg_replace failed.');
        }

        return $result;
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applySubstrRule(mixed $current, array $transform): mixed
    {
        if ($current === null) {
            return null;
        }

        $start = (int) ($transform['start'] ?? $transform['offset'] ?? 0);
        if (array_key_exists('length', $transform)) {
            return mb_substr((string) $current, $start, (int) $transform['length']);
        }

        return mb_substr((string) $current, $start);
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyDateFormatRule(mixed $current, array $transform): mixed
    {
        if ($current === null || $current === '') {
            return null;
        }

        $format = $transform['format'] ?? null;
        if (! is_string($format) || $format === '') {
            throw new RuntimeException('Invalid transform rule "date_format": missing "format".');
        }

        if ($current instanceof DateTimeInterface) {
            return $current->format($format);
        }

        $from = $transform['from'] ?? null;
        if (is_string($from) && $from !== '') {
            $date = DateTimeImmutable::createFromFormat($from, (string) $current);
            if ($date instanceof DateTimeImmutable) {
                return $date->format($format);
            }
        }

        return (new DateTimeImmutable((string) $current))->format($format);
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyCastRule(mixed $current, array $transform): mixed
    {
        $type = strtolower((string) ($transform['type'] ?? $transform['to'] ?? ''));

        return match ($type) {
            'int', 'integer' => $current === null || $current === '' ? null : (int) $current,
            'float', 'double' => $current === null || $current === '' ? null : (float) $current,
            'string' => $current === null ? null : (string) $current,
            'bool', 'boolean' => $this->castToBool($current),
            'json' => $this->castToJson($current),
            'array' => $this->castToArray($current),
            default => throw new RuntimeException(sprintf('Invalid transform rule "cast": unsupported type "%s".', $type)),
        };
    }

    private function castToBool(mixed $value): ?bool
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (is_bool($value)) {
            return $value;
        }
        if (is_int($value) || is_float($value)) {
            return $value != 0;
        }

        return in_array(strtolower(trim((string) $value)), ['1', 'true', 't', 'yes', 'y', 'on'], true);
    }

    private function castToJson(mixed $value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (is_string($value)) {
            json_decode($value, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return $value;
            }
        }

        $encoded = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($encoded === false) {
            throw new RuntimeException('Invalid transform rule "cast": JSON encoding failed.');
        }

        return $encoded;
    }

    /**
     * @return array<mixed>
     */
    private function castToArray(mixed $value): array
    {
        if ($value === null || $value === '') {
            return [];
        }
        if (is_array($value)) {
            return $value;
        }
        if (is_string($value)) {
            $decoded = json_decode($value, true);
            if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                return $decoded;
            }
        }

        return [$value];
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyConcatRule(mixed $current, array $transform, array $sourceRow): string
    {
        $items = $transform['items'] ?? $transform['values'] ?? null;
        if ($items === null) {
            $items = [$current];
        }
        if (! is_array($items)) {
            throw new RuntimeException('Invalid transform rule "concat": "items" must be an array.');
        }

        $separator = (string) ($transform['separator'] ?? '');
        $skipNull = (bool) ($transform['skip_null'] ?? true);
        $parts = [];
        foreach ($items as $item) {
            $resolved = $this->resolveValueExpression($item, $current, $sourceRow);
            if ($skipNull && $resolved === null) {
                continue;
            }
            $parts[] = (string) $resolved;
        }

        return implode($separator, $parts);
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyCoalesceRule(mixed $current, array $transform, array $sourceRow): mixed
    {
        $items = $transform['items'] ?? $transform['values'] ?? null;
        if ($items === null) {
            $items = ['$value'];
        }
        if (! is_array($items)) {
            throw new RuntimeException('Invalid transform rule "coalesce": "items" must be an array.');
        }

        $emptyAsNull = (bool) ($transform['empty_as_null'] ?? true);
        foreach ($items as $item) {
            $resolved = $this->resolveValueExpression($item, $current, $sourceRow);
            if ($resolved === null) {
                continue;
            }
            if ($emptyAsNull && is_string($resolved) && trim($resolved) === '') {
                continue;
            }

            return $resolved;
        }

        return $transform['default'] ?? null;
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyFromColumnsRule(array $transform, array $sourceRow): mixed
    {
        $columns = $transform['columns'] ?? null;
        if (! is_array($columns) || $columns === []) {
            throw new RuntimeException('Invalid transform rule "from_columns": missing "columns" array.');
        }

        if (isset($transform['template']) && is_string($transform['template'])) {
            return preg_replace_callback('/\{([a-zA-Z_][a-zA-Z0-9_]*)\}/', static function (array $matches) use ($sourceRow): string {
                return (string) ($sourceRow[$matches[1]] ?? '');
            }, $transform['template']);
        }

        $values = [];
        foreach ($columns as $column) {
            $column = (string) $column;
            $values[$column] = $sourceRow[$column] ?? null;
        }

        if (array_key_exists('separator', $transform)) {
            $skipNull = (bool) ($transform['skip_null'] ?? true);
            $parts = [];
            foreach ($values as $value) {
                if ($skipNull && $value === null) {
                    continue;
                }
                $parts[] = (string) $value;
            }

            return implode((string) $transform['separator'], $parts);
        }

        return $values;
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyLookupRule(mixed $current, array $transform, array $sourceRow): mixed
    {
        $lookup = $this->normalizeLookupDefinition($transform);
        $table = $lookup['table'] ?? null;
        if (! is_string($table) || $table === '') {
            throw new RuntimeException('Invalid transform rule "lookup": missing "table".');
        }

        $keyColumn = (string) ($lookup['key'] ?? $lookup['key_column'] ?? $lookup['source_column'] ?? 'id');
        $valueColumn = (string) ($lookup['value'] ?? $lookup['value_column'] ?? $lookup['target_column'] ?? 'id');
        $lookupValue = array_key_exists('from_column', $lookup)
            ? ($sourceRow[(string) $lookup['from_column']] ?? null)
            : $current;

        if ($lookupValue === null) {
            return $lookup['default'] ?? null;
        }

        $connection = $lookup['connection'] ?? null;
        $connectionName = is_string($connection) && $connection !== '' ? $connection : null;
        $cacheKey = implode("\0", [$connectionName ?? '', $table, $keyColumn, $valueColumn, (string) $lookupValue]);
        if (! array_key_exists($cacheKey, $this->lookupCache)) {
            $this->lookupCache[$cacheKey] = DB::connection($connectionName)
                ->table($table)
                ->where($keyColumn, '=', $lookupValue)
                ->value($valueColumn);
        }

        return $this->lookupCache[$cacheKey] ?? ($lookup['default'] ?? null);
    }

    /**
     * @param  array<string, mixed>  $transform
     * @return array<string, mixed>
     */
    private function normalizeLookupDefinition(array $transform): array
    {
        foreach (['source', 'target'] as $key) {
            if (isset($transform[$key]) && is_array($transform[$key])) {
                return array_replace($transform, $transform[$key]);
            }
        }

        return $transform;
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyIfEqRule(mixed $current, array $transform): mixed
    {
        if (! array_key_exists('value', $transform)) {
            throw new RuntimeException('Invalid transform rule "if_eq": missing "value".');
        }
        if (! array_key_exists('then', $transform)) {
            throw new RuntimeException('Invalid transform rule "if_eq": missing "then".');
        }

        return $current == $transform['value'] ? $transform['then'] : $current;
    }

    /**
     * @param  array<string, mixed>  $transform
     */
    private function applyMultiplyRule(mixed $current, array $transform): mixed
    {
        if ($current === null) {
            return null;
        }
        if (! array_key_exists('by', $transform)) {
            throw new RuntimeException('Invalid transform rule "multiply": missing "by".');
        }

        return (float) $current * (float) $transform['by'];
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyRoundPrecisionRule(mixed $current, array $transform, array $sourceRow): mixed
    {
        if ($current === null) {
            return null;
        }
        $precision = (int) ($transform['precision'] ?? 0);
        if (isset($transform['when']) && is_array($transform['when']) && $sourceRow !== []) {
            $whenCol = (string) ($transform['when']['column'] ?? '');
            $whenIn = $transform['when']['in'] ?? null;
            if ($whenCol !== '' && is_array($whenIn)) {
                $cell = $sourceRow[$whenCol] ?? null;
                if (in_array($cell, $whenIn, true)) {
                    $precision = (int) ($transform['then_precision'] ?? $precision);
                }
            }
        }

        return round((float) $current, $precision);
    }

    /**
     * @param  array<string, mixed>  $transform
     * @param  array<string, mixed>  $sourceRow
     */
    private function applyInvokeRule(
        mixed $current,
        array $transform,
        array $sourceRow,
        ?string $sourceColumn,
        ?string $targetColumn,
        ?string $targetTable,
    ): mixed {
        $using = $transform['using'] ?? null;
        if (! is_array($using) || count($using) !== 2) {
            throw new RuntimeException('Invalid transform rule "invoke": "using" must be a [class, method] array.');
        }
        $callable = $using;
        if (! is_callable($callable)) {
            throw new RuntimeException('Invalid transform rule "invoke": "using" is not callable.');
        }

        return $callable($current, $sourceRow, $sourceColumn, $targetColumn, $targetTable);
    }

    /**
     * @param  array<string, mixed>  $sourceRow
     */
    private function resolveValueExpression(mixed $expression, mixed $current, array $sourceRow): mixed
    {
        if ($expression === '$value') {
            return $current;
        }

        if (is_array($expression)) {
            if (array_key_exists('column', $expression)) {
                return $sourceRow[(string) $expression['column']] ?? null;
            }
            if (array_key_exists('value', $expression)) {
                return $expression['value'];
            }
        }

        return $expression;
    }

    private function applyZeroDateToNullTransform(mixed $current): mixed
    {
        if (is_string($current) && preg_match('/^0{4}-0{2}-0{2}/', $current) === 1) {
            return null;
        }

        if ($current instanceof DateTimeInterface) {
            $year = (int) $current->format('Y');
            if ($year <= 0) {
                return null;
            }
        }

        return $current;
    }

    private function normalizeValue(mixed $value): mixed
    {
        if (is_string($value) && preg_match('/^0{4}-0{2}-0{2}/', $value) === 1) {
            return null;
        }

        return $value;
    }
}
