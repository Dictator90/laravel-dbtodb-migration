<?php

namespace MB\DbToDb\Support\Database;

use DateTimeImmutable;
use Illuminate\Database\Query\Builder;
use RuntimeException;

class DbToDbFilterEngine
{
    /**
     * @var list<string>
     */
    private const BASIC_OPERATORS = ['=', '!=', '>', '>=', '<', '<=', 'like', 'not_like'];

    /**
     * @var list<string>
     */
    private const VALUE_OPERATORS = ['in', 'not_in', 'between', 'not_between'];

    /**
     * @var list<string>
     */
    private const NULL_OPERATORS = ['null', 'not_null'];

    /**
     * @var list<string>
     */
    private const SPECIAL_OPERATORS = ['exists_in', 'where_column', 'date', 'year', 'month'];

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<array<string, mixed>>
     */
    public function normalizeFilters(array $filters): array
    {
        if ($filters === []) {
            return [];
        }

        if ($this->isGroupDefinition($filters)) {
            return [$this->normalizeGroup($filters)];
        }

        if (array_is_list($filters)) {
            return array_map(fn (mixed $filter): array => $this->normalizeFilterNode($filter), $filters);
        }

        $normalized = [];
        foreach ($filters as $column => $value) {
            if (($column === 'and' || $column === 'or') && is_array($value)) {
                $normalized[] = $this->normalizeGroup([$column => $value]);
                continue;
            }

            $normalized[] = $this->normalizeRule([
                'column' => (string) $column,
                'operator' => '=',
                'value' => $value,
            ]);
        }

        return $normalized;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     */
    public function applyToQuery(Builder $query, array $filters): void
    {
        foreach ($this->normalizeFilters($filters) as $filter) {
            $this->applyNodeToQuery($query, $filter, 'and');
        }
    }

    /**
     * @param  array<string, mixed>  $row
     * @param  array<int|string, mixed>  $filters
     */
    public function passesRow(array $row, array $filters): bool
    {
        foreach ($this->normalizeFilters($filters) as $filter) {
            if (! $this->matchesNode($row, $filter)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<int|string, mixed>  $filters
     * @return list<string>
     */
    public function collectColumnNames(array $filters): array
    {
        $names = [];
        foreach ($this->normalizeFilters($filters) as $filter) {
            foreach ($this->collectColumnNamesFromNode($filter) as $column) {
                $names[$column] = true;
            }
        }

        return array_keys($names);
    }

    private function normalizeFilterNode(mixed $filter): array
    {
        if (! is_array($filter)) {
            throw new RuntimeException('Filter rule must be an array.');
        }

        if ($this->isGroupDefinition($filter)) {
            return $this->normalizeGroup($filter);
        }

        return $this->normalizeRule($filter);
    }

    /**
     * @param  array<int|string, mixed>  $filter
     */
    private function isGroupDefinition(array $filter): bool
    {
        return (array_key_exists('and', $filter) || array_key_exists('or', $filter)) && ! array_key_exists('column', $filter);
    }

    /**
     * @param  array<int|string, mixed>  $filter
     * @return array{type:string, boolean:string, filters:list<array<string, mixed>>}
     */
    private function normalizeGroup(array $filter): array
    {
        $hasAnd = array_key_exists('and', $filter);
        $hasOr = array_key_exists('or', $filter);
        if ($hasAnd && $hasOr) {
            throw new RuntimeException('Filter group cannot contain both "and" and "or" keys.');
        }

        $boolean = $hasOr ? 'or' : 'and';
        $children = $filter[$boolean];
        if (! is_array($children)) {
            throw new RuntimeException(sprintf('Filter group "%s" must contain an array.', $boolean));
        }

        return [
            'type' => 'group',
            'boolean' => $boolean,
            'filters' => $this->normalizeFilters($children),
        ];
    }

    /**
     * @param  array<string, mixed>  $filter
     * @return array<string, mixed>
     */
    private function normalizeRule(array $filter): array
    {
        $column = trim((string) ($filter['column'] ?? ''));
        if ($column === '') {
            throw new RuntimeException('Filter column is required.');
        }

        [$operator, $value, $comparison] = $this->resolveOperatorValueAndComparison($filter);
        $this->validateOperator($operator, $column);

        $rule = [
            'type' => 'rule',
            'column' => $column,
            'operator' => $operator,
            'value' => $value,
        ];

        if ($comparison !== null) {
            $rule['comparison'] = $comparison;
        }

        return $rule;
    }

    /**
     * @param  array<string, mixed>  $filter
     * @return array{string,mixed,string|null}
     */
    private function resolveOperatorValueAndComparison(array $filter): array
    {
        foreach (array_merge(self::VALUE_OPERATORS, self::NULL_OPERATORS, ['like', 'not_like', 'exists_in', 'date', 'year', 'month']) as $key) {
            if (array_key_exists($key, $filter)) {
                $comparison = in_array($key, ['date', 'year', 'month'], true)
                    ? $this->normalizeComparison((string) ($filter['operator'] ?? '='), $key)
                    : null;

                return [$key, $filter[$key], $comparison];
            }
        }

        if (array_key_exists('where_column', $filter)) {
            return [
                'where_column',
                $filter['where_column'],
                $this->normalizeComparison((string) ($filter['operator'] ?? '='), 'where_column'),
            ];
        }

        $operator = strtolower(trim((string) ($filter['operator'] ?? '=')));
        $value = $filter['value'] ?? null;
        if (in_array($operator, ['in', 'not_in'], true) && $value === null && array_key_exists('values', $filter)) {
            $value = $filter['values'];
        }

        $comparison = null;
        if (in_array($operator, ['where_column', 'date', 'year', 'month'], true)) {
            $comparison = $this->normalizeComparison((string) ($filter['comparison'] ?? '='), $operator);
        }

        return [$operator, $value, $comparison];
    }

    private function normalizeComparison(string $operator, string $context): string
    {
        $operator = strtolower(trim($operator));
        if (! in_array($operator, ['=', '!=', '>', '>=', '<', '<='], true)) {
            throw new RuntimeException(sprintf('Filter "%s" expects comparison operator =, !=, >, >=, <, or <=.', $context));
        }

        return $operator;
    }

    private function validateOperator(string $operator, string $column): void
    {
        if (! in_array($operator, array_merge(self::BASIC_OPERATORS, self::VALUE_OPERATORS, self::NULL_OPERATORS, self::SPECIAL_OPERATORS), true)) {
            throw new RuntimeException(sprintf('Unsupported filter operator "%s" for column "%s".', $operator, $column));
        }
    }

    /**
     * @param  array<string, mixed>  $filter
     */
    private function applyNodeToQuery(Builder $query, array $filter, string $boolean): void
    {
        if (($filter['type'] ?? 'rule') === 'group') {
            $method = $boolean === 'or' ? 'orWhere' : 'where';
            $query->{$method}(function (Builder $nested) use ($filter): void {
                foreach ((array) $filter['filters'] as $child) {
                    $this->applyNodeToQuery($nested, $child, (string) $filter['boolean']);
                }
            });

            return;
        }

        $this->applyRuleToQuery($query, $filter, $boolean);
    }

    /**
     * @param  array<string, mixed>  $filter
     */
    private function applyRuleToQuery(Builder $query, array $filter, string $boolean): void
    {
        $column = (string) $filter['column'];
        $operator = (string) $filter['operator'];
        $value = $filter['value'] ?? null;
        $prefix = $boolean === 'or' ? 'or' : '';

        switch ($operator) {
            case '=':
            case '!=':
            case '>':
            case '>=':
            case '<':
            case '<=':
            case 'like':
            case 'not_like':
                $query->{$prefix.'Where'}($column, $operator === 'not_like' ? 'not like' : $operator, $value);

                return;
            case 'in':
            case 'not_in':
                if (! is_array($value)) {
                    throw new RuntimeException(sprintf('Filter "%s" expects array for operator "%s" (use key "value" or "values").', $column, $operator));
                }
                $query->{$prefix.($operator === 'in' ? 'WhereIn' : 'WhereNotIn')}($column, $value);

                return;
            case 'null':
            case 'not_null':
                $query->{$prefix.($operator === 'null' ? 'WhereNull' : 'WhereNotNull')}($column);

                return;
            case 'between':
            case 'not_between':
                if (! is_array($value) || count($value) !== 2) {
                    throw new RuntimeException(sprintf('Filter "%s" expects [from, to] for operator "%s".', $column, $operator));
                }
                $query->{$prefix.($operator === 'between' ? 'WhereBetween' : 'WhereNotBetween')}($column, array_values($value));

                return;
            case 'exists_in':
                $this->applyExistsInFilter($query, $column, $value, $boolean);

                return;
            case 'where_column':
                $rightColumn = trim((string) $value);
                if ($rightColumn === '') {
                    throw new RuntimeException(sprintf('Filter "%s" where_column expects a non-empty target column.', $column));
                }
                $query->{$prefix.'WhereColumn'}($column, (string) $filter['comparison'], $rightColumn);

                return;
            case 'date':
            case 'year':
            case 'month':
                $method = $operator === 'date' ? 'WhereDate' : ($operator === 'year' ? 'WhereYear' : 'WhereMonth');
                $query->{$prefix.$method}($column, (string) $filter['comparison'], $value);

                return;
        }
    }

    private function applyExistsInFilter(Builder $query, string $column, mixed $value, string $boolean): void
    {
        if (! is_array($value)) {
            throw new RuntimeException(sprintf('Filter "%s" with operator "exists_in" expects an array value.', $column));
        }

        $refTable = (string) ($value['table'] ?? '');
        if ($refTable === '') {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.table is required.', $column));
        }

        $refColumn = (string) ($value['column'] ?? $column);
        $nested = $value['where'] ?? [];
        if ($nested !== [] && ! is_array($nested)) {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.where must be an array when present.', $column));
        }

        $inList = $value['in'] ?? null;
        if ($inList !== null && ! is_array($inList)) {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.in must be an array when present.', $column));
        }

        $inColumn = (string) ($value['in_column'] ?? $refColumn);
        $sub = $query->getConnection()->table($refTable)->select($refColumn)->distinct();

        if (is_array($inList) && $inList !== []) {
            if (trim($inColumn) === '') {
                throw new RuntimeException(sprintf('Filter "%s" exists_in: value.in_column must be non-empty when value.in is non-empty.', $column));
            }
            $sub->whereIn($inColumn, array_values($inList));
        }

        $this->applyToQuery($sub, is_array($nested) ? $nested : []);
        $query->{$boolean === 'or' ? 'orWhereIn' : 'whereIn'}($column, $sub);
    }

    /**
     * @param  array<string, mixed>  $row
     * @param  array<string, mixed>  $filter
     */
    private function matchesNode(array $row, array $filter): bool
    {
        if (($filter['type'] ?? 'rule') !== 'group') {
            return $this->matchesRule($row, $filter);
        }

        $children = (array) $filter['filters'];
        if ((string) $filter['boolean'] === 'or') {
            foreach ($children as $child) {
                if ($this->matchesNode($row, $child)) {
                    return true;
                }
            }

            return false;
        }

        foreach ($children as $child) {
            if (! $this->matchesNode($row, $child)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<string, mixed>  $row
     * @param  array<string, mixed>  $filter
     */
    private function matchesRule(array $row, array $filter): bool
    {
        $column = (string) $filter['column'];
        $operator = (string) $filter['operator'];
        $value = $filter['value'] ?? null;
        $actual = array_key_exists($column, $row) ? $row[$column] : null;

        if ($operator === 'exists_in') {
            throw new RuntimeException('Target filter operator "exists_in" is not supported. Use exists_in in source filters (config filters on source table) only.');
        }

        if ($operator === 'where_column') {
            $rightColumn = trim((string) $value);
            if ($rightColumn === '') {
                throw new RuntimeException(sprintf('Target filter "%s" where_column expects a non-empty target column.', $column));
            }

            return $this->compare($actual, $row[$rightColumn] ?? null, (string) $filter['comparison']);
        }

        if (in_array($operator, ['date', 'year', 'month'], true)) {
            $actual = $this->extractDatePart($actual, $operator);
            if ($actual === null) {
                return false;
            }
        }

        return match ($operator) {
            '=' => $actual == $value,
            '!=' => $actual != $value,
            '>' => $actual > $value,
            '>=' => $actual >= $value,
            '<' => $actual < $value,
            '<=' => $actual <= $value,
            'in' => is_array($value) && in_array($actual, $value, true),
            'not_in' => is_array($value) && ! in_array($actual, $value, true),
            'like' => is_string($actual) && $this->matchLike($actual, (string) $value),
            'not_like' => is_string($actual) && ! $this->matchLike($actual, (string) $value),
            'null' => $actual === null,
            'not_null' => $actual !== null,
            'between' => is_array($value) && count($value) === 2 && $actual >= array_values($value)[0] && $actual <= array_values($value)[1],
            'not_between' => is_array($value) && count($value) === 2 && ($actual < array_values($value)[0] || $actual > array_values($value)[1]),
            'date', 'year', 'month' => $this->compare($actual, $value, (string) $filter['comparison']),
            default => throw new RuntimeException(sprintf('Unsupported target filter operator "%s".', $operator)),
        };
    }

    private function compare(mixed $left, mixed $right, string $operator): bool
    {
        return match ($operator) {
            '=' => $left == $right,
            '!=' => $left != $right,
            '>' => $left > $right,
            '>=' => $left >= $right,
            '<' => $left < $right,
            '<=' => $left <= $right,
            default => throw new RuntimeException(sprintf('Unsupported comparison operator "%s".', $operator)),
        };
    }

    private function matchLike(string $actual, string $pattern): bool
    {
        $regex = '/^'.str_replace(['%', '_'], ['.*', '.'], preg_quote($pattern, '/')).'$/ui';

        return preg_match($regex, $actual) === 1;
    }

    private function extractDatePart(mixed $value, string $part): mixed
    {
        if ($value === null || $value === '') {
            return null;
        }

        try {
            $date = is_numeric($value)
                ? (new DateTimeImmutable)->setTimestamp((int) $value)
                : new DateTimeImmutable((string) $value);
        } catch (\Throwable) {
            return null;
        }

        return match ($part) {
            'date' => $date->format('Y-m-d'),
            'year' => (int) $date->format('Y'),
            'month' => (int) $date->format('n'),
            default => null,
        };
    }

    /**
     * @param  array<string, mixed>  $filter
     * @return list<string>
     */
    private function collectColumnNamesFromNode(array $filter): array
    {
        if (($filter['type'] ?? 'rule') === 'group') {
            $columns = [];
            foreach ((array) $filter['filters'] as $child) {
                $columns = array_merge($columns, $this->collectColumnNamesFromNode($child));
            }

            return $columns;
        }

        $columns = [];
        $column = trim((string) ($filter['column'] ?? ''));
        if ($column !== '' && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column)) {
            $columns[] = $column;
        }

        if (($filter['operator'] ?? '') === 'where_column') {
            $rightColumn = trim((string) ($filter['value'] ?? ''));
            if ($rightColumn !== '' && preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $rightColumn)) {
                $columns[] = $rightColumn;
            }
        }

        return $columns;
    }
}
