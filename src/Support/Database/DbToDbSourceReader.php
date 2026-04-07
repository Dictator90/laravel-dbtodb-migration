<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;
use RuntimeException;

class DbToDbSourceReader
{
    /**
     * @param  array<string, mixed>  $source
     */
    public function buildQuery(array $source): Builder
    {
        $table = (string) $source['table'];
        $query = DB::connection((string) $source['connection'])->table($table);

        $select = $source['select'] ?? [];
        if (is_array($select) && $select !== []) {
            $columns = [];
            foreach ($select as $column) {
                if (! is_string($column)) {
                    continue;
                }
                $trimmed = trim($column);
                if ($trimmed === '') {
                    continue;
                }
                $columns[$trimmed] = sprintf('%s.%s', $table, $trimmed);
            }
            if ($columns !== []) {
                $query->select(array_values($columns));
            }
        }

        $filters = $source['filters'] ?? [];
        if (! is_array($filters)) {
            throw new RuntimeException('Source filters must be an array.');
        }

        foreach ($this->normalizeFilters($filters) as $filter) {
            $this->applyFilter($query, $filter);
        }

        return $query;
    }

    /**
     * @param  list<array<string, mixed>>  $filters
     * @return list<array<string, mixed>>
     */
    private function normalizeFilters(array $filters): array
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
     * @param  array<string, mixed>  $filter
     */
    private function applyFilter(Builder $query, array $filter): void
    {
        $column = (string) ($filter['column'] ?? '');
        $operator = strtolower((string) ($filter['operator'] ?? '='));
        $value = $filter['value'] ?? null;
        if (in_array($operator, ['in', 'not_in'], true) && $value === null && array_key_exists('values', $filter)) {
            $value = $filter['values'];
        }

        if ($column === '') {
            throw new RuntimeException('Filter column is required.');
        }

        switch ($operator) {
            case '=':
            case '!=':
            case '>':
            case '>=':
            case '<':
            case '<=':
            case 'like':
            case 'not_like':
                $query->where($column, $operator === 'not_like' ? 'not like' : $operator, $value);

                return;
            case 'in':
            case 'not_in':
                if (! is_array($value)) {
                    throw new RuntimeException(sprintf(
                        'Filter "%s" expects array for operator "%s" (use key "value" or "values").',
                        $column,
                        $operator
                    ));
                }
                if ($operator === 'in') {
                    $query->whereIn($column, $value);
                } else {
                    $query->whereNotIn($column, $value);
                }

                return;
            case 'null':
                $query->whereNull($column);

                return;
            case 'not_null':
                $query->whereNotNull($column);

                return;
            case 'between':
            case 'not_between':
                if (! is_array($value) || count($value) !== 2) {
                    throw new RuntimeException(sprintf('Filter "%s" expects [from, to] for operator "%s".', $column, $operator));
                }
                if ($operator === 'between') {
                    $query->whereBetween($column, array_values($value));
                } else {
                    $query->whereNotBetween($column, array_values($value));
                }

                return;
            case 'exists_in':
                $this->applyExistsInFilter($query, $column, $value);

                return;
            default:
                throw new RuntimeException(sprintf('Unsupported filter operator "%s" for column "%s".', $operator, $column));
        }
    }

    /**
     * Semi-join: keep source rows whose {column} matches ref_table.ref_column for some ref row
     * satisfying nested filters (implemented as INNER JOIN (SELECT DISTINCT ref_col …) alias ON …).
     * Optional nested filters on ref_table (same rule shape as top-level filters).
     * Optional "in": restrict ref table rows so ref_table.{in_column} is in the given list (default in_column = ref column).
     *
     * @param  array<string, mixed>  $value
     */
    private function applyExistsInFilter(Builder $query, string $column, mixed $value): void
    {
        if (! is_array($value)) {
            throw new RuntimeException(sprintf('Filter "%s" with operator "exists_in" expects an array value.', $column));
        }

        $refTable = (string) ($value['table'] ?? '');
        if ($refTable === '') {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.table is required.', $column));
        }

        $refColumn = (string) ($value['column'] ?? $column);
        $mainTable = $this->queryFromTable($query);

        $nested = $value['where'] ?? [];
        if ($nested !== [] && ! is_array($nested)) {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.where must be an array when present.', $column));
        }

        $inList = $value['in'] ?? null;
        if ($inList !== null && ! is_array($inList)) {
            throw new RuntimeException(sprintf('Filter "%s" exists_in: value.in must be an array when present.', $column));
        }

        $inColumn = (string) ($value['in_column'] ?? $refColumn);

        // Semi-join via INNER JOIN (derived table). Often faster on MySQL than WHERE col IN (SELECT …)
        // when the inner select is large (e.g. all idw with go=1 on watches): avoids bad semi-join
        // plans and duplicate-column ambiguity from SELECT * with a plain join to ref.
        if ($query->columns === null) {
            $query->select(sprintf('%s.*', $mainTable));
        }

        $sub = $query->getConnection()->table($refTable)->select($refColumn)->distinct();

        if (is_array($inList) && $inList !== []) {
            if (trim($inColumn) === '') {
                throw new RuntimeException(sprintf('Filter "%s" exists_in: value.in_column must be non-empty when value.in is non-empty.', $column));
            }
            $sub->whereIn($inColumn, array_values($inList));
        }

        foreach ($this->normalizeFilters(is_array($nested) ? $nested : []) as $nestedFilter) {
            $this->applyFilter($sub, $nestedFilter);
        }

        $alias = 'dbtodb_ei_'.substr(hash('sha256', $mainTable.'|'.$column.'|'.$refTable.'|'.$refColumn), 0, 12);

        $query->joinSub($sub, $alias, function ($join) use ($alias, $refColumn, $mainTable, $column): void {
            $join->on(
                sprintf('%s.%s', $alias, $refColumn),
                '=',
                sprintf('%s.%s', $mainTable, $column)
            );
        });
    }

    private function queryFromTable(Builder $query): string
    {
        $from = $query->from;
        if (is_string($from) && $from !== '') {
            return $from;
        }

        throw new RuntimeException('exists_in filter requires a named source table (from).');
    }
}
