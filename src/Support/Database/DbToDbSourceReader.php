<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;
use RuntimeException;

class DbToDbSourceReader
{
    public function __construct(
        private ?DbToDbFilterEngine $filterEngine = null,
    ) {
        $this->filterEngine ??= new DbToDbFilterEngine;
    }

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
        if ($filters instanceof \Closure || is_callable($filters)) {
            $result = $filters($query);
            if ($result instanceof Builder) {
                $query = $result;
            }

            return $query;
        }

        if (! is_array($filters)) {
            throw new RuntimeException('Source filters must be an array or callable.');
        }

        $this->filterEngine->applyToQuery($query, $filters);

        return $query;
    }

}
