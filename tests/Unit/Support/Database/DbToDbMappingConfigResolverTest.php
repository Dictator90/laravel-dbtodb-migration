<?php

namespace MB\DbToDb\Tests\Unit\Support\Database;

use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
use MB\DbToDb\Tests\TestCase;

class DbToDbMappingConfigResolverTest extends TestCase
{
    private DbToDbMappingConfigResolver $resolver;

    protected function setUp(): void
    {
        parent::setUp();

        $this->resolver = new DbToDbMappingConfigResolver();
    }

    public function test_short_inline_format_is_normalized_as_columns(): void
    {
        $normalized = $this->resolver->normalizeMigrationTableDefinition([
            'catalog_banners' => [
                'link' => 'link',
            ],
        ], 'top_banners');

        $this->assertSame([], $normalized['source']['filters']);
        $this->assertSame([], $normalized['source']['runtime']);
        $this->assertSame(['link' => 'link'], $normalized['targets']['catalog_banners']['columns']);
    }

    public function test_full_inline_target_definition_is_preserved(): void
    {
        $normalized = $this->resolver->normalizeMigrationTableDefinition([
            'catalog_banners' => [
                'columns' => [
                    'link' => 'link',
                ],
                'filters' => [
                    ['column' => 'is_active', 'operator' => '=', 'value' => 1],
                ],
                'transforms' => [
                    'link' => ['trim'],
                ],
                'upsert_keys' => ['id'],
                'operation' => 'truncate_insert',
            ],
        ], 'top_banners');

        $target = $normalized['targets']['catalog_banners'];
        $this->assertSame(['link' => 'link'], $target['columns']);
        $this->assertSame([
            ['column' => 'is_active', 'operator' => '=', 'value' => 1],
        ], $target['filters']);
        $this->assertSame(['link' => ['trim']], $target['transforms']);
        $this->assertSame(['id'], $target['upsert_keys']);
        $this->assertSame('truncate_insert', $target['operation']);
    }

    public function test_multiple_targets_for_one_source_are_normalized_in_order(): void
    {
        $normalized = $this->resolver->normalizeMigrationTableDefinition([
            'catalog_banners' => [
                'columns' => [
                    'link' => 'link',
                ],
                'upsert_keys' => ['id'],
            ],
            'catalog_banners_2' => [
                'name' => 'name',
            ],
        ], 'top_banners');

        $this->assertSame(['catalog_banners', 'catalog_banners_2'], array_keys($normalized['targets']));
        $this->assertSame(['link' => 'link'], $normalized['targets']['catalog_banners']['columns']);
        $this->assertSame(['id'], $normalized['targets']['catalog_banners']['upsert_keys']);
        $this->assertSame(['name' => 'name'], $normalized['targets']['catalog_banners_2']['columns']);
    }
}
