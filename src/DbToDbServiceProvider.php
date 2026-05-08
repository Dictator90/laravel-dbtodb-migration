<?php

namespace MB\DbToDb;

use Illuminate\Support\ServiceProvider;
use MB\DbToDb\Console\DbToDbCommand;
use MB\DbToDb\Support\Database\DbToDbMappingConfigResolver;
use MB\DbToDb\Support\Database\DbToDbMappingValidator;
use MB\DbToDb\Support\Database\DbToDbReportWriter;
use MB\DbToDb\Support\Database\DbToDbRoutingExecutor;
use MB\DbToDb\Support\Database\DbToDbSourceReader;
use MB\DbToDb\Support\Database\DbToDbTargetWriter;
use MB\DbToDb\Support\Database\TargetTableMetadataResolver;

class DbToDbServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/dbtodb_mapping.php',
            'dbtodb_mapping'
        );

        $this->app->singleton(TargetTableMetadataResolver::class);
        $this->app->singleton(DbToDbMappingConfigResolver::class);
        $this->app->singleton(DbToDbReportWriter::class);

        $this->app->singleton(
            DbToDbRoutingExecutor::class,
            fn (): DbToDbRoutingExecutor => new DbToDbRoutingExecutor(
                new DbToDbMappingValidator,
                new DbToDbSourceReader,
                new DbToDbTargetWriter,
                $this->app->make(TargetTableMetadataResolver::class),
            )
        );
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([DbToDbCommand::class]);
        }

        $this->publishes([
            __DIR__.'/../config/dbtodb_mapping.php' => config_path('dbtodb_mapping.php'),
        ], 'dbtodb-migration-config');
    }
}
