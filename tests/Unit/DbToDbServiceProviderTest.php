<?php

namespace MB\DbToDb\Tests\Unit;

use MB\DbToDb\Support\ProfileLoggingChannel;
use MB\DbToDb\Tests\TestCase;

class DbToDbServiceProviderTest extends TestCase
{
    public function test_profile_logging_config_resolves_to_default_channel_name(): void
    {
        $this->assertSame('db_to_db', config('dbtodb_mapping.profile_logging'));
        $this->assertSame('db_to_db', ProfileLoggingChannel::resolve());
    }
}
