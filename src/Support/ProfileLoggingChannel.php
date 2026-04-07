<?php

namespace MB\DbToDb\Support;

/**
 * Resolves the log channel name for --profile and related timing logs.
 *
 * @internal
 */
final class ProfileLoggingChannel
{
    public static function resolve(): string
    {
        $v = config('dbtodb_mapping.profile_logging', 'db_to_db');

        if (is_string($v)) {
            $t = trim($v);

            return $t !== '' ? $t : 'db_to_db';
        }

        if (is_array($v) && isset($v['channel']) && is_string($v['channel'])) {
            $t = trim($v['channel']);

            return $t !== '' ? $t : 'db_to_db';
        }

        return 'db_to_db';
    }
}
