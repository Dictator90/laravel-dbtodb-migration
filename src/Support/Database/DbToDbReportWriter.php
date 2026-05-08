<?php

namespace MB\DbToDb\Support\Database;

use Illuminate\Support\Facades\File;
use RuntimeException;

class DbToDbReportWriter
{
    /**
     * @return array{path:string, handle:resource, first:bool}
     */
    public function open(string $path, bool $dryRun, bool $continueOnError): array
    {
        File::ensureDirectoryExists(dirname($path));
        $handle = fopen($path, 'wb');
        if ($handle === false) {
            throw new RuntimeException(sprintf('Cannot open report file "%s" for writing.', $path));
        }

        fwrite($handle, '{"dry_run":'.($dryRun ? 'true' : 'false').',"continue_on_error":'.($continueOnError ? 'true' : 'false').',"pipelines":[');

        return [
            'path' => $path,
            'handle' => $handle,
            'first' => true,
        ];
    }

    /**
     * @param  array{path:string, handle:resource, first:bool}  $stream
     * @param  array<string, mixed>  $report
     */
    public function append(array &$stream, array $report): void
    {
        $json = json_encode($report, JSON_UNESCAPED_UNICODE);
        if (! is_string($json)) {
            throw new RuntimeException('Failed to JSON-encode pipeline report.');
        }

        if (! $stream['first']) {
            fwrite($stream['handle'], ',');
        }
        fwrite($stream['handle'], $json);
        $stream['first'] = false;
    }

    /**
     * @param  array{path:string, handle:resource, first:bool}  $stream
     */
    public function close(array $stream): void
    {
        fwrite($stream['handle'], ']}'.PHP_EOL);
        fclose($stream['handle']);
    }
}
