#!/usr/bin/env bash
set -euo pipefail

application=${1:?Pass the path to a fresh Laravel application.}
test_driver=$(realpath "$(dirname "${BASH_SOURCE[0]}")/laravel-fresh-config-cache.php")
role_launcher=$(realpath "$(dirname "${BASH_SOURCE[0]}")/laravel-fresh-role-launch.php")
client_token=fresh-cache-client-secret
worker_token=fresh-cache-worker-secret
credential_names=(
  DURABLE_WORKFLOW_TOKEN
  DURABLE_WORKFLOW_CLIENT_TOKEN
  DURABLE_WORKFLOW_WORKER_TOKEN
  DURABLE_WORKFLOW_PROCESS_ROLE
  DURABLE_WORKFLOW_PROCESS_TOKEN
)
isolated_environment=(env)
for name in "${credential_names[@]}"; do
  isolated_environment+=(-u "$name")
done

cd "$application"
mkdir -p app/Console/Commands
tee app/Console/Commands/DurableWorkflowApplicationCommand.php >/dev/null <<'PHP'
<?php

declare(strict_types=1);

namespace App\Console\Commands;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use Illuminate\Console\Command;

final class DurableWorkflowApplicationCommand extends Command
{
    protected $signature = 'durable-workflow:application-client-probe';

    protected $description = 'Probe constructor injection for the Durable Workflow application client';

    public function __construct(private readonly LaravelWorkflowClientInterface $workflows)
    {
        parent::__construct();
    }

    public function handle(): int
    {
        $this->components->info($this->workflows::class);

        return self::SUCCESS;
    }
}
PHP
tee app/Providers/DurableWorkflowRoleProbeProvider.php >/dev/null <<'PHP'
<?php

declare(strict_types=1);

namespace App\Providers;

use App\Console\Commands\DurableWorkflowApplicationCommand;
use Illuminate\Support\ServiceProvider;

final class DurableWorkflowRoleProbeProvider extends ServiceProvider
{
    public function boot(): void
    {
        $this->commands([DurableWorkflowApplicationCommand::class]);

        if (class_exists(\LaravelFreshRoleProbe::class, false)) {
            \LaravelFreshRoleProbe::record('after-bootstrap');
        }
    }
}
PHP
sed -i "/return \[/a\    App\\\\Providers\\\\DurableWorkflowRoleProbeProvider::class," bootstrap/providers.php
tee -a routes/console.php >/dev/null <<'PHP'

Artisan::command('durable-workflow:role-client', function (): void {
    $client = app(\DurableWorkflow\WorkflowClientInterface::class);
    if (!$client instanceof \DurableWorkflow\Client) {
        throw new \RuntimeException('Fresh Laravel did not resolve its application client.');
    }
    try {
        app(\DurableWorkflow\Bridge\Laravel\WorkerFactory::class);
    } catch (\InvalidArgumentException $exception) {
        if (str_contains($exception->getMessage(), 'worker credential is required')) {
            $this->info('Fresh Laravel application role isolation passed.');

            return;
        }
        throw $exception;
    }
    throw new \RuntimeException('Fresh Laravel application accepted its client token for a worker.');
});
PHP

"${isolated_environment[@]}" php artisan vendor:publish \
  --tag=durable-workflow-config --force
"${isolated_environment[@]}" php artisan config:cache
"${isolated_environment[@]}" php "$test_driver" "$application" --assert-cache

unset "${credential_names[@]}"
role_probe_log="$application/storage/logs/durable-workflow-role-presence.jsonl"
: > "$role_probe_log"

"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_PROCESS_ROLE=worker \
  php "$test_driver" "$application" --assert-invalid-handoff 'Configure both'
"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_PROCESS_ROLE=invalid \
  DURABLE_WORKFLOW_PROCESS_TOKEN="$worker_token" \
  php "$test_driver" "$application" --assert-invalid-handoff 'must be client or worker'
"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_WORKER_TOKEN="$worker_token" \
  DURABLE_WORKFLOW_PROCESS_ROLE=worker \
  DURABLE_WORKFLOW_PROCESS_TOKEN="$worker_token" \
  php "$test_driver" "$application" --assert-invalid-handoff 'cannot be combined'

set +e
worker_output=$(
  "${isolated_environment[@]}" \
    DURABLE_WORKFLOW_PROCESS_ROLE=worker \
    DURABLE_WORKFLOW_PROCESS_TOKEN="$worker_token" \
    DURABLE_WORKFLOW_ROLE_PROBE_LOG="$role_probe_log" \
    php "$role_launcher" "$application" durable-workflow:worker 2>&1
)
worker_status=$?
set -e
if [ "$worker_status" -eq 0 ] \
  || [[ "$worker_output" != *'No Durable Workflow handlers are configured.'* ]] \
  || [[ "$worker_output" == *'worker credential is required'* ]]; then
  printf '%s\n' "$worker_output" >&2
  echo 'Fresh Laravel worker did not reach handler validation with its isolated credential.' >&2
  exit 1
fi

"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_PROCESS_ROLE=worker \
  DURABLE_WORKFLOW_PROCESS_TOKEN="$worker_token" \
  php "$test_driver" "$application" --child worker
"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_PROCESS_ROLE=client \
  DURABLE_WORKFLOW_PROCESS_TOKEN="$client_token" \
  DURABLE_WORKFLOW_ROLE_PROBE_LOG="$role_probe_log" \
  php "$role_launcher" "$application" durable-workflow:role-client
"${isolated_environment[@]}" \
  DURABLE_WORKFLOW_PROCESS_ROLE=client \
  DURABLE_WORKFLOW_PROCESS_TOKEN="$client_token" \
  php "$test_driver" "$application" --child client
"${isolated_environment[@]}" \
  php "$test_driver" "$application" --assert-probes "$role_probe_log"

echo 'Fresh Laravel cached role isolation passed.'
