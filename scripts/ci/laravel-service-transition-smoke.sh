#!/usr/bin/env bash

set -euo pipefail

source_mode="${SOURCE_MODE:?Set SOURCE_MODE to embedded_v1 or embedded_v2}"
workflow_requirement="${WORKFLOW_REQUIREMENT:?Set WORKFLOW_REQUIREMENT to a maintained durable-workflow/workflow channel}"
laravel_requirement="${LARAVEL_REQUIREMENT:-^12.0}"
sdk_path="${SDK_PATH:?Set SDK_PATH to the PHP SDK checkout}"

if [[ "$source_mode" != embedded_v1 && "$source_mode" != embedded_v2 ]]; then
  echo 'SOURCE_MODE must be embedded_v1 or embedded_v2.' >&2
  exit 1
fi

temporary_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
if [ -d /dev/shm ] && [ -w /dev/shm ]; then
  database_root=/dev/shm
else
  database_root="$temporary_root"
fi

fixture_root=$(mktemp -d "${temporary_root%/}/laravel-service-transition.XXXXXX")
database_path=$(mktemp "${database_root%/}/laravel-service-transition.XXXXXX.sqlite")
application="$fixture_root/application"

cleanup() {
  rm -f -- "$database_path"
  rm -rf -- "$fixture_root"
}
trap cleanup EXIT
trap 'echo "Laravel service transition smoke failed at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

composer create-project "laravel/laravel:${laravel_requirement}" "$application" \
  --no-interaction --no-progress --prefer-dist --no-blocking --quiet
cd "$application"

set_environment() {
  php -r '
    $path = ".env";
    $key = $argv[1];
    $value = $argv[2];
    $contents = file_get_contents($path);
    if (!is_string($contents)) {
        throw new RuntimeException("Unable to read the Laravel environment file.");
    }
    $line = $key."=".$value;
    $pattern = "/^".preg_quote($key, "/")."=.*$/m";
    $contents = preg_match($pattern, $contents) === 1
        ? preg_replace($pattern, $line, $contents)
        : rtrim($contents).PHP_EOL.$line.PHP_EOL;
    file_put_contents($path, $contents);
  ' "$1" "$2"
}

set_environment APP_NAME '"Laravel Adoption Host"'
set_environment APP_ENV testing
set_environment DB_CONNECTION sqlite
set_environment DB_DATABASE "$database_path"
set_environment QUEUE_CONNECTION database
set_environment CACHE_STORE file
set_environment DW_V2_QUEUE embedded-v2
php artisan key:generate --force --no-interaction >/dev/null

composer require "durable-workflow/workflow:${workflow_requirement}" \
  --with-all-dependencies --no-interaction --no-progress --no-blocking --quiet
source_version_before=$(composer show durable-workflow/workflow --format=json | php -r '
  $package = json_decode(stream_get_contents(STDIN), true, 512, JSON_THROW_ON_ERROR);
  echo ltrim((string) $package["versions"][0], "v");
')

if ! grep -Rqs "Schema::create('jobs'" database/migrations; then
  php artisan queue:table --no-interaction >/dev/null
fi
php artisan vendor:publish \
  --provider='Workflow\Providers\WorkflowServiceProvider' \
  --tag=migrations --force --no-interaction >/dev/null
php artisan migrate --force --no-interaction >/dev/null

mkdir -p app/Embedded app/Service app/Support
cat > app/Support/GreetingPrefix.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Support;

final class GreetingPrefix
{
    public function greet(string $name): string
    {
        return "hello from Laravel, {$name}";
    }
}
PHP

if [ "$source_mode" = embedded_v1 ]; then
  cat > app/Embedded/LaravelGreetingActivity.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Embedded;

use App\Support\GreetingPrefix;
use Workflow\Activity;

final class LaravelGreetingActivity extends Activity
{
    public $connection = 'database';
    public $queue = 'embedded-v1';

    public function execute(GreetingPrefix $prefix, string $name): string
    {
        return $prefix->greet($name);
    }
}
PHP
  cat > app/Embedded/LaravelGreetingWorkflow.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Embedded;

use Workflow\Workflow;
use function Workflow\activity;

final class LaravelGreetingWorkflow extends Workflow
{
    public $connection = 'database';
    public $queue = 'embedded-v1';

    public function execute(string $name)
    {
        return yield activity(LaravelGreetingActivity::class, $name);
    }
}
PHP
  cat > app/Embedded/OpenLaravelGreetingWorkflow.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Embedded;

use Workflow\SignalMethod;
use Workflow\Workflow;
use function Workflow\{activity, await};

final class OpenLaravelGreetingWorkflow extends Workflow
{
    public $connection = 'database';
    public $queue = 'embedded-v1';
    private bool $released = false;

    #[SignalMethod]
    public function proceed(): void
    {
        $this->released = true;
    }

    public function execute(string $name)
    {
        yield await(fn (): bool => $this->released);

        return yield activity(LaravelGreetingActivity::class, $name);
    }
}
PHP
  cat > routes/console.php <<'PHP'
<?php

use App\Embedded\LaravelGreetingWorkflow;
use App\Embedded\OpenLaravelGreetingWorkflow;
use Illuminate\Support\Facades\Artisan;
use Workflow\Models\StoredWorkflow;
use Workflow\WorkflowStub;

Artisan::command('transition:embedded-start {id} {--open}', function (): void {
    $class = $this->option('open') ? OpenLaravelGreetingWorkflow::class : LaravelGreetingWorkflow::class;
    $workflow = WorkflowStub::make($class);
    $workflow->start('Ada');
    file_put_contents(storage_path('app/'.$this->argument('id')), $workflow->id());
});

Artisan::command('transition:embedded-inspect {id}', function (): void {
    $workflowId = trim((string) file_get_contents(storage_path('app/'.$this->argument('id'))));
    $workflow = WorkflowStub::load($workflowId);
    $stored = StoredWorkflow::query()->findOrFail($workflowId);
    $this->line(json_encode([
        'status' => $stored->getRawOriginal('status'),
        'completed' => $workflow->completed(),
        'output' => $workflow->output(),
    ], JSON_THROW_ON_ERROR));
});
PHP
  source_tables='workflows,workflow_logs,workflow_signals,workflow_timers,workflow_exceptions,workflow_relationships'
  source_queue=embedded-v1
else
  cat > app/Embedded/LaravelGreetingActivity.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Embedded;

use App\Support\GreetingPrefix;
use Workflow\V2\Activity;
use Workflow\V2\Attributes\Type;

#[Type('laravel.greet')]
final class LaravelGreetingActivity extends Activity
{
    public function __construct(private readonly GreetingPrefix $prefix)
    {
    }

    public function handle(string $name): string
    {
        return $this->prefix->greet($name);
    }
}
PHP
  cat > app/Embedded/LaravelGreetingWorkflow.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Embedded;

use App\Support\GreetingPrefix;
use Workflow\V2\Attributes\Signal;
use Workflow\V2\Attributes\Type;
use Workflow\V2\Workflow;
use function Workflow\V2\{activity, signal};

#[Type('laravel.greeting')]
#[Signal('release', [['name' => 'confirmation', 'type' => 'string']])]
final class LaravelGreetingWorkflow extends Workflow
{
    public function __construct(private readonly GreetingPrefix $prefix)
    {
    }

    public function handle(string $name): string
    {
        if ($this->prefix->greet($name) === '') {
            throw new \LogicException('Laravel did not inject the embedded workflow dependency.');
        }
        signal('release');

        return activity(LaravelGreetingActivity::class, $name);
    }
}
PHP
  cat > routes/console.php <<'PHP'
<?php

use App\Embedded\LaravelGreetingWorkflow;
use Illuminate\Support\Facades\Artisan;
use Workflow\V2\StartOptions;
use Workflow\V2\WorkflowStub;

Artisan::command('transition:embedded-start {id}', function (): void {
    WorkflowStub::make(LaravelGreetingWorkflow::class, $this->argument('id'))->start(
        'Ada',
        new StartOptions(memo: ['runtime_owner' => 'embedded']),
    );
});

Artisan::command('transition:embedded-release {id}', function (): void {
    WorkflowStub::load($this->argument('id'))->signal('release', 'continue');
});

Artisan::command('transition:embedded-inspect {id}', function (): void {
    $workflow = WorkflowStub::load($this->argument('id'))->refresh();
    $this->line(json_encode([
        'status' => $workflow->status(),
        'completed' => $workflow->status() === 'completed',
        'output' => $workflow->output(),
    ], JSON_THROW_ON_ERROR));
});
PHP
  source_tables='workflow_instances,workflow_runs,workflow_history_events,workflow_tasks,workflow_commands,workflow_updates,workflow_signal_records,workflow_memos,workflow_search_attributes'
  source_queue=embedded-v2
fi

composer dump-autoload --no-interaction --no-scripts --quiet

queue_depth() {
  QUEUE_NAME="$1" DATABASE_PATH="$database_path" php -r '
    $database = new PDO("sqlite:".getenv("DATABASE_PATH"));
    $statement = $database->prepare("SELECT COUNT(*) FROM jobs WHERE queue = :queue");
    $statement->execute(["queue" => getenv("QUEUE_NAME")]);
    echo (int) $statement->fetchColumn();
  '
}

drain_queue() {
  local queue="$1"
  local remaining=0
  for _pass in $(seq 1 20); do
    timeout 120 php artisan queue:work database --queue="$queue" \
      --stop-when-empty --sleep=1 --tries=3 --no-interaction >/dev/null
    remaining=$(queue_depth "$queue")
    if [ "$remaining" -eq 0 ]; then
      return 0
    fi
  done
  echo "Queue ${queue} still has ${remaining} job(s)." >&2
  return 1
}

hash_source_tables() {
  TABLES="$source_tables" DATABASE_PATH="$database_path" php -r '
    $database = new PDO("sqlite:".getenv("DATABASE_PATH"));
    $snapshot = [];
    foreach (explode(",", getenv("TABLES")) as $table) {
        $statement = $database->query(sprintf("SELECT * FROM \"%s\" ORDER BY rowid", $table));
        if ($statement === false) {
            throw new RuntimeException("Missing embedded history table {$table}.");
        }
        $snapshot[$table] = $statement->fetchAll(PDO::FETCH_ASSOC);
    }
    echo hash("sha256", json_encode($snapshot, JSON_THROW_ON_ERROR));
  '
}

php artisan transition:embedded-start embedded-completed >/dev/null
drain_queue "$source_queue"
if [ "$source_mode" = embedded_v2 ]; then
  php artisan transition:embedded-release embedded-completed >/dev/null
  drain_queue "$source_queue"
fi
embedded_result=$(php artisan transition:embedded-inspect embedded-completed)
EMBEDDED_RESULT="$embedded_result" php -r '
  $result = json_decode(getenv("EMBEDDED_RESULT"), true, 512, JSON_THROW_ON_ERROR);
  if ($result["completed"] !== true || $result["output"] !== "hello from Laravel, Ada") {
      throw new RuntimeException("The established embedded workflow did not preserve its injected dependency and result.");
  }
'

if [ "$source_mode" = embedded_v1 ]; then
  php artisan transition:embedded-start embedded-open --open >/dev/null
else
  php artisan transition:embedded-start embedded-open >/dev/null
fi
drain_queue "$source_queue"
embedded_open=$(php artisan transition:embedded-inspect embedded-open)
EMBEDDED_RESULT="$embedded_open" php -r '
  $result = json_decode(getenv("EMBEDDED_RESULT"), true, 512, JSON_THROW_ON_ERROR);
  if ($result["completed"] !== false) {
      throw new RuntimeException("The coexistence rehearsal did not retain an open embedded-owned run.");
  }
'
source_state_before=$(hash_source_tables)

sdk_path=$(cd "$sdk_path" && pwd)
composer config repositories.sdk "$(php -r '
  echo json_encode([
      "type" => "path",
      "url" => $argv[1],
      "options" => [
          "symlink" => false,
          "versions" => ["durable-workflow/sdk" => "2.0.x-dev"],
      ],
  ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
' "$sdk_path")"
composer require durable-workflow/sdk:2.0.x-dev \
  --with-all-dependencies --no-interaction --no-progress --no-blocking --quiet

source_version_after=$(composer show durable-workflow/workflow --format=json | php -r '
  $package = json_decode(stream_get_contents(STDIN), true, 512, JSON_THROW_ON_ERROR);
  echo ltrim((string) $package["versions"][0], "v");
')
if [ "$source_version_after" != "$source_version_before" ]; then
  echo 'Installing service mode changed the established embedded package version.' >&2
  exit 1
fi

SOURCE_MODE="$source_mode" php -r '
  $service = json_decode(
      file_get_contents("vendor/durable-workflow/sdk/docs/laravel-adoption-contract.json"),
      true,
      512,
      JSON_THROW_ON_ERROR,
  );
  $reference = $service["embedded_transition_authority"];
  if (getenv("SOURCE_MODE") === "embedded_v1") {
      $source = $service["modes"]["embedded_v1"];
      if ($source["package"] !== "durable-workflow/workflow" || $source["channel"] !== "^1.0") {
          throw new RuntimeException("The service adoption contract did not retain the stable Workflow source identity.");
      }
      exit(0);
  }
  $embedded = json_decode(
      file_get_contents("vendor/durable-workflow/workflow/".$reference["manifest"]),
      true,
      512,
      JSON_THROW_ON_ERROR,
  );
  if ($embedded["schema"] !== $reference["schema"]
      || $embedded["version"] !== $reference["version"]
      || $reference["package"] !== "durable-workflow/workflow") {
      throw new RuntimeException("The service adoption contract did not consume the installed embedded transition authority.");
  }
  $embeddedCells = array_map(
      static fn (array $cell): array => ["laravel" => rtrim($cell["laravel"], ".*"), "php" => $cell["php"]],
      $embedded["supported_intersection"]["cells"],
  );
  $serviceCells = array_map(
      static fn (array $cell): array => ["laravel" => $cell["laravel"], "php" => $cell["php"]],
      $service["framework"]["qualification_matrix"],
  );
  if ($embeddedCells !== $serviceCells) {
      throw new RuntimeException("Embedded and service modes publish different Laravel/PHP support cells.");
  }
'

source_state_after_install=$(hash_source_tables)
if [ "$source_state_after_install" != "$source_state_before" ]; then
  echo 'Installing service mode changed embedded-owned durable history.' >&2
  exit 1
fi

cat > app/Service/LaravelGreetingWorkflow.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Service;

use App\Support\GreetingPrefix;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\WorkflowContext;

final class LaravelGreetingWorkflow
{
    public function __construct(private readonly GreetingPrefix $prefix)
    {
    }

    #[Workflow('laravel.greeting')]
    public function run(WorkflowContext $context, string $name): string
    {
        if ($this->prefix->greet($name) === '') {
            throw new \LogicException('Laravel did not inject the service workflow dependency.');
        }

        return $context->activity('laravel.greet', [$name]);
    }
}
PHP
cat > app/Service/LaravelGreetingActivity.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Service;

use App\Support\GreetingPrefix;
use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Worker\ActivityContext;

final class LaravelGreetingActivity
{
    public function __construct(private readonly GreetingPrefix $prefix)
    {
    }

    #[Activity('laravel.greet')]
    public function run(ActivityContext $context, string $name): string
    {
        return $this->prefix->greet($name);
    }
}
PHP
cat > app/Service/StartLaravelGreeting.php <<'PHP'
<?php

declare(strict_types=1);

namespace App\Service;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;

final class StartLaravelGreeting
{
    public function __construct(private readonly LaravelWorkflowClientInterface $workflows)
    {
    }

    public function __invoke(string $workflowId): string
    {
        return $this->workflows->start(
            LaravelGreetingWorkflow::class,
            ['Ada'],
            workflowId: $workflowId,
        )->result();
    }
}
PHP

php artisan vendor:publish --tag=durable-workflow-config --force --no-interaction >/dev/null
php -r '
  $path = "config/durable-workflow.php";
  $contents = file_get_contents($path);
  $needle = "    ".chr(39)."handlers".chr(39)." => [";
  $replacement = $needle.PHP_EOL
      ."        App\\Service\\LaravelGreetingWorkflow::class,".PHP_EOL
      ."        App\\Service\\LaravelGreetingActivity::class,";
  if (!is_string($contents) || !str_contains($contents, $needle)) {
      throw new RuntimeException("The published Laravel configuration has no handler list.");
  }
  file_put_contents($path, str_replace($needle, $replacement, $contents));
'
cat >> routes/console.php <<'PHP'

Artisan::command('transition:service', function (): void {
    $worker = app(\DurableWorkflow\Bridge\Laravel\WorkerFactory::class)->make();
    $harness = new \DurableWorkflow\Testing\WorkerTestHarness($worker);
    $harness->assertActivityResult('laravel.greet', 'hello from Laravel, Ada', ['Ada']);
    $codec = app(\DurableWorkflow\Client::class)->payloadCodec();
    $completed = $harness->runWorkflow('laravel.greeting', ['Ada'], [
        ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'laravel.greet']],
        [
            'event_type' => 'ActivityCompleted',
            'payload' => [
                'sequence' => 1,
                'activity_type' => 'laravel.greet',
                'result' => $codec->envelope('hello from Laravel, Ada'),
            ],
        ],
    ]);
    $result = $codec->decodeEnvelope($completed->commands[0]['result'] ?? []);
    if ($result !== 'hello from Laravel, Ada') {
        throw new RuntimeException('The service worker did not preserve the injected application dependency.');
    }

    $fake = \DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow::fake()
        ->setWorkflowResult('service-new-run', 'hello from Laravel, Ada');
    if (app(\App\Service\StartLaravelGreeting::class)('service-new-run') !== 'hello from Laravel, Ada') {
        throw new RuntimeException('The service-owned start did not preserve the representative result.');
    }
    $fake->assertWorkflowStarted(
        \App\Service\LaravelGreetingWorkflow::class,
        ['Ada'],
        workflowId: 'service-new-run',
    );
    $fake->assertResultRequested('service-new-run');
    $this->line('service-transition=passed');
});
PHP

composer dump-autoload --no-interaction --no-scripts --quiet
env -u DURABLE_WORKFLOW_TOKEN \
  -u DURABLE_WORKFLOW_CLIENT_TOKEN \
  -u DURABLE_WORKFLOW_WORKER_TOKEN \
  DURABLE_WORKFLOW_RUNTIME_URL=http://localhost:8080 \
  DURABLE_WORKFLOW_NAMESPACE=transition \
  DURABLE_WORKFLOW_TASK_QUEUE=service-transition \
  php artisan config:cache --no-interaction >/dev/null
DURABLE_WORKFLOW_CLIENT_TOKEN=client-secret \
  DURABLE_WORKFLOW_WORKER_TOKEN=worker-secret \
  php artisan transition:service | grep -q 'service-transition=passed'

source_state_after_service=$(hash_source_tables)
if [ "$source_state_after_service" != "$source_state_before" ]; then
  echo 'The service-mode rehearsal changed embedded-owned durable history.' >&2
  exit 1
fi

embedded_open_after=$(php artisan transition:embedded-inspect embedded-open)
EMBEDDED_RESULT="$embedded_open_after" php -r '
  $result = json_decode(getenv("EMBEDDED_RESULT"), true, 512, JSON_THROW_ON_ERROR);
  if ($result["completed"] !== false) {
      throw new RuntimeException("The embedded-owned open run did not remain routed to its original engine.");
  }
'

printf 'source_mode=%s laravel=%s workflow=%s sdk=2.0.x-dev result=%s history_owner=embedded status=passed\n' \
  "$source_mode" "$laravel_requirement" "$source_version_after" 'hello from Laravel, Ada'
