<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** Build selection for the successor run created by continue-as-new. */
enum ContinueAsNewTargetBuild: string
{
    case Inherited = 'inherited';
    case Promoted = 'promoted';

    /** @internal */
    public const CAPABILITY = 'continue_as_new_target_build';
}
