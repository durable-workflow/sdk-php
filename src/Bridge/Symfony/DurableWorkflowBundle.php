<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Bridge\Symfony\DependencyInjection\DurableWorkflowExtension;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;

/** Optional Symfony Bundle for Durable Workflow service mode. */
final class DurableWorkflowBundle extends Bundle
{
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);

        foreach ([Workflow::class, Activity::class, Query::class, Signal::class, Update::class] as $attribute) {
            $container->registerAttributeForAutoconfiguration(
                $attribute,
                static function (
                    ChildDefinition $definition,
                    Workflow|Activity|Query|Signal|Update $_attribute,
                    \Reflector $_reflector,
                ): void {
                    if (!$definition->hasTag(DurableWorkflowExtension::HANDLER_TAG)) {
                        $definition->addTag(DurableWorkflowExtension::HANDLER_TAG);
                    }
                },
            );
        }
    }
}
