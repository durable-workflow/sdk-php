<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony\DependencyInjection;

use DurableWorkflow\Attribute\Workflow;
use ReflectionClass;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/** Keeps workflow services fresh while leaving injected collaborators under container ownership. */
final class WorkflowLifetimePass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        foreach ($container->getDefinitions() as $definition) {
            if (!$definition->isAutoconfigured() && !$definition->hasTag(DurableWorkflowExtension::HANDLER_TAG)) {
                continue;
            }
            $class = $definition->getClass();
            if ($class === null || !class_exists($class)) {
                continue;
            }
            foreach ((new ReflectionClass($class))->getMethods() as $method) {
                if ($method->getAttributes(Workflow::class) !== []) {
                    $definition->setShared(false);
                    break;
                }
            }
        }
    }
}
