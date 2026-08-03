<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony\DependencyInjection;

use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Bridge\Symfony\WorkerCommand;
use DurableWorkflow\Bridge\Symfony\WorkerFactory;
use DurableWorkflow\Client;
use DurableWorkflow\WorkflowClientInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Argument\TaggedIteratorArgument;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;

/** Registers validated, autowired Symfony services without owning durable state. */
final class DurableWorkflowExtension extends Extension
{
    public const HANDLER_TAG = 'durable_workflow.handler';

    /** @param array<int, array<string, mixed>> $configs */
    public function load(array $configs, ContainerBuilder $container): void
    {
        /** @var array<string, mixed> $values */
        $values = $this->processConfiguration(new Configuration(), $configs);

        $container->register(ServiceConfiguration::class, ServiceConfiguration::class)
            ->setFactory([ServiceConfiguration::class, 'fromArray'])
            ->setArguments([$values]);
        $container->register(Client::class, Client::class)
            ->setFactory([new Reference(ServiceConfiguration::class), 'client'])
            ->setPublic(true);
        $container->setAlias(WorkflowClientInterface::class, Client::class)->setPublic(true);

        foreach ($values['handlers'] as $handler) {
            if (!is_string($handler)) {
                continue;
            }
            if (!$container->hasDefinition($handler) && !$container->hasAlias($handler)) {
                $container->register($handler, $handler)
                    ->setAutowired(true)
                    ->setAutoconfigured(true);
            }
            $container->findDefinition($handler)->addTag(self::HANDLER_TAG);
        }

        $container->register(WorkerFactory::class, WorkerFactory::class)
            ->setArguments([
                new Reference(ServiceConfiguration::class),
                new Reference(Client::class),
                new TaggedIteratorArgument(self::HANDLER_TAG),
                new Reference(LoggerInterface::class, ContainerBuilder::NULL_ON_INVALID_REFERENCE),
                new Reference(EventDispatcherInterface::class, ContainerBuilder::NULL_ON_INVALID_REFERENCE),
            ])
            ->setPublic(true);
        $container->register(WorkerCommand::class, WorkerCommand::class)
            ->setArguments([new Reference(WorkerFactory::class)])
            ->addTag('console.command');
    }

    public function getAlias(): string
    {
        return 'durable_workflow';
    }
}
