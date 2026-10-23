<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony\DependencyInjection;

use LogicException;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/** Symfony configuration schema for Server and Cloud service-mode connections. */
final class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('durable_workflow');
        $this->configureRoot($treeBuilder->getRootNode());

        return $treeBuilder;
    }

    private function configureRoot(NodeDefinition $rootNode): void
    {
        if (!$rootNode instanceof ArrayNodeDefinition) {
            throw new LogicException('The Durable Workflow configuration root must be an array node.');
        }

        $children = $rootNode->children();
        $children->scalarNode('endpoint')
            ->defaultValue('http://localhost:8080')
            ->cannotBeEmpty()
            ->info('Self-hosted Server origin or complete Cloud runtime base URI without /api.');
        $children->scalarNode('namespace')->defaultValue('default')->cannotBeEmpty();
        $children->scalarNode('task_queue')->defaultValue('php-workers')->cannotBeEmpty();
        $children->integerNode('poll_timeout_seconds')->defaultValue(5)->min(0)->max(60);
        $children->floatNode('request_timeout_seconds')->defaultNull()->min(0.001)
            ->info('Application Client control-request budget; null uses the SDK default. Worker and payload I/O are separate.');

        $worker = $children->arrayNode('worker')->addDefaultsIfNotSet();
        $stickyCache = $worker->children()->arrayNode('sticky_cache')->addDefaultsIfNotSet();
        $cacheChildren = $stickyCache->children();
        $cacheChildren->integerNode('capacity')->defaultValue(100)->min(1);
        $cacheChildren->integerNode('ttl_seconds')->defaultValue(300)->min(1)->max(3600);

        $credentials = $children->arrayNode('credentials');
        $credentials->addDefaultsIfNotSet();
        $credentialChildren = $credentials->children();
        $credentialChildren->scalarNode('token')->defaultNull();
        $credentialChildren->scalarNode('control_token')->defaultNull();
        $credentialChildren->scalarNode('worker_token')->defaultNull();

        $children->arrayNode('handlers')
            ->defaultValue([])
            ->scalarPrototype()
            ->cannotBeEmpty();
    }
}
