---
layout: layout.njk
layoutType: home
title: PHP workflows that keep going
description: Build, test, deploy, and operate durable PHP workflows with first-party paths for plain PHP, Laravel, Symfony, Cloud, and self-hosted Server.
---
<section class="hero">
  <div class="hero-grid">
    <div>
      <div class="release-kicker">{{ release.sdkChannelLabel }}</div>
      <h1>Work that keeps going. <em>Even when PHP stops.</em></h1>
      <p class="hero-copy">Start a workflow, survive process restarts, retry unreliable work, and come back for the result. Choose the PHP and deployment path that matches your application.</p>
      <div class="hero-actions">
        <a class="button primary" href="/getting-started/first-workflow/">Run your first workflow <span aria-hidden="true">→</span></a>
        <a class="button secondary" href="/build/workflows-activities/">Explore the concepts</a>
      </div>
    </div>
    <div class="hero-terminal" aria-label="Example terminal session">
      <div class="terminal-bar"><i></i><i></i><i></i><span>first-workflow</span></div>
      <pre><span class="prompt">$</span> {{ release.composerCommand }}
<span class="prompt">$</span> env -u DURABLE_WORKFLOW_CLIENT_TOKEN php worker.php &amp;
<span class="result">✓ worker registered · queue php-quickstart</span>
<span class="prompt">$</span> env -u DURABLE_WORKFLOW_WORKER_TOKEN php client.php
<span class="result">{"workflow_id":"php-quickstart-a83f…","result":{"greeting":"hello, PHP"}}</span></pre>
    </div>
  </div>
</section>

<div class="capability-strip" aria-label="SDK capabilities"><div><span>Workflows</span><span>Activities</span><span>Signals</span><span>Queries</span><span>Updates</span><span>Retries</span></div></div>

<section class="home-section" id="choose-php-path">
  <div class="section-heading">
    <h2>Start where your PHP already lives.</h2>
    <p>The durable model is shared. The bootstrap, container, and operating experience stay idiomatic to your stack.</p>
  </div>
  <div class="path-grid">
    <a class="path-card" href="/getting-started/first-workflow/">
      <span class="number">01 / PLAIN PHP</span>
      <h3>Client + remote worker</h3>
      <p>Use the framework-neutral SDK in a script, service, or existing application.</p>
      <span class="arrow" aria-hidden="true">→</span>
    </a>
    <a class="path-card" href="/frameworks/laravel/">
      <span class="number">02 / LARAVEL</span>
      <h3>Choose embedded or service mode</h3>
      <p>Use the first-party Laravel engine in-app, or inject the standalone SDK for Cloud and Server.</p>
      <span class="arrow" aria-hidden="true">→</span>
    </a>
    <a class="path-card" href="/frameworks/symfony/">
      <span class="number">03 / SYMFONY</span>
      <h3>Container-managed worker</h3>
      <p>Configure the first-party SDK as services and run a Console command under your supervisor.</p>
      <span class="arrow" aria-hidden="true">→</span>
    </a>
  </div>
</section>

<section class="home-section" id="choose-runtime">
  <div class="section-heading">
    <h2>Choose who operates the runtime.</h2>
    <p>Your workflow code and task queue stay the same. The runtime URL, credentials, and operating boundary change.</p>
  </div>
  <div class="deployment-grid">
    <article class="deployment-card dw-cloud-promotion" data-promotion-source="sdk-php-reference">
      <span class="tag">Managed · limited access</span>
      <h3>Durable Workflow Cloud</h3>
      <p>Request a managed namespace, then use its full runtime URL with separate client and worker credentials.</p>
      <a class="arrow dw-cloud-promotion__action" data-promotion-action="early-access" href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference">Request early access →</a>
    </article>
    <a class="deployment-card" href="/operate/deployment/#self-hosted-server">
      <span class="tag">Your infrastructure</span>
      <h3>Self-hosted Server</h3>
      <p>Run the published Server image with your database, auth policy, and operational controls.</p>
      <span class="arrow" aria-hidden="true">Run Server →</span>
    </a>
  </div>
</section>

<section class="home-section">
  <div class="section-heading">
    <h2>Learn one useful layer at a time.</h2>
    <p>Get a result first. Then add durable messages, retry policy, tests, and production operations as your workflow needs them.</p>
  </div>
  <div class="path-grid">
    <a class="path-card" href="/build/messages/"><span class="number">COMMUNICATE</span><h3>Signals, queries &amp; updates</h3><p>Send events, inspect replayed state, and request tracked changes.</p><span class="arrow">→</span></a>
    <a class="path-card" href="/build/testing/"><span class="number">PROVE</span><h3>Replay-safe tests</h3><p>Test handlers quickly, then qualify the real worker and runtime boundary.</p><span class="arrow">→</span></a>
    <a class="path-card" href="/operate/troubleshooting/"><span class="number">RECOVER</span><h3>Operate with evidence</h3><p>Diagnose auth, queue, replay, retry, and version mismatches by symptom.</p><span class="arrow">→</span></a>
  </div>
</section>
