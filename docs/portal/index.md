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
      <p class="hero-copy">Move an existing Laravel workflow application to Durable Workflow 2.0 without giving up its container, configuration, commands, logs, or tests. Choose embedded state or a separately operated service.</p>
      <div class="hero-actions">
        <a class="button primary" href="/frameworks/laravel/">Choose a Laravel path <span aria-hidden="true">→</span></a>
        <a class="button secondary" href="/getting-started/first-workflow/">Start with plain PHP</a>
      </div>
    </div>
    <div>
      <div class="code-stage hero-terminal" aria-label="Quickstart commands">
        <div class="code-stage__bar"><span>first-workflow</span><button class="copy-button" type="button">Copy</button></div>
        <pre><code>{{ release.composerCommand }}
env -u DURABLE_WORKFLOW_CLIENT_TOKEN \
  php worker.php &amp;
env -u DURABLE_WORKFLOW_WORKER_TOKEN \
  php client.php</code></pre>
      </div>
      <p class="terminal-note">After <a href="/getting-started/first-workflow/">configuring the runtime and saving the example files</a>.</p>
    </div>
  </div>
</section>

<section class="home-section" id="choose-php-path">
  <div class="section-heading">
    <h2>Start where your PHP already lives</h2>
    <p>The durable model is shared. The bootstrap, container, and operating experience stay idiomatic to your stack.</p>
  </div>
  <div class="path-grid">
    <a class="path-card" href="/frameworks/laravel/">
      <span class="number">01 · LARAVEL</span>
      <h3>Move from v1 without losing Laravel</h3>
      <p>Choose v2 embedded or service mode now, then change ownership later through one supported transition matrix.</p>
      <span class="arrow">Laravel guide →</span>
    </a>
    <a class="path-card" href="/getting-started/first-workflow/">
      <span class="number">02 · PLAIN PHP</span>
      <h3>Client + remote worker</h3>
      <p>Use the framework-neutral SDK in a script, service, or existing application.</p>
      <span class="arrow">Plain PHP guide →</span>
    </a>
    <a class="path-card" href="/frameworks/symfony/">
      <span class="number">03 · SYMFONY</span>
      <h3>Container-managed worker</h3>
      <p>Configure the first-party SDK as services and run a Console command under your supervisor.</p>
      <span class="arrow">Symfony guide →</span>
    </a>
  </div>
</section>

<section class="home-section" id="choose-runtime">
  <div class="section-heading">
    <h2>Choose who runs the runtime</h2>
    <p>Your workflow code and task queue stay the same. The runtime URL, credentials, and operating boundary change.</p>
  </div>
  <div class="deployment-grid">
    <a class="deployment-card" href="/operate/deployment/#self-hosted-server">
      <span class="tag">Your infrastructure · no account</span>
      <h3>Self-hosted Server</h3>
      <p>Run the published Server image with your database, auth policy, and operational controls.</p>
      <span class="arrow">Run Server →</span>
    </a>
    <article class="deployment-card dw-cloud-promotion" data-promotion-source="sdk-php-reference">
      <span class="tag">Managed · limited access</span>
      <h3>Durable Workflow Cloud</h3>
      <p>Request a managed namespace, then use its full runtime URL with separate client and worker credentials.</p>
      <a class="arrow dw-cloud-promotion__action" data-promotion-action="early-access" href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference">Request early access →</a>
    </article>
  </div>
</section>
