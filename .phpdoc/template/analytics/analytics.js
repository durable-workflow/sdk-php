(function () {
  'use strict';

  const TOKEN = document.currentScript?.dataset.cloudflareWebAnalyticsToken
    || '__CLOUDFLARE_WEB_ANALYTICS_TOKEN__';
  const BEACON_URL = 'https://static.cloudflareinsights.com/beacon.min.js';
  const BEACON_SELECTOR = `script[src^="${BEACON_URL}"]`;
  const LOADER_ID = 'durable-workflow-cloudflare-web-analytics';
  const PUBLIC_STATIC_HOSTS = new Set([
    'durable-workflow.com',
    'php.durable-workflow.com',
    'python.durable-workflow.com',
    'rust.durable-workflow.com',
  ]);
  const PUBLIC_ROUTE_HOSTS = Object.freeze({
    'cloud.durable-workflow.com': new Set(['/', '/early-access', '/early-access/']),
    'status.durable-workflow.com': new Set(['/']),
  });

  function isPublicPage(hostname, pathname) {
    if (PUBLIC_STATIC_HOSTS.has(hostname)) return true;
    return PUBLIC_ROUTE_HOSTS[hostname]?.has(pathname) === true;
  }

  if (
    !/^[a-f0-9]{32}$/.test(TOKEN)
    || !isPublicPage(window.location.hostname, window.location.pathname)
    || document.getElementById(LOADER_ID)
    || document.querySelector(BEACON_SELECTOR)
  ) {
    return;
  }

  const loader = document.createElement('script');
  loader.id = LOADER_ID;
  loader.defer = true;
  loader.src = BEACON_URL;
  loader.dataset.cfBeacon = JSON.stringify({token: TOKEN, spa: true});
  document.head.appendChild(loader);
}());
