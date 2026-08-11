(function () {
  'use strict';

  const TOKEN = '__CLOUDFLARE_WEB_ANALYTICS_TOKEN__';
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
  const PROMOTION_SOURCE = 'sdk-php-reference';
  const PROMOTION_EVENT_URL = 'https://cloud.durable-workflow.com/early-access/promotion-events';

  function isPublicPage(hostname, pathname) {
    if (PUBLIC_STATIC_HOSTS.has(hostname)) return true;
    return PUBLIC_ROUTE_HOSTS[hostname]?.has(pathname) === true;
  }

  function sendPromotionEvent(event) {
    if (window.location.hostname !== 'php.durable-workflow.com') return;

    window.fetch(PROMOTION_EVENT_URL, {
      method: 'POST',
      mode: 'cors',
      credentials: 'omit',
      keepalive: true,
      referrerPolicy: 'origin',
      headers: {'Content-Type': 'text/plain'},
      body: JSON.stringify({source: PROMOTION_SOURCE, event}),
    }).catch(() => {});
  }

  function initializePromotion() {
    const promotion = document.querySelector(`[data-promotion-source="${PROMOTION_SOURCE}"]`);
    const action = promotion?.querySelector('[data-promotion-action="early-access"]');
    if (!promotion || !action) return;

    let impressionSent = false;
    const recordImpression = function () {
      if (impressionSent) return;
      impressionSent = true;
      sendPromotionEvent('impression');
    };
    action.addEventListener('click', function () { sendPromotionEvent('click'); });

    if (!('IntersectionObserver' in window)) {
      recordImpression();
      return;
    }

    const observer = new IntersectionObserver(function (entries) {
      if (!entries.some(entry => entry.isIntersecting)) return;
      recordImpression();
      observer.disconnect();
    }, {threshold: 0.35});
    observer.observe(promotion);
  }

  initializePromotion();

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
  loader.type = 'module';
  loader.src = BEACON_URL;
  loader.dataset.cfBeacon = JSON.stringify({token: TOKEN});
  document.head.appendChild(loader);
}());
