(function () {
  'use strict';

  const MEASUREMENT_ID = 'G-HD1YHT442Y';
  const SITE_HOSTNAME = 'php.durable-workflow.com';
  const PARENT_COOKIE_DOMAIN = 'durable-workflow.com';
  const CONSENT_KEY = 'durable-workflow.analytics-consent.v1';
  const LOADER_ID = 'durable-workflow-ga4-loader';
  const BANNER_ID = 'durable-workflow-analytics-consent';
  const PREFERENCES_ID = 'durable-workflow-analytics-preferences';
  const configuredPath = document.currentScript?.dataset.analyticsPath;
  const runtime = window.__durableWorkflowAnalytics || {};

  if (runtime.initialized) return;
  runtime.initialized = true;
  window.__durableWorkflowAnalytics = runtime;

  function readConsent() {
    try { return window.localStorage.getItem(CONSENT_KEY); } catch (_error) { return null; }
  }

  function writeConsent(value) {
    try { window.localStorage.setItem(CONSENT_KEY, value); } catch (_error) { /* Use the choice in memory. */ }
  }

  function normalizedPath() {
    const path = configuredPath || window.location.pathname || '/';
    if (!path.startsWith('/') || path.includes('?') || path.includes('#')) return '/';
    return path.replace(/\/index\.html$/, '/') || '/';
  }

  function pageFields() {
    const pagePath = normalizedPath();
    return {
      page_hostname: SITE_HOSTNAME,
      page_location: `https://${SITE_HOSTNAME}${pagePath}`,
      page_path: pagePath,
      page_referrer: '',
      page_title: document.title,
    };
  }

  function enableAnalytics() {
    if (runtime.analyticsEnabled || window.location.hostname !== SITE_HOSTNAME) return;
    runtime.analyticsEnabled = true;
    window.dataLayer = window.dataLayer || [];
    window.gtag = window.gtag || function () { window.dataLayer.push(arguments); };
    window.gtag('consent', 'default', {
      ad_personalization: 'denied', ad_storage: 'denied', ad_user_data: 'denied', analytics_storage: 'denied',
    });
    window.gtag('consent', 'update', {
      ad_personalization: 'denied', ad_storage: 'denied', ad_user_data: 'denied', analytics_storage: 'granted',
    });
    window.gtag('js', new Date());
    window.gtag('config', MEASUREMENT_ID, {
      ...pageFields(),
      allow_ad_personalization_signals: false,
      allow_google_signals: false,
      anonymize_ip: true,
      cookie_domain: SITE_HOSTNAME,
      send_page_view: true,
    });
    if (!document.getElementById(LOADER_ID)) {
      const loader = document.createElement('script');
      loader.id = LOADER_ID;
      loader.async = true;
      loader.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(MEASUREMENT_ID)}`;
      loader.addEventListener('error', function () {});
      document.head.appendChild(loader);
    }
  }

  function removeAnalyticsCookies() {
    for (const cookie of document.cookie.split(';')) {
      const name = cookie.split('=', 1)[0].trim();
      if (!name.startsWith('_ga')) continue;
      document.cookie = `${name}=; Max-Age=0; Path=/; SameSite=Lax`;
      for (const domain of new Set([SITE_HOSTNAME, PARENT_COOKIE_DOMAIN])) {
        document.cookie = `${name}=; Max-Age=0; Path=/; Domain=.${domain}; SameSite=Lax`;
      }
    }
  }

  function hideBanner() { document.getElementById(BANNER_ID)?.setAttribute('hidden', ''); }

  function showPreferencesButton() {
    let button = document.getElementById(PREFERENCES_ID);
    if (!button) {
      button = document.createElement('button');
      button.id = PREFERENCES_ID;
      button.className = 'dw-analytics-preferences';
      button.type = 'button';
      button.textContent = 'Analytics preferences';
      button.addEventListener('click', showBanner);
      (document.querySelector('.phpdocumentor-header') || document.body).appendChild(button);
    }
    button.removeAttribute('hidden');
  }

  function chooseConsent(value) {
    const previous = readConsent();
    writeConsent(value);
    hideBanner();
    showPreferencesButton();
    if (value === 'granted') { enableAnalytics(); return; }
    removeAnalyticsCookies();
    if (previous === 'granted' && runtime.analyticsEnabled) window.location.reload();
  }

  function showBanner() {
    let banner = document.getElementById(BANNER_ID);
    if (!banner) {
      banner = document.createElement('section');
      banner.id = BANNER_ID;
      banner.className = 'dw-analytics-consent';
      banner.setAttribute('role', 'dialog');
      banner.setAttribute('aria-modal', 'false');
      banner.setAttribute('aria-label', 'Analytics preferences');
      banner.innerHTML = '<div><strong>Optional site analytics</strong><p>With your permission, Google Analytics receives this site\'s hostname and page path. Query strings and form values are not sent.</p></div><div class="dw-analytics-consent__actions"><button type="button" data-consent="denied">Only necessary</button><button type="button" data-consent="granted">Allow analytics</button></div>';
      banner.querySelector('[data-consent="denied"]').addEventListener('click', function () { chooseConsent('denied'); });
      banner.querySelector('[data-consent="granted"]').addEventListener('click', function () { chooseConsent('granted'); });
      document.body.appendChild(banner);
    }
    document.getElementById(PREFERENCES_ID)?.setAttribute('hidden', '');
    banner.removeAttribute('hidden');
  }

  function initializeConsent() {
    const consent = readConsent();
    if (consent === 'granted') { showPreferencesButton(); enableAnalytics(); }
    else if (consent === 'denied') showPreferencesButton();
    else showBanner();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initializeConsent, { once: true });
  else initializeConsent();
}());
