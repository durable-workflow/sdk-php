(() => {
  'use strict';
  const toggle = document.querySelector('.nav-toggle');
  const navigation = document.querySelector('.primary-nav');
  const closeNavigation = () => {
    toggle?.setAttribute('aria-expanded', 'false');
    toggle?.setAttribute('aria-label', 'Open navigation');
    navigation?.classList.remove('is-open');
  };
  toggle?.addEventListener('click', () => {
    const open = toggle.getAttribute('aria-expanded') !== 'true';
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-label', open ? 'Close navigation' : 'Open navigation');
    navigation?.classList.toggle('is-open', open);
  });
  const switcher = document.querySelector('.sdk-switcher');
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    if (switcher?.open) { switcher.open = false; switcher.querySelector('summary').focus(); }
    else if (toggle?.getAttribute('aria-expanded') === 'true') { closeNavigation(); toggle.focus(); }
  });
  document.addEventListener('click', (event) => {
    if (switcher?.open && !switcher.contains(event.target)) switcher.open = false;
  });
  const theme = document.querySelector('.theme-toggle');
  const syncTheme = () => {
    const dark = document.documentElement.dataset.theme === 'dark';
    theme?.setAttribute('aria-pressed', String(dark));
    theme?.setAttribute('aria-label', dark ? 'Switch to light theme' : 'Switch to dark theme');
    if (theme) theme.textContent = dark ? 'Light' : 'Dark';
  };
  theme?.addEventListener('click', () => {
    const selected = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = selected;
    try { localStorage.setItem('durable-workflow-theme', selected); } catch (_) { /* Theme still works for this page. */ }
    syncTheme();
  });
  syncTheme();
  // Markdown and generated reference blocks get the same working copy control.
  for (const pre of document.querySelectorAll('pre')) {
    if (!pre.querySelector('code') || pre.closest('.code-stage')) continue;
    const stage = document.createElement('div'); stage.className = 'code-stage';
    pre.before(stage);
    const bar = document.createElement('div'); bar.className = 'code-stage__bar';
    const label = document.createElement('span'); label.textContent = 'Code';
    const copy = document.createElement('button'); copy.className = 'copy-button'; copy.type = 'button'; copy.textContent = 'Copy';
    bar.append(label, copy); stage.append(bar, pre);
  }
  for (const button of document.querySelectorAll('.copy-button')) {
    button.setAttribute('aria-live', 'polite');
    button.addEventListener('click', async () => {
      const source = button.closest('.code-stage')?.querySelector('code')?.textContent;
      if (!source) return;
      try { await navigator.clipboard.writeText(source); button.textContent = 'Copied'; }
      catch (_) {
        // Clipboard permission or browser support may be unavailable; report it in the control.
        button.textContent = 'Copy unavailable';
      }
      window.setTimeout(() => { button.textContent = 'Copy'; }, 1800);
    });
  }
})();
