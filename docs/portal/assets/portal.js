(() => {
  'use strict';

  const toggle = document.querySelector('.nav-toggle');
  const navigation = document.querySelector('.primary-nav');
  if (toggle && navigation) {
    const setNavigationOpen = (open) => {
      toggle.setAttribute('aria-expanded', String(open));
      toggle.setAttribute('aria-label', open ? 'Close navigation' : 'Open navigation');
      navigation.classList.toggle('is-open', open);
      document.body.classList.toggle('nav-open', open);
    };
    toggle.addEventListener('click', () => setNavigationOpen(toggle.getAttribute('aria-expanded') !== 'true'));
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && toggle.getAttribute('aria-expanded') === 'true') {
        setNavigationOpen(false);
        toggle.focus();
      }
    });
  }

  for (const button of document.querySelectorAll('.copy-button')) {
    button.addEventListener('click', async () => {
      const source = button.closest('.code-stage')?.querySelector('code')?.textContent;
      if (!source) return;
      await navigator.clipboard.writeText(source);
      button.textContent = 'Copied';
      window.setTimeout(() => { button.textContent = 'Copy'; }, 1600);
    });
  }
})();
