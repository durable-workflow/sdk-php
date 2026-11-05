import {watchPublicStatus} from './identity/status.mjs';

const pill = document.querySelector('.status-pill');
const label = pill?.querySelector('.status-label');
const labels = {
  operational: 'All systems operational', degraded: 'Service degraded',
  incident: 'Service incident', maintenance: 'Maintenance', stale: 'Status stale',
  unverified: 'Status unverified', unavailable: 'Status unavailable', unknown: 'Status unknown',
};
let cleanup;
const show = (state) => {
  const selected = Object.hasOwn(labels, state) ? state : 'unknown';
  if (!pill || !label) return;
  pill.dataset.state = selected;
  if (label.textContent !== labels[selected]) label.textContent = labels[selected];
};
const start = () => {
  cleanup?.();
  show('unknown');
  cleanup = watchPublicStatus(show);
};
if (pill && label) {
  start();
  window.addEventListener('pagehide', () => { cleanup?.(); cleanup = undefined; show('unknown'); });
  window.addEventListener('pageshow', event => { if (event.persisted) start(); });
}
