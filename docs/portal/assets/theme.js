(() => {
  // Light is the default. Dark is a deliberate, persistent user choice.
  try {
    if (localStorage.getItem('durable-workflow-theme') === 'dark') document.documentElement.dataset.theme = 'dark';
  } catch (_) { /* A blocked storage API keeps the light default. */ }
})();
