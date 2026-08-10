const searchResults = document.querySelector('.phpdocumentor-search-results');
const reference = document.querySelector('.phpdocumentor');
const searchForm = document.querySelector('[data-search-form]');
const searchResultsBody = searchResults?.querySelector('.phpdocumentor-search-results__body');

if (searchResults && reference && searchForm && searchResultsBody) {
  const searchField = searchForm.querySelector('.phpdocumentor-search__field');
  const searchFormHome = searchForm.parentElement;
  const searchFormNextSibling = searchForm.nextSibling;
  const background = [
    document.querySelector('.phpdocumentor-header'),
    document.querySelector('.dw-cloud-promotion'),
    ...reference.querySelectorAll(
      ':scope > .phpdocumentor-section > :not(.phpdocumentor-search-results), '
        + ':scope > .phpdocumentor-back-to-top',
    ),
  ].filter(Boolean);

  const syncSearchBackground = () => {
    const searchIsOpen = !searchResults.classList.contains('phpdocumentor-search-results--hidden');

    if (searchIsOpen) {
      searchResultsBody.before(searchForm);
      searchField?.focus({preventScroll: true});
    } else if (searchForm.parentElement !== searchFormHome) {
      searchFormHome.insertBefore(searchForm, searchFormNextSibling);
    }

    for (const element of background) {
      if (searchIsOpen && !element.hasAttribute('inert')) {
        element.setAttribute('inert', '');
        element.dataset.apiReferenceSearchBackground = '';
      } else if (!searchIsOpen && element.hasAttribute('data-api-reference-search-background')) {
        element.removeAttribute('inert');
        delete element.dataset.apiReferenceSearchBackground;
      }
    }
  };

  new MutationObserver(syncSearchBackground).observe(searchResults, {
    attributes: true,
    attributeFilter: ['class'],
  });
  syncSearchBackground();
}

const onThisPageContent = document.querySelector('.phpdocumentor-on-this-page__content');

if (onThisPageContent) {
  let heightFrame;

  const syncOnThisPageHeight = () => {
    heightFrame = undefined;

    if (!window.matchMedia('(min-width: 1000px)').matches) {
      onThisPageContent.style.removeProperty('height');
      return;
    }

    const top = Math.max(0, onThisPageContent.getBoundingClientRect().top);
    let availableHeight = Math.max(1, Math.floor(window.innerHeight - top - 1));
    const availableBottom = top + availableHeight;
    const clippedEntry = [...onThisPageContent.querySelectorAll('a[href]')].find((entry) => {
      const box = entry.getBoundingClientRect();
      return box.top < availableBottom && box.bottom > availableBottom;
    });

    if (clippedEntry) {
      availableHeight = Math.max(
        1,
        Math.floor(clippedEntry.getBoundingClientRect().top - top),
      );
    }
    onThisPageContent.style.height = `${availableHeight}px`;
  };

  const scheduleOnThisPageHeight = () => {
    if (heightFrame !== undefined) window.cancelAnimationFrame(heightFrame);
    heightFrame = window.requestAnimationFrame(syncOnThisPageHeight);
  };

  window.addEventListener('resize', scheduleOnThisPageHeight);
  window.addEventListener('scroll', scheduleOnThisPageHeight, {passive: true});
  new MutationObserver(scheduleOnThisPageHeight).observe(onThisPageContent, {
    attributes: true,
    attributeFilter: ['class'],
  });
  if (searchResults) {
    new MutationObserver(syncOnThisPageHeight).observe(searchResults, {
      attributes: true,
      attributeFilter: ['class'],
    });
  }
  scheduleOnThisPageHeight();
}
