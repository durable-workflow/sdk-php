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
