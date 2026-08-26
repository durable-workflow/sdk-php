import assert from 'node:assert/strict';

export function assertPrimaryGuideNavigation(navigation) {
  const items = navigation.flatMap(({items: groupItems}) => groupItems);
  const urls = items.map(({url}) => url);

  assert.equal(
    new Set(urls).size,
    urls.length,
    'Every guide route must have exactly one primary navigation owner.',
  );

  return items;
}
