import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import test from 'node:test';
import MarkdownIt from 'markdown-it';
import {fragmentLinks} from './check-docs-portal-links.mjs';

const {headingIds} = createRequire(import.meta.url)('../.eleventy.cjs');

function renderedIds(markdown) {
  const html = new MarkdownIt().use(headingIds).render(markdown);
  return [...html.matchAll(/<h\d id="([^"]+)"/g)].map((match) => match[1]);
}

test('repeated headings take the next free suffix', () => {
  assert.deepEqual(renderedIds('# Foo\n\n# Foo\n\n# Foo\n'), ['foo', 'foo-1', 'foo-2']);
});

test('a literal suffixed heading never duplicates an emitted id', () => {
  assert.deepEqual(renderedIds('# Foo\n\n# Foo\n\n# Foo-1\n\n# Foo\n'), ['foo', 'foo-1', 'foo-1-1', 'foo-2']);
  assert.deepEqual(renderedIds('# Foo-1\n\n# Foo\n\n# Foo\n'), ['foo-1', 'foo', 'foo-2']);
});

test('fragment links resolve against the page that contains them', () => {
  const html = [
    '<a href="#main-content">',
    '<a href="/operate/deployment/#self-hosted-server">',
    '<a href="../client-setup/#bound-ordinary-http-requests">',
    '<a href="https://php.durable-workflow.com/frameworks/laravel/#install">',
    '<a href="//php.durable-workflow.com/operate/#scale">',
    '<a href="https://durable-workflow.com/docs/polyglot/#queues">',
    '<a href="https://cloud.durable-workflow.com/early-access#source=sdk-php">',
    '<a href="/getting-started/">',
    '<a href="#">',
  ].join('\n');

  assert.deepEqual(
    fragmentLinks(html, 'getting-started/quickstart/index.html')
      .map(({url, fragment}) => [url.pathname, fragment]),
    [
      ['/getting-started/quickstart/index.html', 'main-content'],
      ['/operate/deployment/', 'self-hosted-server'],
      ['/getting-started/client-setup/', 'bound-ordinary-http-requests'],
      ['/frameworks/laravel/', 'install'],
      ['/operate/', 'scale'],
    ],
  );
});

test('encoded fragments are compared as element ids', () => {
  const [link] = fragmentLinks('<a href="../setup/#caf%C3%A9">', 'guide/index.html');
  assert.equal(link.fragment, 'café');
  assert.equal(link.url.pathname, '/setup/');
});
