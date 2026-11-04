const fs = require('node:fs');
const path = require('node:path');

const QUICKSTART_CONTRACT = require('./docs/quickstart-contract.json');
const QUICKSTART_BASES = QUICKSTART_CONTRACT.reference_resolution?.bases;
const QUICKSTART_SOURCES = Object.freeze(Object.fromEntries(
  Object.entries(QUICKSTART_CONTRACT.sources).map(([name, reference]) => {
    const base = QUICKSTART_BASES?.[reference.base];
    if (reference.kind !== 'composer_package_path'
      || base?.kind !== 'composer_package'
      || base.package_pointer !== '/package'
      || typeof reference.path !== 'string') {
      throw new Error(`Invalid package-owned quickstart source reference: ${name}`);
    }
    return [name, reference.path];
  }),
));

function escapeHtml(value) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll('\n', '&#10;');
}

// GitHub-style heading slugs, so links written against the GitHub rendering of
// these pages (and from durable-workflow.com) resolve on the portal too.
function headingSlug(text) {
  return text
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{M}\p{N}\p{Pc}\- ]/gu, '')
    .replaceAll(' ', '-');
}

// Ids follow GitHub's slugger: a repeated slug takes the next free numeric
// suffix, skipping ids already emitted (including a literal "Foo-1" heading).
function headingIds(markdown) {
  markdown.core.ruler.push('heading_ids', (state) => {
    const occurrences = new Map();
    for (const token of state.tokens) {
      const id = token.type === 'heading_open' ? token.attrGet('id') : null;
      if (id !== null) occurrences.set(id, 0);
    }
    state.tokens.forEach((token, index) => {
      if (token.type !== 'heading_open' || token.attrGet('id') !== null) return;
      const inline = state.tokens[index + 1];
      const text = (inline?.children ?? [])
        .filter((child) => child.type === 'text' || child.type === 'code_inline')
        .map((child) => child.content)
        .join('');
      const base = headingSlug(text);
      if (base === '') return;
      let id = base;
      while (occurrences.has(id)) {
        const count = occurrences.get(base) + 1;
        occurrences.set(base, count);
        id = `${base}-${count}`;
      }
      occurrences.set(id, 0);
      token.attrSet('id', id);
    });
  });
}

module.exports = function configure(eleventyConfig) {
  eleventyConfig.amendLibrary('md', (markdown) => markdown.use(headingIds));

  eleventyConfig.addPassthroughCopy({'docs/CNAME': 'CNAME'});
  eleventyConfig.addPassthroughCopy({'docs/portal/assets': 'assets'});
  eleventyConfig.addPassthroughCopy({'docs/quickstart-contract.json': 'quickstart-contract.json'});
  eleventyConfig.addPassthroughCopy({'docs/quickstart-contract.schema.v2.json': 'quickstart-contract.schema.v2.json'});
  eleventyConfig.addPassthroughCopy({'docs/laravel-adoption-contract.json': 'laravel-adoption-contract.json'});
  eleventyConfig.addPassthroughCopy({'docs/laravel-adoption-contract.schema.json': 'laravel-adoption-contract.schema.json'});
  eleventyConfig.addPassthroughCopy({'.phpdoc/template/analytics': 'analytics'});
  eleventyConfig.addPassthroughCopy({'.phpdoc/template/assets': 'assets'});
  eleventyConfig.addPassthroughCopy({'build/api': 'api'});

  eleventyConfig.addShortcode('sourceFile', function sourceFile(sourceName, language = 'text', label = sourceName) {
    const sourcePath = QUICKSTART_SOURCES[sourceName];
    if (typeof sourcePath !== 'string') {
      throw new Error(`Unknown package-owned quickstart source: ${sourceName}`);
    }
    const resolved = path.resolve(__dirname, sourcePath);
    const examplesDirectory = path.resolve(__dirname, 'examples');
    if (!resolved.startsWith(`${examplesDirectory}${path.sep}`)) {
      throw new Error(`Quickstart source must stay under ${examplesDirectory}: ${sourcePath}`);
    }
    const source = fs.readFileSync(resolved, 'utf8').trimEnd();
    return `<div class="code-stage" data-quickstart-source="${escapeHtml(sourcePath)}"><div class="code-stage__bar"><span>${escapeHtml(label)}</span><button class="copy-button" type="button">Copy</button></div><pre><code class="language-${escapeHtml(language)}">${escapeHtml(source)}</code></pre></div>`;
  });

  eleventyConfig.addFilter('absoluteUrl', (url) => new URL(url, 'https://php.durable-workflow.com').toString());

  return {
    dir: {
      input: 'docs/portal',
      includes: '_includes',
      data: '_data',
      output: 'build/site',
    },
    htmlTemplateEngine: 'njk',
    markdownTemplateEngine: 'njk',
    templateFormats: ['md', 'njk'],
  };
};

module.exports.headingIds = headingIds;
