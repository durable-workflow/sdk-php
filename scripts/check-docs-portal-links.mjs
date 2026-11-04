export const PORTAL_ORIGIN = 'https://php.durable-workflow.com';

// Every portal link with a fragment, resolved against the page it appears on:
// in-page (#frag), root-relative (/path/#frag), relative (../path/#frag) and
// absolute same-origin (https://php.durable-workflow.com/path/#frag) links.
// Links to other origins are left to their own sites.
export function fragmentLinks(html, relativePage) {
  const page = new URL(relativePage, `${PORTAL_ORIGIN}/`);
  return [...html.matchAll(/\shref="([^"]*#[^"]*)"/g)].flatMap(([, href]) => {
    const url = new URL(href.replaceAll('&amp;', '&'), page);
    if (url.origin !== PORTAL_ORIGIN || url.hash.length < 2) return [];
    return [{href, url, fragment: decodeURIComponent(url.hash.slice(1))}];
  });
}
