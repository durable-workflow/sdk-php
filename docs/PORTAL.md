# PHP documentation presentation and published source

The One Identity presentation lives in the existing Eleventy portal and
phpDocumentor templates. Both rendering engines share the static identity
header/footer. The canonical flowchart SVG is unchanged; typography is
self-hosted with its font licenses. Light is the default; dark is an explicit
user preference. The linked status pill uses the unmodified shared Docs #484 reducer/watch module, bounded aggregate labels and an honest neutral fallback. Cloud owns live public-GET CORS acceptance; source fixture results do not assert service health.

## Build and qualify

Use the repository's normal documentation commands and CI. The browser check
covers the home, framework guides and generated API at desktop, tablet,
mobile and short-height sizes. New presentation pages exercise both themes,
keyboard SDK switching, default theme behavior and exact clipboard copying.
Historical published pages retain their legacy compatibility checks.
`DOCS_SCREENSHOTS_DIRECTORY` saves representative home/guide/API screenshots.

## Stable publication inputs

The existing protected **PHP developer portal** workflow still selects
`stable_tag` and optional `stable_source` using the unchanged published-product
comparison. The presentation does not change that SDK source, its PHP/framework
support requirements, examples, contracts or API implementation.

Use `build_only: true` to render and qualify the exact stable tuple before a separately authorized deployment through the existing Pages environment. It does not bypass channel or presentation guards. Existing dispatch behavior is preserved when the flag is false.

The optional `presentation_source` names an exact accepted-main commit. It
requires a published stable tag. `prepare-docs-presentation.py` stages only the
landing page, layout, static assets, generated-reference layout and identity
components over copies of the selected source's portal/templates. Other guide
bodies and the API index remain selected-source inputs. The workflow-owned
release-data adapter reads the selected SDK's manifest and quickstart contract.
It never reads release facts from the presentation commit.

The staged template keeps phpDocumentor's `.phpdoc/template` directory shape.
The renderer runs from that staging root, so phpDocumentor's implicit overrides
cannot select the published checkout's older layout. Normal CI retains the
legacy render and also exercises the accepted presentation over the stable
documentation source, including its full browser checks.

Source code, Composer manifests, quickstart/adoption contracts, examples and
runtime configuration cannot be overlaid. Generated API symbols still come
from the selected published SDK's `src`. The selected Git checkout remains
unchanged. Mutable/unaccepted presentation references, symlinks, incomplete
presentations and changed staged files refuse. Seven offline Git-history controls
exercise that separation and final artifact binding.

The deployment audit keeps `source_revision` and also records the SDK and
presentation revisions, plus the published tag commit and exact workflow revision for a stable dispatch. Record the published tag, selected stable source,
presentation commit, exact workflow revision, required-check evidence and
artifact digest before deployment; verify the resulting public audit and
actual rendered pages afterward. Source CI and previews do not authorize a
tag, package release, future API/support claim or a held publishing path.
The combined-minor gates remain separate from this documentation redesign.
