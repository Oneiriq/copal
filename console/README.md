# Console checks

One command, against a console this repository just built:

```
./console/run.sh
```

The console is generated markup, so most of what could go wrong with
it is caught by the Rust tests: `crates/copal-server/tests/console.rs`
asserts the pages render, the links resolve, every GraphQL example the
reference offers runs against the endpoint this deployment serves, and
the schema it prints parses as a schema.

What those cannot see is layout. Whether a table is cut off with no
sign that it scrolls, whether a control is too small to hit, whether a
page scrolls sideways on a laptop, and whether any of it changes under
the other theme are questions about boxes on a screen, and answering
them takes an engine that lays out boxes. That is what lives here.

Both checks have found real defects: a listing cut at a hard edge
because an overlay scrollbar reserves no space and appears only after
a reader already scrolled, and checkboxes rendering at the browser
default of 13px, smaller than anything else on the page a person has
to hit.

## What each one asks

`frame.mjs` walks every console page in both themes and asserts the
frame is whole: header, rail, main, footer and the appearance control
present, the page never scrolling sideways, nothing drawn past the
right edge of the column it lives in, and every control tall enough to
hit. A checkbox is measured by the label around it, because that is
what takes the click.

`clipping.mjs` walks the same pages at six window sizes from 1920x1080
down to 820x1180, opens every disclosure, and looks for content a
clipping ancestor is hiding with no way to reach it. Content inside a
box that scrolls is fine; content behind an edge is not.

## Running it

`run.sh` builds the server, starts it on a scratch database, runs both
checks and stops it. It needs Node and a Chromium that Playwright can
find:

```
npm --prefix console ci
npx --prefix console playwright install chromium
```

`CONSOLE_BASE` points the checks at a server that is already running,
in which case `run.sh` starts nothing. `CHROMIUM_PATH` names a browser
to use instead of the one Playwright downloaded.
