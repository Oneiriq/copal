// Is anything cut off with no way to reach it?
//
// A table wider than its box is fine, because the box scrolls. A box
// that hides content behind a hard edge is not, and the two look
// identical in a screenshot. This asks the browser which is which, at
// the window sizes people actually use.
import { BASE, AUTH, pages, browser } from './shared.mjs';

const SIZES = [
  [1920, 1080],
  [1600, 1000],
  [1366, 768],
  [1280, 800],
  [1024, 768],
  [820, 1180],
];

const chrome = await browser();
const found = [];

for (const [width, height] of SIZES) {
  const context = await chrome.newContext({
    httpCredentials: AUTH,
    viewport: { width, height },
  });
  const page = await context.newPage();

  for (const url of pages()) {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    // Closed content cannot be seen to be cut.
    await page.evaluate(() => {
      document.querySelectorAll('details').forEach((d) => (d.open = true));
    });
    const hits = await page.evaluate(() => {
      const out = [];
      const label = (el) =>
        el.tagName.toLowerCase() +
        (el.className && typeof el.className === 'string'
          ? '.' + el.className.trim().split(/\s+/).join('.')
          : '') +
        (el.id ? '#' + el.id : '');

      // A box that clips, holding more than it shows, offering no way
      // to reach the rest.
      for (const el of document.querySelectorAll('*')) {
        const style = getComputedStyle(el);
        const clipsDown = style.overflowY === 'hidden' || style.overflowY === 'clip';
        const clipsAcross = style.overflowX === 'hidden' || style.overflowX === 'clip';
        if (clipsDown && el.scrollHeight > el.clientHeight + 1) {
          out.push(`${label(el)} hides ${el.scrollHeight - el.clientHeight}px below its bottom`);
        }
        if (clipsAcross && el.scrollWidth > el.clientWidth + 1) {
          out.push(`${label(el)} hides ${el.scrollWidth - el.clientWidth}px past its right`);
        }
      }

      // And the page itself never scrolls sideways.
      const over = document.documentElement.scrollWidth - document.documentElement.clientWidth;
      if (over > 1) out.push(`the page scrolls sideways by ${over}px`);
      return [...new Set(out)];
    });
    for (const hit of hits) {
      found.push(`${width}x${height} ${url.replace(BASE, '')}: ${hit}`);
    }
  }
  await context.close();
}

await chrome.close();
if (found.length) {
  console.log(found.join('\n'));
  process.exit(1);
}
console.log(`clipping: nothing hidden across ${SIZES.length} window sizes`);
