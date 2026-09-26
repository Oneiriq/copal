// Every console page, in both themes: is the frame whole, and is
// everything on it reachable?
//
// The Rust tests assert the pages render and their links resolve.
// These are the questions that need an engine laying out boxes.
import { BASE, AUTH, pages, browser } from './shared.mjs';

const chrome = await browser();
const problems = [];

for (const theme of ['dark', 'light']) {
  const context = await chrome.newContext({
    httpCredentials: AUTH,
    viewport: { width: 1600, height: 1000 },
    colorScheme: theme,
  });
  const page = await context.newPage();
  page.on('pageerror', (e) => problems.push(`${theme} script error: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() === 'error') problems.push(`${theme} console: ${m.text()}`);
  });

  for (const url of pages()) {
    const response = await page.goto(url, { waitUntil: 'domcontentloaded' });
    const short = url.replace(BASE, '');
    if (!response.ok()) {
      problems.push(`${theme} ${short}: HTTP ${response.status()}`);
      continue;
    }
    const found = await page.evaluate(() => {
      const seen = [];

      // The frame every page is meant to share.
      for (const selector of ['header', '.frame', '.rail', 'main', 'footer']) {
        if (!document.querySelector(selector)) seen.push(`missing ${selector}`);
      }
      if (!document.querySelector('button.icon[onclick*="kayakTheme"]')) {
        seen.push('missing the appearance control');
      }

      // The page never scrolls sideways; wide things scroll in a box.
      const wide = document.documentElement.scrollWidth - window.innerWidth;
      if (wide > 1) seen.push(`body scrolls sideways by ${wide}px`);

      // Nothing draws past the right edge of its own column.
      const main = document.querySelector('main');
      const room = main.getBoundingClientRect().right;
      const boxes = 'h1, h2, p, .card, .definition, .scroll, form.inline, .toolbar';
      for (const el of main.querySelectorAll(boxes)) {
        const box = el.getBoundingClientRect();
        if (box.width > 0 && box.right > room + 1) {
          seen.push(
            `${el.className || el.tagName} overflows its column by ${Math.round(box.right - room)}px`,
          );
        }
      }

      // Text has to be readable against what is behind it.
      const body = getComputedStyle(document.body);
      if (body.color === body.backgroundColor) seen.push('text matches its ground');

      // Controls a person has to hit. A checkbox is measured by the
      // label around it, because that is what takes the click.
      const tick = 'input[type=checkbox], input[type=radio]';
      const controls = 'button, select, input:not([type=hidden]), a.here';
      for (const el of document.querySelectorAll(controls)) {
        const target = el.matches(tick) ? el.closest('label') || el : el;
        const box = target.getBoundingClientRect();
        if (box.height > 0 && box.height < 20) {
          seen.push(`${el.tagName}.${el.className} is only ${Math.round(box.height)}px tall`);
        }
      }
      return [...new Set(seen)];
    });
    for (const problem of found) problems.push(`${theme} ${short}: ${problem}`);
  }
  await context.close();
}

await chrome.close();
if (problems.length) {
  console.log(problems.join('\n'));
  process.exit(1);
}
console.log(`frame: clean across ${pages().length} pages x 2 themes`);
