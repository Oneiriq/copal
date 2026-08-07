// Launching a browser the same way for both checks, so neither
// carries a path to one machine.
import { chromium } from 'playwright-core';

export const BASE = process.env.CONSOLE_BASE || 'http://127.0.0.1:8099';
export const AUTH = {
  username: process.env.CONSOLE_USER || 'operator',
  password: process.env.CONSOLE_ADMIN_TOKEN || 'local-admin-token',
};
export const TENANT = process.env.CONSOLE_TENANT || 'demo';

export function pages() {
  const t = `${BASE}/admin/console/t/${TENANT}`;
  return [
    `${BASE}/admin/console`,
    t,
    `${t}/reference`,
    `${t}/r/files`,
    `${t}/r/webhooks`,
    `${t}/r/events`,
    `${t}/r/runs`,
    `${t}/q/search`,
    `${t}/q/file_text`,
  ];
}

export async function browser() {
  const executablePath = process.env.CHROMIUM_PATH || undefined;
  return chromium.launch(executablePath ? { executablePath } : {});
}
