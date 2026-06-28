// Dev-mode wrapper for the Canopy BFF.
//
// Watches `canopy/web/dist/` and respawns the BFF whenever the production
// web bundle is rebuilt. This is the missing piece: the BFF caches
// in-memory references to the bundle HTML + asset paths at startup, so
// rebuilding `web/dist/` without restarting the BFF served stale references.
//
// Usage:
//   node scripts/dev-bff.mjs
//
// Press Ctrl+C once to stop everything.

import { spawn } from 'node:child_process';
import { watch, existsSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(__dirname, '..', '..');
const DIST_DIR = resolve(REPO_ROOT, 'canopy', 'web', 'dist');
const SERVER_CWD = resolve(REPO_ROOT, 'canopy', 'server');

let child = null;
let restarting = false;
let pendingRestart = false;
let userStopping = false;

function startBff() {
  console.log(`[dev-bff] starting BFF (port 4000, watching ${DIST_DIR})`);
  child = spawn('node', ['--loader', 'ts-node/esm', 'src/index.ts'], {
    cwd: SERVER_CWD,
    env: {
      ...process.env,
      SESSION_SECRET: process.env.SESSION_SECRET ?? 'canopy-e2e-test-secret-32chars!!',
      CANOPY_DEV_NOAUTH: process.env.CANOPY_DEV_NOAUTH ?? 'true',
      BANYANDB_TARGET: process.env.BANYANDB_TARGET ?? 'http://127.0.0.1:17913',
      PORT: '4000',
      LOG_LEVEL: process.env.LOG_LEVEL ?? 'warn',
    },
    stdio: 'inherit',
  });
  child.on('exit', (code, signal) => {
    if (userStopping) {
      console.log(`[dev-bff] BFF exited (${signal}) — user-requested stop, exiting wrapper`);
      process.exit(0);
    }
    if (signal === 'SIGINT' || signal === 'SIGTERM') {
      // Killed by the watcher-triggered restart path; the once('exit', ...)
      // handler in restartBff is responsible for respawning. Do nothing here.
      return;
    }
    if (!restarting) {
      console.log(`[dev-bff] BFF exited unexpectedly (code=${code}); respawning in 1s`);
      setTimeout(() => startBff(), 1000);
    }
  });
}

function restartBff(reason) {
  if (restarting) { pendingRestart = true; return; }
  restarting = true;
  console.log(`[dev-bff] ${reason} — restarting BFF`);
  if (child) {
    child.once('exit', () => {
      restarting = false;
      startBff();
      if (pendingRestart) { pendingRestart = false; /* subsequent change already queued by the watcher */ }
    });
    child.kill('SIGTERM');
  } else {
    restarting = false;
    startBff();
  }
}

if (!existsSync(DIST_DIR)) {
  console.error(`[dev-bff] ${DIST_DIR} does not exist — run \`npm run -w web build\` first`);
  process.exit(1);
}

startBff();

// Watch dist for rebuilds. Use recursive + a debounce so multi-file writes
// (Vite writes index.html + assets/*.js + assets/*.css) trigger one restart.
let timer = null;
function notifyChange(filename) {
  if (!filename) return;
  if (timer) clearTimeout(timer);
  timer = setTimeout(() => restartBff(`dist changed: ${filename}`), 200);
}

try {
  watch(DIST_DIR, { recursive: true }, (_event, filename) => notifyChange(filename));
  console.log('[dev-bff] watching dist/ for changes');
} catch (e) {
  console.error(`[dev-bff] failed to watch ${DIST_DIR}: ${e.message}`);
  console.error('[dev-bff] (recursive watch is required; Node 20+ on Linux is supported)');
}

process.on('SIGINT', () => {
  userStopping = true;
  console.log('[dev-bff] SIGINT — stopping BFF and exiting wrapper');
  if (child) child.kill('SIGTERM');
  // The child.on('exit') handler will call process.exit(0) once userStopping is true.
  setTimeout(() => process.exit(0), 2000).unref();
});
process.on('SIGTERM', () => {
  userStopping = true;
  if (child) child.kill('SIGTERM');
  setTimeout(() => process.exit(0), 2000).unref();
});
