/*
 * Seed BanyanDB (via the BFF) with M3-reviewable data:
 *   - Measure group `sw_metric` with 3 lifecycle stages (Hot/Warm/Cold)
 *   - Stream group `sw_logs`
 *   - Measure `cpu_usage` with entity tags host, service
 *   - Stream `app_logs` with entity tags host, service
 *   - IndexRule `by_host` (TREE)
 *   - IndexRuleBinding `metric-bind` (subject=cpu_usage, rule=by_host)
 *
 * Idempotent: if a resource already exists (409), it's skipped silently.
 *
 * Usage: BFF=http://127.0.0.1:4000 node scripts/seed-m3-review.mjs
 */

const BFF = process.env.BFF ?? 'http://127.0.0.1:4000';

async function login() {
  const res = await fetch(`${BFF}/auth/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ username: 'admin', password: 'admin', endpoint: 'http://127.0.0.1:17913' }),
  });
  if (!res.ok) throw new Error(`login failed: ${res.status} ${await res.text()}`);
  const setCookie = res.headers.get('set-cookie') ?? '';
  const cookie = setCookie.split(';')[0];
  return cookie;
}

async function api(cookie, method, path, body) {
  const res = await fetch(`${BFF}${path}`, {
    method,
    headers: { 'content-type': 'application/json', cookie },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { ok: res.ok, status: res.status, body: await res.text() };
}

async function ensure(cookie, label, method, path, body) {
  const r = await api(cookie, method, path, body);
  if (r.ok) {
    console.log(`✓ ${label} created`);
  } else if (r.status === 409 || r.status === 400) {
    console.log(`· ${label} already exists (${r.status})`);
  } else {
    console.warn(`✗ ${label} failed ${r.status}: ${r.body.slice(0, 200)}`);
  }
}

async function main() {
  const cookie = await login();
  console.log(`[seed] logged in (BFF=${BFF})`);

  // 1. Measure group with 3 lifecycle stages
  await ensure(cookie, 'measure group sw_metric (with Hot/Warm/Cold stages)', 'POST', '/api/v1/group/schema', {
    group: {
      metadata: { name: 'sw_metric' },
      catalog: 'CATALOG_MEASURE',
      resourceOpts: {
        shardNum: 2,
        segmentInterval: { unit: 'UNIT_DAY', num: 1 },
        ttl: { unit: 'UNIT_DAY', num: 30 },
        stages: [
          { name: 'hot',  shardNum: 2, segmentInterval: { unit: 'UNIT_HOUR', num: 6 },  ttl: { unit: 'UNIT_DAY', num: 3 }, nodeSelector: 'tier=hot',  close: true,  replicas: 0 },
          { name: 'warm', shardNum: 2, segmentInterval: { unit: 'UNIT_DAY', num: 1 },  ttl: { unit: 'UNIT_DAY', num: 7 }, nodeSelector: 'tier=warm', close: false, replicas: 0 },
          { name: 'cold', shardNum: 1, segmentInterval: { unit: 'UNIT_DAY', num: 7 },  ttl: { unit: 'UNIT_DAY', num:30 }, nodeSelector: 'tier=cold', close: true,  replicas: 0 },
        ],
      },
    },
  });

  // 2. Stream group
  await ensure(cookie, 'stream group sw_logs', 'POST', '/api/v1/group/schema', {
    group: {
      metadata: { name: 'sw_logs' },
      catalog: 'CATALOG_STREAM',
      resourceOpts: {
        shardNum: 2,
        segmentInterval: { unit: 'UNIT_DAY', num: 1 },
        ttl: { unit: 'UNIT_DAY', num: 7 },
      },
    },
  });

  // 3. Measure schema with entity tags
  await ensure(cookie, 'measure cpu_usage', 'POST', '/api/v1/measure/schema', {
    measure: {
      metadata: { name: 'cpu_usage', group: 'sw_metric' },
      tagFamilies: [
        { name: 'default', tags: [
          { name: 'host',    type: 'TAG_TYPE_STRING' },
          { name: 'service', type: 'TAG_TYPE_STRING' },
          { name: 'region',  type: 'TAG_TYPE_STRING' },
        ] },
      ],
      fields: [
        { name: 'value', fieldType: 'FIELD_TYPE_FLOAT', encodingMethod: 'ENCODING_METHOD_GORILLA', compressionMethod: 'COMPRESSION_METHOD_ZSTD' },
        { name: 'count', fieldType: 'FIELD_TYPE_INT',   encodingMethod: 'ENCODING_METHOD_GORILLA', compressionMethod: 'COMPRESSION_METHOD_ZSTD' },
      ],
      entity: { tagNames: ['host', 'service'] },
      interval: '30s',
    },
  });

  // 4. Stream schema
  await ensure(cookie, 'stream app_logs', 'POST', '/api/v1/stream/schema', {
    stream: {
      metadata: { name: 'app_logs', group: 'sw_logs' },
      tagFamilies: [
        { name: 'default', tags: [
          { name: 'host',    type: 'TAG_TYPE_STRING' },
          { name: 'service', type: 'TAG_TYPE_STRING' },
          { name: 'level',   type: 'TAG_TYPE_STRING' },
          { name: 'msg',     type: 'TAG_TYPE_STRING' },
        ] },
      ],
      entity: { tagNames: ['host', 'service'] },
    },
  });

  // 5. IndexRule
  await ensure(cookie, 'index rule by_host', 'POST', '/api/v1/index-rule/schema', {
    indexRule: {
      metadata: { name: 'by_host', group: 'sw_metric' },
      tags: ['host', 'service'],
      type: 'TYPE_TREE',
    },
  });

  // 6. IndexRuleBinding
  await ensure(cookie, 'index rule binding metric-bind', 'POST', '/api/v1/index-rule-binding/schema', {
    indexRuleBinding: {
      metadata: { name: 'metric-bind', group: 'sw_metric' },
      rules: ['by_host'],
      subject: { name: 'cpu_usage', catalog: 'CATALOG_MEASURE' },
      beginAt: '2026-01-01T00:00:00Z',
      expireAt: '2026-12-31T23:59:59Z',
    },
  });

  console.log('[seed] done');
}

main().catch((e) => { console.error(e); process.exit(1); });