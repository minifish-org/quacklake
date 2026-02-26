import http from 'node:http';
import os from 'node:os';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import * as duckdb from '@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs';

const MAX_REQUEST_BODY_BYTES = 4 * 1024 * 1024;

const moduleRoot = path.dirname(fileURLToPath(import.meta.url));
let dbPromise = null;
let extensionInitPromise = null;
const HTTP_URL_REGEX = /https?:\/\/[^\s"')]+/g;
const extensionNames = parseExtensionNames(process.env.DUCKDB_EXTENSIONS || 'fts,vss');
const maxInputBytes = parsePositiveInt(process.env.RUNNER_MAX_INPUT_BYTES, 268_435_456);
const runnerAuthTokens = parseCsvList(
  process.env.RUNNER_AUTH_TOKENS || process.env.RUNNER_AUTH_TOKEN || '',
);
const maxConcurrentQueries = parsePositiveInt(process.env.RUNNER_MAX_CONCURRENT_QUERIES, 4);
const appEnv = String(process.env.APP_ENV || 'development');
const allowedUrlPrefixes = parseCsvList(
  process.env.RUNNER_ALLOWED_URL_PREFIXES
    || 'http://gateway:8080/objects/,http://localhost:8080/objects/,http://127.0.0.1:8080/objects/',
);
let inFlightQueries = 0;

validateProductionSettings(appEnv, runnerAuthTokens, allowedUrlPrefixes);

async function getDb() {
  if (dbPromise) return dbPromise;
  dbPromise = (async () => {
    const bundles = {
      mvp: {
        mainModule: path.join(moduleRoot, 'node_modules', '@duckdb', 'duckdb-wasm', 'dist', 'duckdb-mvp.wasm'),
        mainWorker: path.join(moduleRoot, 'node_modules', '@duckdb', 'duckdb-wasm', 'dist', 'duckdb-node-mvp.worker.cjs'),
      },
      eh: {
        mainModule: path.join(moduleRoot, 'node_modules', '@duckdb', 'duckdb-wasm', 'dist', 'duckdb-eh.wasm'),
        mainWorker: path.join(moduleRoot, 'node_modules', '@duckdb', 'duckdb-wasm', 'dist', 'duckdb-node-eh.worker.cjs'),
      },
    };

    const db = await duckdb.createDuckDB(bundles, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
    await db.instantiate();
    await db.open({});
    return db;
  })();
  return dbPromise;
}

async function handleExecute(req, res) {
  if (!isRunnerTokenAuthorized(runnerAuthTokens, getHeader(req.headers, 'x-runner-token'))) {
    return sendJson(res, 401, { error: 'unauthorized runner token' });
  }
  if (shouldRejectForConcurrency(inFlightQueries, maxConcurrentQueries)) {
    return sendJson(res, 429, { error: 'too many concurrent queries' });
  }

  const startedAt = Date.now();
  const body = await parseJsonBody(req);
  const { sql, output_format: outputFormat } = body || {};

  if (!sql || typeof sql !== 'string') {
    return sendJson(res, 400, { error: 'sql is required' });
  }
  if (outputFormat !== 'parquet') {
    return sendJson(res, 400, { error: 'only parquet output_format is supported' });
  }

  let conn;
  let outDir;
  inFlightQueries += 1;
  try {
    const db = await getDb();
    const extensionStatus = await ensureOptionalExtensions(db, false);
    conn = await db.connect();
    const { rewrittenSql, scannedBytes } = await registerRemoteUrls(db, sql);

    outDir = await mkdtemp(path.join(os.tmpdir(), 'ducklake-'));
    const outPath = path.join(outDir, 'result.parquet');
    const escapedPath = outPath.replace(/'/g, "''");

    await conn.query(`COPY (${rewrittenSql}) TO '${escapedPath}' (FORMAT PARQUET);`);

    const artifactBytes = await readFile(outPath);

    return sendJson(res, 200, {
      artifact_base64: artifactBytes.toString('base64'),
      elapsed_ms: Date.now() - startedAt,
      bytes_scanned: scannedBytes,
      peak_memory_mb: 128,
      extension_status: extensionStatus,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return sendJson(res, 500, { error: message });
  } finally {
    if (conn) {
      try {
        conn.close();
      } catch (_err) {
        // ignore close errors in teardown path
      }
    }
    if (outDir) {
      try {
        await rm(outDir, { recursive: true, force: true });
      } catch (_err) {
        // ignore cleanup errors in teardown path
      }
    }
    inFlightQueries = Math.max(0, inFlightQueries - 1);
  }
}

async function registerRemoteUrls(db, sql) {
  const urls = [...new Set(sql.match(HTTP_URL_REGEX) || [])];
  const { rewrittenSql: initialSql, aliases } = buildHttpAliasPlan(sql, urls);
  let rewrittenSql = initialSql;
  let scannedBytes = 0;

  for (const { url, alias } of aliases) {
    if (!isUrlAllowed(url, allowedUrlPrefixes)) {
      throw new Error(`remote input URL is not allowed: ${url}`);
    }
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`failed to fetch remote input ${url}: status ${response.status}`);
    }
    const bytes = await readResponseBytesWithLimit(
      response,
      maxInputBytes - scannedBytes,
      `input byte budget exceeded while downloading ${url}`,
    );
    scannedBytes += bytes.byteLength;
    db.registerFileBuffer(alias, bytes);
  }

  return { rewrittenSql, scannedBytes };
}

function buildHttpAliasPlan(sql, urls) {
  let rewrittenSql = sql;
  const aliases = urls.map((url, i) => ({
    url,
    alias: `remote_${i}.parquet`,
  }));

  for (const { url, alias } of aliases) {
    rewrittenSql = rewrittenSql.split(url).join(alias);
  }

  return { rewrittenSql, aliases };
}

async function ensureOptionalExtensions(db, forceRefresh) {
  if (extensionNames.length === 0) {
    return {};
  }
  if (!extensionInitPromise || forceRefresh) {
    extensionInitPromise = loadExtensions(db, extensionNames);
  }
  return extensionInitPromise;
}

async function loadExtensions(db, names) {
  let conn;
  const status = Object.fromEntries(
    names.map((name) => [
      name,
      {
        available: false,
        installed: false,
        loaded: false,
        install_error: null,
        load_error: null,
      },
    ]),
  );

  try {
    conn = await db.connect();
    const availableRows = await queryExtensionRows(conn, names);
    for (const row of availableRows) {
      const name = row.extension_name;
      if (!status[name]) continue;
      status[name].available = true;
      status[name].installed = Boolean(row.installed);
      status[name].loaded = Boolean(row.loaded);
    }

    for (const name of names) {
      if (!status[name].available) {
        status[name].install_error = 'extension not available in this DuckDB runtime';
        continue;
      }

      if (!status[name].installed) {
        try {
          await conn.query(`INSTALL ${name};`);
        } catch (err) {
          status[name].install_error = err instanceof Error ? err.message : String(err);
        }
      }

      try {
        await conn.query(`LOAD ${name};`);
      } catch (err) {
        status[name].load_error = err instanceof Error ? err.message : String(err);
      }
    }

    const refreshedRows = await queryExtensionRows(conn, names);
    for (const row of refreshedRows) {
      const name = row.extension_name;
      if (!status[name]) continue;
      status[name].installed = Boolean(row.installed);
      status[name].loaded = Boolean(row.loaded);
    }
  } finally {
    if (conn) {
      try {
        conn.close();
      } catch (_err) {
        // ignore close errors in teardown path
      }
    }
  }

  return status;
}

async function queryExtensionRows(conn, names) {
  const inList = names.map((name) => `'${name}'`).join(',');
  const table = await conn.query(
    `select extension_name, installed, loaded from duckdb_extensions() where extension_name in (${inList})`,
  );
  if (typeof table.toArray !== 'function') {
    return [];
  }
  return table.toArray().map((row) => ({
    extension_name: String(row.extension_name),
    installed: Boolean(row.installed),
    loaded: Boolean(row.loaded),
  }));
}

function parseExtensionNames(rawValue) {
  return rawValue
    .split(',')
    .map((name) => name.trim().toLowerCase())
    .filter((name) => name.length > 0)
    .filter((name) => /^[a-z_][a-z0-9_]*$/.test(name));
}

async function readResponseBytesWithLimit(response, remainingBudget, overBudgetMessage) {
  if (remainingBudget <= 0) {
    throw new Error(overBudgetMessage);
  }

  const contentLengthHeader = response.headers.get('content-length');
  if (contentLengthHeader) {
    const declared = Number(contentLengthHeader);
    if (Number.isFinite(declared) && declared > remainingBudget) {
      throw new Error(overBudgetMessage);
    }
  }

  if (!response.body || typeof response.body.getReader !== 'function') {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > remainingBudget) {
      throw new Error(overBudgetMessage);
    }
    return bytes;
  }

  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
    total += chunk.byteLength;
    if (total > remainingBudget) {
      throw new Error(overBudgetMessage);
    }
    chunks.push(chunk);
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

function parsePositiveInt(rawValue, fallback) {
  const parsed = Number(rawValue);
  if (Number.isInteger(parsed) && parsed > 0) {
    return parsed;
  }
  return fallback;
}

function parseCsvList(rawValue) {
  return String(rawValue || '')
    .split(',')
    .map((v) => v.trim())
    .filter((v) => v.length > 0);
}

function isUrlAllowed(url, allowedPrefixes) {
  return allowedPrefixes.some((prefix) => url.startsWith(prefix));
}

function isRunnerTokenAuthorized(expectedTokens, providedToken) {
  if (!expectedTokens.length) {
    return true;
  }
  return expectedTokens.includes(String(providedToken || ''));
}

function shouldRejectForConcurrency(current, max) {
  return current >= max;
}

function validateProductionSettings(envName, authTokens, urlPrefixes) {
  if (envName !== 'production') return;
  if (!authTokens.length) {
    throw new Error('production mode requires RUNNER_AUTH_TOKENS or RUNNER_AUTH_TOKEN');
  }
  if (!urlPrefixes.length) {
    throw new Error('production mode requires RUNNER_ALLOWED_URL_PREFIXES');
  }
}

const port = Number(process.env.PORT || 3000);
if (process.env.RUNNER_DISABLE_LISTEN !== '1') {
  const server = http.createServer((req, res) => {
    routeRequest(req, res).catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      sendJson(res, 500, { error: message });
    });
  });
  server.listen(port, '0.0.0.0', () => {
    console.log(`runner listening on ${port}`);
  });
}

async function routeRequest(req, res) {
  const method = req.method || 'GET';
  const url = req.url || '/';

  if (method === 'GET' && url === '/healthz') {
    sendJson(res, 200, { ok: true });
    return;
  }

  if (method === 'GET' && url === '/extensions') {
    try {
      const db = await getDb();
      const status = await ensureOptionalExtensions(db, true);
      sendJson(res, 200, { extensions: status });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      sendJson(res, 500, { error: message });
    }
    return;
  }

  if (method === 'POST' && url === '/execute') {
    await handleExecute(req, res);
    return;
  }

  sendJson(res, 404, { error: 'not found' });
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.statusCode = statusCode;
  res.setHeader('content-type', 'application/json');
  res.setHeader('content-length', Buffer.byteLength(body));
  res.end(body);
}

function getHeader(headers, name) {
  const v = headers[name.toLowerCase()];
  if (Array.isArray(v)) return v[0];
  return v;
}

async function parseJsonBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_REQUEST_BODY_BYTES) {
      throw new Error('request body too large');
    }
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf8');
  if (!raw) return {};
  return JSON.parse(raw);
}

export {
  buildHttpAliasPlan,
  parseExtensionNames,
  readResponseBytesWithLimit,
  parsePositiveInt,
  parseCsvList,
  isUrlAllowed,
  isRunnerTokenAuthorized,
  shouldRejectForConcurrency,
  validateProductionSettings,
};
