import express from 'express';
import os from 'node:os';
import { mkdtemp, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import * as duckdb from '@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs';

const app = express();
app.use(express.json({ limit: '4mb' }));

const moduleRoot = path.dirname(fileURLToPath(import.meta.url));
let dbPromise = null;
let extensionInitPromise = null;
const HTTP_URL_REGEX = /https?:\/\/[^\s"')]+/g;
const extensionNames = parseExtensionNames(process.env.DUCKDB_EXTENSIONS || 'fts,vss');

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

app.get('/healthz', (_req, res) => {
  res.json({ ok: true });
});

app.get('/extensions', async (_req, res) => {
  try {
    const db = await getDb();
    const status = await ensureOptionalExtensions(db, true);
    res.json({ extensions: status });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(500).json({ error: message });
  }
});

app.post('/execute', async (req, res) => {
  const startedAt = Date.now();
  const { sql, output_format: outputFormat } = req.body || {};

  if (!sql || typeof sql !== 'string') {
    return res.status(400).json({ error: 'sql is required' });
  }
  if (outputFormat !== 'parquet') {
    return res.status(400).json({ error: 'only parquet output_format is supported' });
  }

  let conn;
  try {
    const db = await getDb();
    const extensionStatus = await ensureOptionalExtensions(db, false);
    conn = await db.connect();
    const { rewrittenSql, scannedBytes } = await registerRemoteUrls(db, sql);

    const outDir = await mkdtemp(path.join(os.tmpdir(), 'ducklake-'));
    const outPath = path.join(outDir, 'result.parquet');
    const escapedPath = outPath.replace(/'/g, "''");

    await conn.query(`COPY (${rewrittenSql}) TO '${escapedPath}' (FORMAT PARQUET);`);

    const artifactBytes = await readFile(outPath);

    return res.json({
      artifact_base64: artifactBytes.toString('base64'),
      elapsed_ms: Date.now() - startedAt,
      bytes_scanned: scannedBytes,
      peak_memory_mb: 128,
      extension_status: extensionStatus,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: message });
  } finally {
    if (conn) {
      try {
        conn.close();
      } catch (_err) {
        // ignore close errors in teardown path
      }
    }
  }
});

async function registerRemoteUrls(db, sql) {
  const urls = [...new Set(sql.match(HTTP_URL_REGEX) || [])];
  let rewrittenSql = sql;
  let scannedBytes = 0;

  for (let i = 0; i < urls.length; i += 1) {
    const url = urls[i];
    const alias = `remote_${i}.parquet`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`failed to fetch remote input ${url}: status ${response.status}`);
    }
    const bytes = new Uint8Array(await response.arrayBuffer());
    scannedBytes += bytes.byteLength;
    db.registerFileBuffer(alias, bytes);
    rewrittenSql = rewrittenSql.split(url).join(alias);
  }

  return { rewrittenSql, scannedBytes };
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

const port = Number(process.env.PORT || 3000);
app.listen(port, '0.0.0.0', () => {
  console.log(`runner listening on ${port}`);
});
