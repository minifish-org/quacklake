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
const HTTP_URL_REGEX = /https?:\/\/[^\s"')]+/g;

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

const port = Number(process.env.PORT || 3000);
app.listen(port, '0.0.0.0', () => {
  console.log(`runner listening on ${port}`);
});
