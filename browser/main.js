const DUCKDB_WASM_VERSION = '1.30.0';

const runBtn = document.getElementById('runBtn');
const statusEl = document.getElementById('status');
const logEl = document.getElementById('log');
const resultEl = document.getElementById('result');
const sqlEl = document.getElementById('sql');
const parquetUrlEl = document.getElementById('parquetUrl');

let dbPromise;

function setStatus(text, ok = true) {
  statusEl.textContent = text;
  statusEl.className = ok ? 'ok' : 'err';
}

function log(msg) {
  logEl.textContent += `${msg}\n`;
}

async function getDb() {
  if (dbPromise) return dbPromise;
  dbPromise = (async () => {
    setStatus('initializing duckdb-wasm...');
    const duckdb = await import(
      `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_WASM_VERSION}/+esm`
    );
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }),
    );

    const worker = new Worker(workerUrl);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    setStatus('duckdb-wasm ready');
    return db;
  })().catch((err) => {
    dbPromise = undefined;
    throw err;
  });
  return dbPromise;
}

function renderTable(rows) {
  resultEl.innerHTML = '';
  if (!rows.length) {
    resultEl.textContent = '(no rows)';
    return;
  }

  const cols = Object.keys(rows[0]);
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');

  for (const col of cols) {
    const th = document.createElement('th');
    th.textContent = col;
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  for (const row of rows) {
    const tr = document.createElement('tr');
    for (const col of cols) {
      const td = document.createElement('td');
      td.textContent = String(row[col]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  resultEl.appendChild(table);
}

runBtn.addEventListener('click', async () => {
  logEl.textContent = '';
  resultEl.innerHTML = '';
  let conn;

  try {
    const db = await getDb();
    conn = await db.connect();
    const parquetUrl = parquetUrlEl.value.trim();
    const sql = sqlEl.value.replaceAll('$PARQUET_URL', parquetUrl);

    log(`SQL: ${sql}`);
    setStatus('running query...');

    const table = await conn.query(sql);
    const rows = table.toArray();
    renderTable(rows);

    setStatus(`done (${rows.length} rows)`);
  } catch (err) {
    const msg = err?.message ?? String(err);
    setStatus('failed', false);
    log(msg);
  } finally {
    if (conn) {
      try {
        await conn.close();
      } catch (_err) {
        // ignore close errors in UI teardown path
      }
    }
  }
});

// Enable only after the click handler is ready; import errors appear in the UI.
runBtn.disabled = false;
