import test from 'node:test';
import assert from 'node:assert/strict';

process.env.RUNNER_DISABLE_LISTEN = '1';

const {
  buildHttpAliasPlan,
  parseExtensionNames,
  readResponseBytesWithLimit,
  parsePositiveInt,
  parseCsvList,
  isUrlAllowed,
  isRunnerTokenAuthorized,
  shouldRejectForConcurrency,
  validateProductionSettings,
} = await import('./index.js');

test('parseExtensionNames filters invalid values and normalizes case', () => {
  const parsed = parseExtensionNames('FTS, vss, bad-name, ;drop, sqlite_scanner');
  assert.deepEqual(parsed, ['fts', 'vss', 'sqlite_scanner']);
});

test('buildHttpAliasPlan rewrites all matching URLs', () => {
  const sql = 'select * from read_parquet("http://g/one.parquet") union all select * from read_parquet("http://g/two.parquet")';
  const urls = ['http://g/one.parquet', 'http://g/two.parquet'];
  const plan = buildHttpAliasPlan(sql, urls);

  assert.deepEqual(plan.aliases, [
    { url: 'http://g/one.parquet', alias: 'remote_0.parquet' },
    { url: 'http://g/two.parquet', alias: 'remote_1.parquet' },
  ]);
  assert.equal(plan.rewrittenSql.includes('http://g/one.parquet'), false);
  assert.equal(plan.rewrittenSql.includes('http://g/two.parquet'), false);
  assert.equal(plan.rewrittenSql.includes('remote_0.parquet'), true);
  assert.equal(plan.rewrittenSql.includes('remote_1.parquet'), true);
});

test('parsePositiveInt falls back for invalid values', () => {
  assert.equal(parsePositiveInt('512', 100), 512);
  assert.equal(parsePositiveInt('0', 100), 100);
  assert.equal(parsePositiveInt('-7', 100), 100);
  assert.equal(parsePositiveInt('abc', 100), 100);
});

test('readResponseBytesWithLimit rejects large content-length eagerly', async () => {
  const response = new Response(new Uint8Array([1, 2, 3]), {
    headers: { 'content-length': '100' },
  });
  await assert.rejects(
    () => readResponseBytesWithLimit(response, 10, 'too big'),
    /too big/,
  );
});

test('parseCsvList trims and drops empties', () => {
  assert.deepEqual(parseCsvList('a, b ,, c '), ['a', 'b', 'c']);
});

test('isUrlAllowed enforces prefix allow list', () => {
  const prefixes = ['http://gateway:8080/objects/', 'http://localhost:8080/objects/'];
  assert.equal(
    isUrlAllowed('http://gateway:8080/objects/lakehouse/demo/events.parquet', prefixes),
    true,
  );
  assert.equal(
    isUrlAllowed('http://169.254.169.254/latest/meta-data', prefixes),
    false,
  );
});

test('isRunnerTokenAuthorized accepts empty expected token', () => {
  assert.equal(isRunnerTokenAuthorized([], undefined), true);
});

test('isRunnerTokenAuthorized enforces exact token match', () => {
  assert.equal(isRunnerTokenAuthorized(['abc'], 'abc'), true);
  assert.equal(isRunnerTokenAuthorized(['abc'], 'xyz'), false);
  assert.equal(isRunnerTokenAuthorized(['abc'], undefined), false);
  assert.equal(isRunnerTokenAuthorized(['old', 'new'], 'new'), true);
});

test('shouldRejectForConcurrency blocks at max', () => {
  assert.equal(shouldRejectForConcurrency(0, 2), false);
  assert.equal(shouldRejectForConcurrency(1, 2), false);
  assert.equal(shouldRejectForConcurrency(2, 2), true);
});

test('validateProductionSettings enforces required security config', () => {
  assert.doesNotThrow(() => validateProductionSettings('development', [], []));
  assert.doesNotThrow(() => validateProductionSettings('production', ['t'], ['http://gateway:8080/objects/']));
  assert.throws(() => validateProductionSettings('production', [], ['http://gateway:8080/objects/']));
  assert.throws(() => validateProductionSettings('production', ['t'], []));
});
