import test from 'ava';
import { Glue } from '../index.js';

/** Fresh Glue instance for each test (memory storage — fully isolated). */
function glue() {
  return new Glue();
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

test('CREATE TABLE returns correct payload', async (t) => {
  const g = glue();
  const result = await g.query('CREATE TABLE users (id INTEGER, name TEXT)');
  t.is(result.length, 1);
  t.is(result[0].type, 'CREATE TABLE');
});

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

test('INSERT returns affected count', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE items (id INTEGER, name TEXT)');
  const result = await g.query("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')");
  t.is(result[0].type, 'INSERT');
  t.is(result[0].affected, 2);
});

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

test('SELECT returns rows with correct shape', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)');
  await g.query("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)");

  const [payload] = await g.query('SELECT * FROM products ORDER BY id');
  t.is(payload.type, 'SELECT');
  t.is(payload.rows.length, 2);

  // column values — verify JS types, not just presence
  t.is(payload.rows[0].id, 1);           // INTEGER → JS number
  t.is(payload.rows[0].name, 'Widget');  // TEXT → JS string
  t.is(payload.rows[0].price, 9.99);     // FLOAT → JS number (not string)
  t.is(payload.rows[1].id, 2);
  t.is(payload.rows[1].name, 'Gadget');
});

test('SELECT on empty table returns empty rows array', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE empty (id INTEGER)');
  const [payload] = await g.query('SELECT * FROM empty');
  t.is(payload.type, 'SELECT');
  t.deepEqual(payload.rows, []);
});

test('SELECT NULL column value comes through as JS null', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE nullable (id INTEGER, note TEXT)');
  await g.query('INSERT INTO nullable VALUES (1, NULL)');

  const [payload] = await g.query('SELECT * FROM nullable');
  t.is(payload.rows[0].id, 1);
  t.is(payload.rows[0].note, null);
});

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

test('UPDATE returns affected count', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE scores (id INTEGER, score INTEGER)');
  await g.query('INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30)');

  const [payload] = await g.query('UPDATE scores SET score = score + 5 WHERE score < 25');
  t.is(payload.type, 'UPDATE');
  t.is(payload.affected, 2);
});

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

test('DELETE returns affected count', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE logs (id INTEGER)');
  await g.query('INSERT INTO logs VALUES (1), (2), (3)');

  const [payload] = await g.query('DELETE FROM logs WHERE id <= 2');
  t.is(payload.type, 'DELETE');
  t.is(payload.affected, 2);
});

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

test('ALTER TABLE returns correct payload', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE alter_test (id INTEGER)');
  const [payload] = await g.query('ALTER TABLE alter_test ADD COLUMN note TEXT');
  t.is(payload.type, 'ALTER TABLE');
});

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

test('DROP TABLE returns affected:1 and table is gone', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE tmp (x INTEGER)');
  await g.query('INSERT INTO tmp VALUES (1), (2)');

  const [payload] = await g.query('DROP TABLE IF EXISTS tmp');
  t.is(payload.type, 'DROP TABLE');
  t.is(payload.affected, 1); // number of tables dropped, not rows

  // table must no longer exist
  await t.throwsAsync(() => g.query('SELECT * FROM tmp'), { instanceOf: Error });
});

// ---------------------------------------------------------------------------
// CREATE INDEX / DROP INDEX
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// SHOW TABLES
// ---------------------------------------------------------------------------

test('SHOW TABLES lists created tables', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE alpha (x INTEGER)');
  await g.query('CREATE TABLE beta (y TEXT)');

  const [payload] = await g.query('SHOW TABLES');
  t.is(payload.type, 'SHOW TABLES');
  t.true(payload.tables.includes('alpha'));
  t.true(payload.tables.includes('beta'));
});

// ---------------------------------------------------------------------------
// SHOW COLUMNS
// ---------------------------------------------------------------------------

test('SHOW COLUMNS returns column names and SQL types', async (t) => {
  const g = glue();
  await g.query('CREATE TABLE schema_test (id INTEGER, label TEXT)');

  const [payload] = await g.query('SHOW COLUMNS FROM schema_test');
  t.is(payload.type, 'SHOW COLUMNS');

  const byName = Object.fromEntries(payload.columns.map((c) => [c.name, c.type]));
  t.is(byName.id, 'INT');
  t.is(byName.label, 'TEXT');
});

// ---------------------------------------------------------------------------
// SHOW VERSION
// ---------------------------------------------------------------------------

test('SHOW VERSION returns type and non-empty version string', async (t) => {
  const g = glue();
  const [payload] = await g.query('SHOW VERSION');
  t.is(payload.type, 'SHOW VERSION');
  t.is(typeof payload.version, 'string');
  t.true(payload.version.length > 0);
});

// ---------------------------------------------------------------------------
// Multiple statements in a single query() call
// ---------------------------------------------------------------------------

test('multiple statements return one payload per statement', async (t) => {
  const g = glue();
  const result = await g.query(`
    CREATE TABLE multi (id INTEGER, val TEXT);
    INSERT INTO multi VALUES (1, 'x');
    SELECT * FROM multi;
  `);

  t.is(result.length, 3);
  t.is(result[0].type, 'CREATE TABLE');
  t.is(result[1].type, 'INSERT');
  t.is(result[2].type, 'SELECT');
  t.is(result[2].rows[0].val, 'x');
});


// ---------------------------------------------------------------------------
// Error handling — each stage of the pipeline propagates to a JS Error
// ---------------------------------------------------------------------------

test('parse error rejects with an Error', async (t) => {
  const g = glue();
  await t.throwsAsync(() => g.query('THIS IS NOT SQL'), { instanceOf: Error });
});

test('execute error (missing table) rejects with an Error', async (t) => {
  const g = glue();
  await t.throwsAsync(() => g.query('SELECT * FROM no_such_table'), { instanceOf: Error });
});

// ---------------------------------------------------------------------------
// Isolation between instances
// ---------------------------------------------------------------------------

test('two Glue instances have independent storage', async (t) => {
  const a = glue();
  const b = glue();

  await a.query('CREATE TABLE private (x INTEGER)');
  await a.query('INSERT INTO private VALUES (1)');

  await t.throwsAsync(() => b.query('SELECT * FROM private'), { instanceOf: Error });
});
