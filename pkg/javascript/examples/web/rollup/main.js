import { gluesql } from 'gluesql/gluesql.rollup';

async function run() {
  const db = await gluesql();
  await db.loadIndexedDB();

  const result = await db.query(`
    DROP TABLE IF EXISTS Foo, Bar;
    CREATE TABLE Foo (id INTEGER, name TEXT);
    CREATE TABLE Bar (bar_id INTEGER) ENGINE = indexedDB;
    INSERT INTO Foo VALUES (1, 'hello'), (2, 'world');
    INSERT INTO Bar VALUES (10), (20);
    SELECT *, id as wow_id FROM Foo JOIN Bar;
  `);

  for (const item of result) {
    const node = document.createElement('code');

    node.innerHTML = `
      type: ${item.type}
      <br>
      ${item.affected ? `affected: ${item.affected}` : ''}
      ${item.rows ? `rows: ${JSON.stringify(item.rows)}` : ''}
    `;

    console.log(item);
    document.querySelector('#box').append(node);
  }
}

window.onload = run;
