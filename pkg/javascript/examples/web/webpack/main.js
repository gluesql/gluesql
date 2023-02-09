import { gluesql } from 'gluesql';

async function run() {
  const db = await gluesql();

  const result = await db.query(`
    CREATE TABLE Foo (id INTEGER, name TEXT);
    INSERT INTO Foo VALUES (1, 'hello'), (2, 'world');
    SELECT *, id as wow_id FROM Foo;
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
