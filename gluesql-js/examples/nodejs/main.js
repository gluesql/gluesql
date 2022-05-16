const { gluesql } = require('../../gluesql.node.js');
const db = gluesql();

async function run() {
  await db.query(`
    CREATE TABLE User (id INTEGER, name TEXT);
    CREATE TABLE Device (name TEXT, userId INTEGER);
    INSERT INTO User VALUES
      (1, "glue"), (2, "sticky"), (3, "watt");
    INSERT INTO Device VALUES
      ("Phone", 1), ("Mic", 1), ("Monitor", 3),
      ("Mouse", 2), ("Touchpad", 2);
  `);

  let sql;

  sql = 'SHOW TABLES;';
  const [{ tables }] = await db.query(sql);
  console.log(`\n[Query]\n${sql}`);
  console.table(tables);

  sql = `
    SELECT
      u.name as user,
      d.name as device
    FROM User u
    JOIN Device d ON u.id = d.userId
  `.trim().replace(/[ ]{4}/g, '');
  const [{ rows }] = await db.query(sql);
  console.log(`\n[Query]\n${sql}`);
  console.table(rows);
}

run();
