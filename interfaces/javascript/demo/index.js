const gluesql = import("../pkg/gluesql.js");

async function run() {
  const { Glue } = await gluesql;

  const sqls = [`
    CREATE TABLE Test (
        id INTEGER,
        msg TEXT,
        flag BOOLEAN
    )`,
    "INSERT INTO Test (id, msg, flag) VALUES (1, \"Hello GlueSQL\", false)",
    "INSERT INTO Test (id, msg, flag) VALUES (2, \"Good Luck!\", true)",
    "SELECT * FROM Test",
    "SELECT * FROM Something",
  ];

  const db = new Glue();

  for (sql of sqls) {
    console.log(`[RUN] ${sql}`);

    let result = db.execute(sql);
    result = Object.values(result[0])[0];

    console.log('>', result);
  }
}

run();
