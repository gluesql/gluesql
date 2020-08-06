const gluesql = import("../pkg/gluesql.js");

async function run() {
  const { Glue } = await gluesql;

  const sqls = [`
    CREATE TABLE Test (
        id INTEGER,
        msg TEXT,
        flag BOOLEAN
    );`,
    "INSERT INTO Test (id, msg, flag) VALUES (1, \"Hello GlueSQL\", false);",
    "INSERT INTO Test (id, msg, flag) VALUES (2, \"Good Luck!\", true);",
    "SELECT * FROM Test;",
    "SLEE * WIEJF;",
    "SELECT * FROM Something;",
  ];

  const db = new Glue();

  const stringify = v => JSON.stringify(v, null, ' ');

  for (sql of sqls) {
    print({
      color: 'white',
      content: `[RUN] ${sql}`,
    });

    let message;

    try {
      let result = db.execute(sql)[0];

      message = {
        color: '#3090F2',
        content: `[RES] ${stringify(result)}`,
      };
    } catch (error) {
      message = {
        color: '#f72145',
        content: `[ERR] ${stringify(error)}`,
      };
    }

    print(message);
    print(' ');
    print(' ');
  }
}

function print(message) {
  const code = document.createElement('code');

  if (message) {
    const { content, color } = message;

    code.textContent = content;
    code.style.color = color;
  }

  document.querySelector('#box').appendChild(code);
}

run();
