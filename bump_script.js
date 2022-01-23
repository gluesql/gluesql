#!/usr/bin/env node
const shell = require("shelljs");

/**
 * variable index is priority.
 */
const publishCrateList = [
  "gluesql-utils",
  "gluesql-core",
  "gluesql-test-suite",
  "gluesql_memory_storage",
  "gluesql_sled_storage",
  "gluesql-cli",
  "gluesql",
];

const publish = () => {
  publishCrateList.map((crateName) => {
    const result = shell.exec(`cargo publish -p ${crateName}`);

    if (result.code !== 0) {
      const error = result.stderr.split("\n");

      shell.echo(
        `failed publish: ${error
          .slice(error.length - 2, error.length)
          .join("\n")}`
      );
      exit();
    }
  });

  shell.echo(`success publish: ${publishCrateList}`);
};

const exit = () => shell.exit(1);

const cat = (path) => {
  const result = shell.cat(path);

  if (result.code !== 0) {
    shell.echo("Error: cat command failed");
    exit();
  }

  return result.stdout.split("\n");
};

const validateCrateList = () => {
  const crateNameListFromCat = cat("**/Cargo.toml")
    .filter((text) => text.includes("name = "))
    .map((text) => text.replaceAll(`"`, "").replace(`name = `, ""));
  publishCrateList.forEach((crateName) => {
    if (!crateNameListFromCat.includes(crateName)) {
      shell.echo(`Missing crate: ${crateName}`);
      exit();
    }
  });
};

const main = () => {
  if (!shell.which("cargo")) {
    shell.echo("command not found: cargo");
    shell.echo(`	install link: "https://www.rust-lang.org/tools/install"`);
    exit();
  }

  validateCrateList();

  publish();
  exit();
};

main();
