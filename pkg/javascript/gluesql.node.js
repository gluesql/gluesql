const { Glue } = require('./dist/nodejs/gluesql_js.js');

function gluesql() {
  return new Glue();
}

module.exports = { gluesql };
