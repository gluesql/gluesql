const { Glue } = require('./dist_nodejs/gluesql_js.js');

function gluesql() {
  return new Glue();
}

module.exports = { gluesql };
