const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT,
});


module.exports = {
  query: (text, params) => pool.query(text, params),
};


//AWS RDS
// const { Pool } = require("pg");
// require("dotenv").config();

// const pool = new Pool({
//   host: process.env.DB_HOST,
//   user: process.env.DB_USER,
//   password: process.env.DB_PASSWORD,
//   database: process.env.DB_NAME,
//   port: process.env.DB_PORT || 5432,
//   ssl: {
//     rejectUnauthorized: false, // Safe for RDS; don't use in sensitive production unless using verified certs
//   },
// });

// module.exports = {
//   query: (text, params) => pool.query(text, params),
// };
