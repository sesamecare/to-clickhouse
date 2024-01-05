import fs from 'fs';
import path from 'path';

import { Pool } from 'pg';

import { TESTPGDB } from './constants';

export async function createPgDb() {
  // Create the postgres db and load the schema
  let pool = new Pool({
    database: 'postgres',
    host: process.env.PGHOST || 'localhost',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
  });
  await pool.query(`DROP DATABASE IF EXISTS ${TESTPGDB}`);
  await pool.query(`CREATE DATABASE ${TESTPGDB}`);
  await pool.end();

  pool = new Pool({
    database: TESTPGDB,
    host: process.env.PGHOST || 'localhost',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
  });
  await pool.query(fs.readFileSync(path.resolve(__dirname, 'db/pg.sql'), 'utf8'));
  await pool.end();
}

if (require.main === module) {
  createPgDb().catch((error) => {
    console.error(error);
    process.exit(-1);
  });
}
