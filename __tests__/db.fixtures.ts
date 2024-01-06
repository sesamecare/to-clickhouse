import fs from 'fs';
import path from 'path';

import { Pool } from 'pg';
import { createClient } from '@clickhouse/client';

export function pgPool(db: string) {
  return new Pool({
    database: db,
    host: process.env.PGHOST || 'localhost',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
  });
}

export async function createPgDb(db: string) {
  // Create the postgres db and load the schema
  let pool = pgPool('postgres');
  await pool.query(`DROP DATABASE IF EXISTS ${db}`);
  await pool.query(`CREATE DATABASE ${db}`);
  await pool.end();

  pool = new Pool({
    database: db,
    host: process.env.PGHOST || 'localhost',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
  });
  await pool.query(fs.readFileSync(path.resolve(__dirname, 'db/pg.sql'), 'utf8'));
  await pool.end();
}

export function chdb(db: string) {
  return createClient({
    database: db,
    host: process.env.CHHOST || 'http://localhost:8123',
    username: process.env.CHUSERNAME || 'default',
    password: process.env.CHPASSWORD || '',
  });
}

export async function createChDb(db: string) {
  // Create the clickhouse db and load the schema
  const ch = chdb('default');
  await ch.command({ query: `DROP DATABASE IF EXISTS ${db}` });
  await ch.command({ query: `CREATE DATABASE ${db}` });

  const ch2 = chdb(db);
  const cmds = fs.readFileSync(path.resolve(__dirname, 'db/clickhouse.sql'), 'utf8').split('---');
  await cmds.reduce((prev, cmd) => prev.then(() => ch2.command({ query: cmd }).then(() => undefined)), Promise.resolve(undefined));
  return ch2;
}

if (require.main === module) {
  createPgDb('chtest').catch((error) => {
    console.error(error);
    process.exit(-1);
  });
}
