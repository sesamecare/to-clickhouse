import fs from 'fs';
import path from 'path';

import { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ClickHouseClient, createClient } from '@clickhouse/client';
import Cursor from 'pg-cursor';
import { Kysely, PostgresDialect } from 'kysely';

import { synchronizeTable } from '../src/index';

import { DB } from './generated/database';

const TESTPGDB = 'chtest';

describe('move tables from postgres to clickhouse', () => {
  let ch: ClickHouseClient;
  let db: Kysely<DB>;

  beforeEach(async () => {
    // Create the postgres db and load the schema
    let pool = new Pool({
      database: 'postgres',
      host: process.env.PGHOST || 'localhost',
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'postgres',
    });
    await pool.query(`DROP DATABASE IF EXISTS ${TESTPGDB}`);
    await pool.query(`CREATE DATABASE ${TESTPGDB}`);
    pool = new Pool({
      database: TESTPGDB,
      host: process.env.PGHOST || 'localhost',
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'postgres',
    });
    await pool.query(fs.readFileSync(path.resolve(__dirname, 'db/pg.sql'), 'utf8'));
    const dialect = new PostgresDialect({
      cursor: Cursor,
      pool,
    });
    db = new Kysely<DB>({ dialect });

    // Create the clickhouse db and load the schema
    ch = createClient({
      database: 'default',
      host: process.env.CHHOST || 'http://localhost:8123',
      username: process.env.CHUSERNAME || 'default',
      password: process.env.CHPASSWORD || '',
    });
    await ch.command({ query: `DROP DATABASE IF EXISTS ${TESTPGDB}` });
    await ch.command({ query: `CREATE DATABASE ${TESTPGDB}` });
    ch = createClient({
      database: 'chtest',
      host: process.env.CHHOST || 'http://localhost:8123',
      username: process.env.CHUSERNAME || 'default',
      password: process.env.CHPASSWORD || '',
    });
    await ch.command({ query: fs.readFileSync(path.resolve(__dirname, 'db/clickhouse.sql'), 'utf8') });
  });

  afterEach(async () => {
    await Promise.all([db.destroy(), ch.close()]).catch((error) => {
      console.error('Shutdown failed', error);
      throw error;
    });
  });

  test('synchronize a table', async () => {
    const detail = await synchronizeTable({
      getRows(bookmark, limit) {
        return db
          .selectFrom('address_types')
          .selectAll()
          .where((eb) => bookmark?.rowId ? eb('address_type_id', '>', Number(bookmark.rowId)) : eb.val(true))
          .orderBy('address_type_id')
          .limit(limit)
          .stream();
      },
      getBookmark(row) {
        return {
          rowId: String(row.address_type_id),
          rowTimestamp: row.created_at as Date,
        };
      },
      insert(stream) {
        return ch.insert({
          table: 'identity__address_types',
          values: stream,
          format: 'JSONEachRow',
        });
      },
    }, {});
    expect(detail.rows).toBe(2);
  });

  test('synchronize a table with simple insert', async () => {
    const detail = await synchronizeTable({
      getRows(bookmark, limit) {
        return db
          .selectFrom('address_types')
          .selectAll()
          .where((eb) => bookmark?.rowId ? eb('address_type_id', '>', Number(bookmark.rowId)) : eb.val(true))
          .orderBy('address_type_id')
          .limit(limit)
          .stream();
      },
      getBookmark(row) {
        return {
          rowId: String(row.address_type_id),
          rowTimestamp: row.created_at as Date,
        };
      },
      clickhouse: ch,
      tableName: 'identity__address_types',
    }, {});
    expect(detail.rows).toBe(2);
  });
});
