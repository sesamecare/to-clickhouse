import path from 'path';

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ClickHouseClient } from '@clickhouse/client';
import { Kysely } from 'kysely';

import { getMigrationsInDirectory, getMigrationsToApply, synchronizeTable } from '../src/index';

import { DB } from './generated/database';
import { createChDb, createPgDb } from './db.fixtures';
import { kyselyDb } from './kysely.fixtures';

const TESTPGDB = 'chtest';

describe('move tables from postgres to clickhouse', () => {
  let ch: ClickHouseClient;
  let db: Kysely<DB>;

  beforeEach(async () => {
    await createPgDb(TESTPGDB);
    ({ db } = kyselyDb(TESTPGDB));
    ch = await createChDb(TESTPGDB);
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

  test('migrations are recorded', async () => {
    const migrations = getMigrationsInDirectory(path.resolve(__dirname, 'migrations'));
    const toApply = await getMigrationsToApply(ch, migrations);
    expect(toApply.length).toBe(0);
  });
});
