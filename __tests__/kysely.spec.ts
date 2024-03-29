import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ClickHouseClient } from '@clickhouse/client';
import { Kysely } from 'kysely';

import { KyselySyncContext, copyTable, syncTable } from '../src/dbs/kysely';

import { createChDb, createPgDb } from './db.fixtures';
import { kyselyDb } from './kysely.fixtures';
import { DB } from './generated/database';

const TESTPGDB = 'kytest';

describe('simple kysely interface', () => {
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

  test('should sync to clickhouse', async () => {
    const detail = await copyTable(db, ch, {}, {
      from: 'address_types',
      to: 'address_types',
      pk: 'address_type_id',
    });
    expect(detail.rows).toBe(2);

    const indSpec = {
      from: 'individuals',
      to: 'individuals',
      pk: 'individual_id',
      delaySeconds: 0,
    } as const;
    const ind = await syncTable(db, ch, {}, indSpec);
    expect(ind.rows, 'Table should copy 3 rows').toBe(3);

    await new Promise((resolve) => setTimeout(resolve, 250));
    await db.updateTable('individuals')
      .set({ favorite_color: 'green' })
      .where('individual_id', '=', '1')
      .execute();

    const upd = await syncTable(db, ch, ind.bookmark, indSpec);
    expect(upd.rows, 'Table should still copy 3 rows because updated_at >= should match previous rows too').toBe(3);

    await new Promise((resolve) => setTimeout(resolve, 250));
    await db.updateTable('individuals')
      .set({ favorite_color: 'green' })
      .where('individual_id', '=', '2')
      .execute();

    const upd2 = await syncTable(db, ch, upd.bookmark, indSpec);
    expect(upd2.rows, 'Copy 2 rows after second update').toBe(2);
  });

  test('should sync to clickhouse with simplified syntax', async () => {
    await db.insertInto('individuals')
      .columns(['updated_at'])
      .values([{
        updated_at: new Date('2024-01-01T00:00:00.000Z'),
      }])
      .execute();
    let context = new KyselySyncContext(db, ch, {});
    await Promise.all([
      context.forwardOnly('address_types'),
      context.withUpdatedAt('individuals'),
    ]);
    expect(context.results).toMatchInlineSnapshot(`
      {
        "address_types": {
          "lastCount": 2,
          "rowId": 2,
        },
        "individuals": {
          "lastCount": 4,
          "rowId": "4",
          "rowTimestamp": "2024-01-01T00:00:00.000",
        },
      }
    `);
    context = new KyselySyncContext(db, ch, context.results);
    const secondPass = await Promise.all([
      context.forwardOnly('address_types'),
      context.withUpdatedAt('individuals'),
    ]);
    expect(secondPass).toMatchInlineSnapshot(`
      [
        {
          "bookmark": {
            "lastCount": 0,
            "rowId": 2,
          },
          "table": "address_types",
        },
        {
          "bookmark": {
            "lastCount": 1,
            "rowId": "4",
            "rowTimestamp": "2024-01-01T00:00:00.000",
          },
          "table": "individuals",
        },
      ]
    `);
  });
});
