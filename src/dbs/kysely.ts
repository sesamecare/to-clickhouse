import { type Kysely, type AnyColumn, sql } from 'kysely';
import type { ClickHouseClient } from '@clickhouse/client';

import type { Bookmark } from '../types';
import { synchronizeTable } from '../stream-copy';

interface HasUpdatedAt {
  updated_at: Date;
}

// Some types are too complicated to figure out how to match in kysely. Give up once.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TSComplex = any;

/**
 * Given a table with an updated_at column, get the latest rows ordered by updated_at and the PK,
 * to get a stable load. Note that it is important to set a delay that is long enough for all reasonable
 * transaction durations, otherwise rows could sneak in that were updated before the last sync. I think
 * for most use cases 1 minute would be more than enough.
 */
export async function syncTable<
  Schema,
  T extends keyof Schema & string,
  PK extends AnyColumn<Schema, T>,
  PKT extends string | number = Schema[T][PK] extends string | number ? Schema[T][PK] : never,
>(
  db: Kysely<Schema>,
  ch: ClickHouseClient,
  bookmark: Bookmark<PKT>,
  spec: {
    from: T;
    to: string;
    pk: PK;
    delaySeconds: number;
  },
) {
  // Type assertion: Ensure that Schema[T] extends HasUpdatedAt
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  type AssertSchema = Schema[T] extends HasUpdatedAt ? Schema[T] : never;

  const baseQuery = db
    .selectFrom(spec.from)
    .selectAll();

  return synchronizeTable({
    getRows(bookmark, limit) {
      type TableWhere = Parameters<typeof baseQuery['where']>;
      const pkColumn = spec.pk as unknown as TableWhere[0];
      const udColumn = 'updated_at' as unknown as TableWhere[0];
      let completeQuery = baseQuery
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .where(udColumn, '<', sql<any>`NOW() - INTERVAL \'1 SECOND\' * ${spec.delaySeconds}`)
      // Too complicated to figure out how to get this type to be accurate. But it is.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if (bookmark?.rowTimestamp && bookmark?.rowId) {
        completeQuery = completeQuery.where((eb) => eb.or([
          eb(udColumn, '>=', bookmark.rowTimestamp as TSComplex),
          eb.and([
            eb(udColumn, '=', bookmark.rowTimestamp as TSComplex),
            eb(pkColumn, '>', bookmark.rowId as TSComplex),
          ])
        ]));
      } else if (bookmark?.rowTimestamp) {
        completeQuery = completeQuery.where(udColumn, '>=', bookmark.rowTimestamp as TSComplex);
      }
      return completeQuery
        .orderBy(udColumn, 'asc')
        .orderBy(pkColumn, 'asc')
        .limit(limit)
        .stream();
    },
    getBookmark(row) {
      return {
        // Too complicated to figure out how to get this type to be accurate. But it is.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        rowId: (row as any)[spec.pk],
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        rowTimestamp: (row as any).updated_at,
      };
    },
    clickhouse: ch,
    tableName: spec.to,
  }, bookmark);
}

/**
 * Copy the contents of a table that is "forward only" - that is, the id column is enough
 * to get a stable load. If you do not pass a bookmark, then we will copy all the rows
 * in PK order. If you do pass a bookmark, we will copy all the rows with a PK greater
 * than the provided rowId. This allows you to handle both tables that do not update existing content
 * and tables that do.
 */
export async function copyTable<
  Schema,
  T extends keyof Schema & string,
  PK extends AnyColumn<Schema, T>,
  PKT extends string | number = Schema[T][PK] extends string | number ? Schema[T][PK] : never,
>(
  db: Kysely<Schema>,
  ch: ClickHouseClient,
  bookmark: Bookmark<PKT>,
  spec: {
    from: T;
    to: string;
    pk: PK;
  },
) {
  const baseQuery = db.selectFrom(spec.from).selectAll();

  return synchronizeTable({
    getRows(bookmark, limit) {
      type TableWhere = Parameters<typeof baseQuery['where']>;
      let completeQuery = baseQuery;
      const pkColumn = spec.pk as unknown as TableWhere[0];
      if (bookmark?.rowId) {
        completeQuery = completeQuery.where(
          pkColumn,
          '>',
          // Too complicated to figure out how to get this type to be accurate. But it is.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          bookmark.rowId as any,
        );
      }
      return completeQuery
        .orderBy(pkColumn, 'asc')
        .limit(limit)
        .stream();
    },
    getBookmark(row) {
      return {
        // Too complicated to figure out how to get this type to be accurate. But it is.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        rowId: (row as any)[spec.pk],
      };
    },
    clickhouse: ch,
    tableName: spec.to,
  }, bookmark);
}
