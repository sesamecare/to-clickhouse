import { type Kysely, type AnyColumn, sql } from 'kysely';
import type { ClickHouseClient } from '@clickhouse/client';

import type { Bookmark, ClickhouseRowRecord, RowMapper, SourceDatabaseRowRecord } from '../types';
import { synchronizeTable } from '../stream-copy';

type HasUpdatedAt<ColName extends string> = {
  [K in ColName]: Date;
};

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
  UC extends string = 'updated_at',
>(
  db: Kysely<Schema>,
  ch: ClickHouseClient,
  bookmark: Bookmark<PKT>,
  spec: {
    from: T;
    to: string;
    pk: PK;
    timestampColumn?: UC;
    delaySeconds?: number;
    rowMapper?: RowMapper;
    optimize?: boolean;
    pageSize?: number;
  },
) {
  // Type assertion: Ensure that Schema[T] extends HasUpdatedAt
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  type AssertSchema = Schema[T] extends HasUpdatedAt<UC> ? Schema[T] : never;

  const baseQuery = db
    .selectFrom(spec.from)
    .selectAll();

  const syncResult = synchronizeTable({
    rowMapper: spec.rowMapper as (row: SourceDatabaseRowRecord) => ClickhouseRowRecord,
    getRows(bookmark, limit) {
      type TableWhere = Parameters<typeof baseQuery['where']>;
      const pkColumn = spec.pk as unknown as TableWhere[0];
      const udColumn = (spec.timestampColumn || 'updated_at') as unknown as TableWhere[0];
      let completeQuery = baseQuery
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .where(udColumn, '<', sql<any>`NOW() - INTERVAL \'1 SECOND\' * ${spec.delaySeconds === undefined ? 60 : spec.delaySeconds}`)
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
        rowTimestamp: (row as any)[spec.timestampColumn || 'updated_at'],
      };
    },
    clickhouse: ch,
    tableName: spec.to,
    pageSize: spec.pageSize,
  }, bookmark);
  if (spec.optimize !== false) {
    await ch.command({ query: `OPTIMIZE TABLE ${spec.to} FINAL` });
  }
  return syncResult;
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
    optimize?: boolean;
    rowMapper?: RowMapper;
  },
) {
  const baseQuery = db.selectFrom(spec.from).selectAll();

  const result = synchronizeTable({
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
  if (spec.optimize !== false) {
    await ch.command({ query: `OPTIMIZE TABLE ${spec.to} FINAL` });
  }
  return result;
}

/**
 * Synchronize multiple tables with simpler syntax.
 */
export class KyselySyncContext<Schema, B extends Partial<Record<keyof Schema, Bookmark<string> | Bookmark<number>>>> {
  // Log function that can be overridden
  log: (level: 'info' | 'error', message: string, meta?: Record<string, unknown>) => void = () => { };
  // The default name of the updated_at column
  updatedAtColumn = 'updated_at';
  // The default name of the primary key column based on the table name (the default strips a plural 's' from the end and adds _id)
  getDefaultPrimaryKeyColumn = (table: string) => `${table.slice(0, -1)}_id`;

  // This will be filled out with the results of the syncs in such a way that it can be used
  // as a collective bookmark for the next sync. You can also get the individual bookmarks
  // from the return value of the individual functions.
  results = {} as Record<string, Bookmark<string | number>>;

  constructor(
    private readonly db: Kysely<Schema>,
    private readonly clickhouse: ClickHouseClient,
    private readonly bookmark?: B,
  ) { }

  /**
   * Sync a table that only gets additions, no updates (or update tracking)
   */
  async forwardOnly<T extends keyof Schema & string>(table: T, opts?: {
    pk?: AnyColumn<Schema, T>,
    rowMapper?: RowMapper,
  }) {
    const { pk } = opts || {};
    return copyTable(
      this.db,
      this.clickhouse,
      (this.bookmark?.[table as keyof B] || {}) as Bookmark<string | number>,
      {
        ...opts,
        from: table,
        to: table,
        pk: (pk || this.getDefaultPrimaryKeyColumn(table)) as AnyColumn<Schema, T>,
        optimize: true,
      })
      .then((result) => {
        this.log('info', 'Copy complete', { table, rows: result.rows });
        const newBookmark = { ...result.bookmark, lastCount: result.rows };
        this.results[table as string] = newBookmark;
        return {
          table,
          bookmark: newBookmark,
        };
      })
      .catch((error) => {
        this.log('error', `Failed to copy table ${table}`, { table, error });
        throw error;
      });
  }

  /**
   * Sync a table that tracks its updates with an updated_at column
   */
  async withUpdatedAt<T extends keyof Schema & string>(table: T, opts?: {
    pk?: AnyColumn<Schema, T>;
    timestampColumn?: AnyColumn<Schema, T>;
    rowMapper?: RowMapper;
    pageSize?: number;
  }) {
    const { pk, timestampColumn } = opts || {};
    return syncTable(
      this.db,
      this.clickhouse,
      (this.bookmark?.[table as keyof B] || {}) as Bookmark<string | number>,
      {
        ...opts,
        from: table,
        to: table,
        pk: (pk || this.getDefaultPrimaryKeyColumn(table)) as AnyColumn<Schema, T>,
        optimize: true,
        timestampColumn: timestampColumn || this.updatedAtColumn as AnyColumn<Schema, T>,
      })
      .then((result) => {
        this.log('info', 'Sync complete', { table, rows: result.rows });
        const newBookmark = { ...result.bookmark, lastCount: result.rows };
        this.results[table as string] = newBookmark;
        return {
          table,
          bookmark: newBookmark,
        };
      })
      .catch((error) => {
        this.log('error', `Failed to sync table ${table}`, { table, error });
        throw error;
      });
  }
}
