import internal from 'stream';

import { ClickHouseClient, InsertResult } from '@clickhouse/client';

export type SourceDatabaseRowRecord = Record<string, unknown>;

export type ClickhouseRowRecord = Record<string, unknown>;

export interface Bookmark<PK extends string | number> {
  rowId?: PK;
  rowTimestamp?: Date | null;
}

export type RowFetchFunction<T, PK extends string | number> = (bookmark: Bookmark<PK>, limit: number) => AsyncIterableIterator<T>;

interface BaseTableSyncSpec<T extends SourceDatabaseRowRecord, PK extends string | number> {
  // Define a function that gets rows from the table in a stable order of your choosing,
  // returning a maximum of `limit` rows. If you return limit rows, the caller will assume
  // there may be more, and call you again with a new bookmark
  getRows: RowFetchFunction<T, PK>;
  getBookmark(row: T): Bookmark<PK>;
  // Defaults to 10,000 but if you want precise control over the select size, you can set it here
  pageSize?: number;
  rowMapper?: (row: T) => ClickhouseRowRecord;
}

interface InsertTableSyncSpec<T extends SourceDatabaseRowRecord, PK extends string | number> extends BaseTableSyncSpec<T, PK> {
  insert(stream: internal.Readable): Promise<InsertResult>;
}

interface AutoTableSyncSpec<T extends SourceDatabaseRowRecord, PK extends string | number> extends BaseTableSyncSpec<T, PK> {
  clickhouse: ClickHouseClient;
  tableName: string;
}

export type TableSyncSpec<T extends SourceDatabaseRowRecord, PK extends string | number> = InsertTableSyncSpec<T, PK> | AutoTableSyncSpec<T, PK>;
