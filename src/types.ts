import internal from 'stream';

import { ClickHouseClient, InsertResult } from '@clickhouse/client';

export type SourceDatabaseRowRecord = Record<string, string | Date | number | boolean | null | undefined>;

export interface Bookmark {
  rowId?: string;
  rowTimestamp?: Date | null;
}

export type RowFetchFunction<T> = (bookmark: Bookmark, limit: number) => AsyncIterableIterator<T>;

interface BaseTableSyncSpec<T extends SourceDatabaseRowRecord> {
  // Define a function that gets rows from the table in a stable order of your choosing,
  // returning a maximum of `limit` rows. If you return limit rows, the caller will assume
  // there may be more, and call you again with a new bookmark
  getRows: RowFetchFunction<T>;
  getBookmark(row: T): Bookmark;
  // Defaults to 10,000 but if you want precise control over the select size, you can set it here
  pageSize?: number;
}

interface InsertTableSyncSpec<T extends SourceDatabaseRowRecord> extends BaseTableSyncSpec<T> {
  insert(stream: internal.Readable): Promise<InsertResult>;
}

interface AutoTableSyncSpec<T extends SourceDatabaseRowRecord> extends BaseTableSyncSpec<T> {
  clickhouse: ClickHouseClient;
  tableName: string;
}

export type TableSyncSpec<T extends SourceDatabaseRowRecord> = InsertTableSyncSpec<T> | AutoTableSyncSpec<T>;
