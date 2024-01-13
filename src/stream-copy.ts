import { Stream } from 'stream';

import { Bookmark, SourceDatabaseRowRecord, TableSyncSpec } from './types';
import { batchFetch } from './batch';
import { standardClickhouseValueMapper } from './type-mapping';

export async function synchronizeTable<T extends SourceDatabaseRowRecord, PK extends string | number>(spec: TableSyncSpec<T, PK>, bookmark: Bookmark<PK>) {
  const batcher = batchFetch(spec.getRows, spec.getBookmark);
  const stream = new Stream.Readable({ objectMode: true, read() { } });
  const insertPromise = 'insert' in spec ? spec.insert(stream) : spec.clickhouse.insert({
    table: spec.tableName,
    values: stream,
    format: 'JSONEachRow',
  });
  let rowsSynced = 0;
  let lastRow: T | undefined;
  for await (const row of batcher(bookmark, spec.pageSize || 10000)) {
    lastRow = row;
    stream.push(spec.rowMapper ? spec.rowMapper(row) : standardClickhouseValueMapper(row));
    rowsSynced++;
  }
  stream.push(null);
  await insertPromise;
  return {
    rows: rowsSynced,
    bookmark: lastRow ? spec.getBookmark(lastRow) : bookmark,
  };
}
