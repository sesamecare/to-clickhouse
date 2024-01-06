import { SourceDatabaseRowRecord, RowFetchFunction, Bookmark } from './types';

/**
 * Creates a batch-fetching generator function for retrieving rows from a database in batches.
 * This function enhances performance and efficiency by using streaming for each batch.
 *
 * @param {RowFetchFunction<T>} getRows - A function that fetches rows from the database.
 *   It takes a bookmark and a limit as arguments, returning an asynchronous iterator of rows.
 * @param {function(row: T): Bookmark} getBookmark - A function to extract a bookmark
 *   (a pointer or reference) from a row, which is used to fetch the next batch of rows.
 *
 * @returns {RowFetchFunction<T>} A generator function that fetches rows in batches, starting from
 *   a given bookmark up to a specified limit. The function streams rows one by one to manage memory
 *   efficiently, especially useful for large datasets.
 *
 * Usage:
 * The returned function is an async generator function that can be used with a for-await-of loop.
 * It retrieves rows in batches up to the specified limit. After exhausting a batch, it fetches the
 * next batch starting from the last retrieved row (using the bookmark). This continues until the number
 * of rows in a batch is less than the limit, indicating the end of available data.
 *
 * Example:
 * for await (const row of batchFetchFunction(bookmark, limit)) {
 *   // Process each row
 * }
 */export function batchFetch<T extends SourceDatabaseRowRecord, PK extends string | number>(getRows: RowFetchFunction<T, PK>, getBookmark: (row: T) => Bookmark<PK>): RowFetchFunction<T, PK> {
  return async function* (bookmark, limit) {
    let rowsThisRun = 0;
    let lastRow: T | undefined;
    do {
      for await (const row of getRows(lastRow ? getBookmark(lastRow) : bookmark, limit)) {
        rowsThisRun++;
        lastRow = row;
        yield row;
      }
    } while (rowsThisRun === limit && rowsThisRun > 0);
  };
}
