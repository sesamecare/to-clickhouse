import { SourceDatabaseRowRecord } from './types';

export function standardClickhouseValueMapper(row: SourceDatabaseRowRecord) {
  Object.entries(row).forEach(([key, value]) => {
    if (value instanceof Date) {
      row[key] = value.toISOString().replace('Z', '');
    }
  });
  return row;
}
