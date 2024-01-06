# to-clickhouse

@sesamecare-oss/to-clickhouse is a set of utilities meant to make it easier to synchronize data between a relational store such as Postgres and a Clickhouse database.

## "Fact" tables

Low row count tables such as "address types" can be synchronized using a simple copy strategy. Given a Clickhouse table like so:

```sql
CREATE TABLE identity__address_types (
    address_type_id Int32,
    name String,
    created_at DateTime64
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY address_type_id;
```

...this module can copy that to Clickhouse from Postgres like so:

```typescript
await synchronizeTable({
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
    }, {})
```

## Kysely

We have custom implentnations for Kysely that make things even easier. To copy a fact table from a Kysely DB (including nice autocomplete):

```typescript
import { copyTable } from '@sesamecare-oss/to-clickhouse/kysely';

await copyTable(db, ch, {}, {
  from: 'address_types',
  to: 'identity__address_types',
  pk: 'address_type_id',
});
```