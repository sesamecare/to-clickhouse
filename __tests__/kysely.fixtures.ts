import { Kysely, PostgresDialect } from 'kysely';
import Cursor from 'pg-cursor';

import { pgPool } from './db.fixtures';
import { DB } from './generated/database';

export function kyselyDb(db: string) {
  const pool = pgPool(db);
  const dialect = new PostgresDialect({
    cursor: Cursor,
    pool,
  });
  return { pool, db: new Kysely<DB>({ dialect }) }
}
