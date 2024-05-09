import fs from 'fs';
import { createHash } from 'crypto';
import { ClickHouseClient, ClickHouseClientConfigOptions, createClient } from "@clickhouse/client";
import { sql_queries, sql_sets } from './sql-queries';

export function createDatabase(clickhouse: ClickHouseClient, database: string, { engine = 'Atomic' }: { engine?: string }) {
  return clickhouse.command({
    query: `CREATE DATABASE IF NOT EXISTS ${database} ENGINE = ${engine}`,
    clickhouse_settings: {
      wait_end_of_query: 1,
    },
  });
}

export async function initializeMigrationTable(clickhouse: ClickHouseClient) {
  const q = `CREATE TABLE IF NOT EXISTS _migrations (
    uid UUID DEFAULT generateUUIDv4(),
    version UInt32,
    checksum String,
    migration_name String,
    applied_at DateTime DEFAULT now()
  )
  ENGINE = MergeTree
  ORDER BY tuple(applied_at)`;

  return clickhouse.command({
    query: q,
    clickhouse_settings: {
      wait_end_of_query: 1,
    },
  });
}

interface Migration {
  version: number;
  filename: string;
  commands: string;
}

interface CompletedMigration {
  version: number;
  checksum: string;
  migration_name: string;
}

export async function getMigrationsToApply(clickhouse: ClickHouseClient, migrations: Migration[]) {
  const alreadyAppliedMigrations = await clickhouse.query({
    query: `SELECT version, checksum, migration_name FROM _migrations ORDER BY version`,
    format: 'JSONEachRow',
  })
    .then((rz) => rz.json<CompletedMigration>())
    .then((rows) => rows.reduce((acc, row) => {
      acc[row.version] = row;
      return acc;
    }, {} as Record<number, CompletedMigration>));

  Object.values(alreadyAppliedMigrations).forEach((migration) => {
    if (!migrations.find((m) => m.version === migration.version)) {
      throw new Error(`Migration ${migration.version} has been applied but no longer exists`);
    }
  });

  const appliedMigrations = [] as Migration[];

  for (const migration of migrations) {
    const checksum = createHash('md5').update(migration.commands).digest('hex');

    if (alreadyAppliedMigrations[migration.version]) {
      // Check if migration file was not changed after apply.
      if (alreadyAppliedMigrations[migration.version].checksum !== checksum) {
        throw new Error(`A migration file should't be changed after apply. Please, restore content of the ${alreadyAppliedMigrations[migration.version].migration_name
          } migrations.`)
      }

      // Skip if a migration is already applied.
      continue;
    }
    appliedMigrations.push(migration);
  }

  return appliedMigrations;
}

export async function applyMigrations(clickhouse: ClickHouseClient, migrations: Migration[]) {
  for (const migration of migrations) {
    const checksum = createHash('md5').update(migration.commands).digest('hex');

    // Extract sql from the migration.
    const queries = sql_queries(migration.commands);
    const sets = sql_sets(migration.commands);

    for (const query of queries) {
      try {
        await clickhouse.command({
          query: query,
          clickhouse_settings: sets,
        });
      } catch (e) {
        throw new Error(
          `the migrations ${migration.filename} has an error. Please, fix it (be sure that already executed parts of the migration would not be run second time) and re-run migration script.
${(e as Error).message}`);
      }
    }

    try {
      await clickhouse.insert({
        table: '_migrations',
        values: [{ version: migration.version, checksum: checksum, migration_name: migration.filename }],
        format: 'JSONEachRow',
      });
    } catch (e: unknown) {
      throw new Error(`can't insert a data into the table _migrations: ${(e as Error).message}`);
    }
  }
}

export function getMigrationsInDirectory(directory: string): Migration[] {
  const migrations = [] as Migration[];

  fs.readdirSync(directory).forEach((filename) => {
    // Manage only .sql files.
    if (!filename.endsWith('.sql')) return;

    const version = Number(filename.split('_')[0]);
    const commands = fs.readFileSync(`${directory}/${filename}`, 'utf8');

    migrations.push({
      version,
      filename,
      commands,
    });
  });

  return migrations.sort((a, b) => a.version - b.version);
}

export async function applyMigrationsInDirectory(config: ClickHouseClientConfigOptions & { database: string }, directory: string) {
  const defaultDb = createClient({
    ...config,
    database: undefined,
  });
  await createDatabase(defaultDb, config.database, { engine: 'Atomic' });
  const targetDb = createClient(config);
  await initializeMigrationTable(targetDb);
  const migrations = getMigrationsInDirectory(directory);
  const toApply = await getMigrationsToApply(targetDb, migrations);
  if (toApply.length > 0) {
    return applyMigrations(targetDb, toApply);
  }
  return toApply.map((m) => m.filename);
}
