import { Pool, PoolClient } from 'pg';
import Queue, { Job } from 'bull';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';

export interface MiddlewareConfig {
  db: {
    user: string;
    host: string;
    database: string;
    password: string;
    port: number;
  };
  queue?: {
    redisHost?: string;
    redisPort?: number;
    concurrency?: number;
    maxQueueSize?: number;
  };
  cleanup?: {
    tableTTLHours?: number;
    cleanupIntervalMs?: number;
  };
}

export const SENTINEL_JOB_COMPLETED = (jobId: string) => `job-completed:${jobId}`;
export const SENTINEL_JOB_FAILED = (jobId: string) => `job-failed:${jobId}`;

/**
 * Maps Postgres data type IDs to appropriate column types.
 * This is a partial mapping. Additional mappings can be added if needed.
 */
function mapPostgresType(pgTypeId: number): string {
  switch (pgTypeId) {
    case 20: // BIGINT
    case 21: // SMALLINT
    case 23: // INTEGER
      return 'INTEGER';
    case 700: // REAL
    case 701: // DOUBLE PRECISION
      return 'DOUBLE PRECISION';
    case 1700: // NUMERIC
      return 'NUMERIC';
    case 16: // BOOLEAN
      return 'BOOLEAN';
    case 1082: // DATE
      return 'DATE';
    case 1114: // TIMESTAMP WITHOUT TIME ZONE
    case 1184: // TIMESTAMP WITH TIME ZONE
      return 'TIMESTAMP';
    case 114: // JSON
    case 3802: // JSONB
      return 'JSONB';
    case 25: // TEXT
    case 1043: // VARCHAR
    default:
      return 'TEXT';
  }
}

/**
 * Validates that a query is a SELECT statement.
 * Blocks semicolons or certain keywords for security.
 */
function validateSelectQuery(query: string) {
  const normalized = query.trim().toUpperCase();
  if (!normalized.startsWith('SELECT ')) {
    throw new Error('Only SELECT queries are allowed.');
  }
  if (normalized.includes(';')) {
    throw new Error('Semicolons are not permitted for security reasons.');
  }
  const forbiddenRegex = /\b(DROP|UPDATE|DELETE|INSERT|ALTER)\b/i;
  if (forbiddenRegex.test(query)) {
    throw new Error('Forbidden SQL keyword detected in the query.');
  }
}

class DBSentinel extends EventEmitter {
  private dbPool!: Pool;
  private queryQueue!: Queue.Queue;
  private concurrency = 1;
  private maxQueueSize = 100;
  private cleanupIntervalMs = 3600000;
  private tableTTLHours = 24;
  private cleanupTimer: NodeJS.Timeout | null = null;
  private initialized = false;

  public initialize(config: MiddlewareConfig): void {
    if (this.initialized) {
      throw new Error('DBSentinel is already initialized.');
    }
    this.initialized = true;

    this.dbPool = new Pool({
      user: config.db.user,
      host: config.db.host,
      database: config.db.database,
      password: config.db.password,
      port: config.db.port,
    });

    const redisHost = config.queue?.redisHost ?? '127.0.0.1';
    const redisPort = config.queue?.redisPort ?? 6379;
    this.queryQueue = new Queue('queryQueue', {
      redis: {
        host: redisHost,
        port: redisPort,
      },
    });

    this.concurrency = config.queue?.concurrency ?? this.concurrency;
    this.maxQueueSize = config.queue?.maxQueueSize ?? this.maxQueueSize;
    if (config.cleanup?.tableTTLHours !== undefined) {
      this.tableTTLHours = config.cleanup.tableTTLHours;
    }
    if (config.cleanup?.cleanupIntervalMs !== undefined) {
      this.cleanupIntervalMs = config.cleanup.cleanupIntervalMs;
    }

    this.initQueueProcessor();
    this.setupCleanupTimer();
  }

  public async shutdown(): Promise<void> {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    await this.queryQueue.close();
    await this.dbPool.end();
  }

  public async createQueryJob(query: string, params: any[] = []): Promise<string> {
    const waitingCount = await this.queryQueue.getWaitingCount();
    if (waitingCount >= this.maxQueueSize) {
      throw new Error(`Maximum queue size of ${this.maxQueueSize} reached.`);
    }

    validateSelectQuery(query);

    const jobId = crypto.randomUUID();

    try {
      await this.queryQueue.add(
        { jobId, query, params },
        {
          removeOnComplete: true,
          removeOnFail: false,
        }
      );
    } catch (err) {
      throw new Error(`Unable to enqueue job: ${(err as Error).message}`);
    }

    return jobId;
  }

  public async getTableNameForJob(jobId: string): Promise<string | null> {
    try {
      const result = await this.dbPool.query(
        `SELECT table_name FROM job_metadata WHERE job_id = $1`,
        [jobId]
      );
      if (result.rows.length > 0) {
        return result.rows[0].table_name;
      }
    } catch (err) {
      throw new Error(`Error retrieving table name: ${(err as Error).message}`);
    }
    return null;
  }

  private initQueueProcessor(): void {
    this.queryQueue.process(this.concurrency, async (job: Job) => {
      return this.handleJob(job);
    });
  }

  private async handleJob(job: Job) {
    const { jobId, query, params } = job.data;
    const client = await this.dbPool.connect();
    let tableName = '';

    try {
      const metaQuery = `${query} LIMIT 0`;
      const metaResult = await client.query(metaQuery, params);
      const fields = metaResult.fields;
      tableName = `query_results_${jobId.replace(/-/g, '_')}`;

      await client.query(`
        CREATE TABLE IF NOT EXISTS job_metadata (
          job_id TEXT PRIMARY KEY,
          table_name TEXT NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
      `);

      const columnsSql = fields
        .map((field) => {
          const columnType = mapPostgresType(field.dataTypeID);
          return `"${field.name}" ${columnType}`;
        })
        .join(', ');

      const createTableSql = `
        CREATE TABLE "${tableName}" (
          ${columnsSql}
        )
      `;
      await client.query(createTableSql);

      await client.query(
        `INSERT INTO job_metadata (job_id, table_name) VALUES ($1, $2)`,
        [jobId, tableName]
      );

      const dataResult = await client.query(query, params);
      const rows = dataResult.rows;

      for (const row of rows) {
        const rowObj = row as Record<string, any>;
        const colNames = Object.keys(rowObj);
        const placeholders = colNames.map((_, i) => `$${i + 1}`).join(', ');
        const insertSql = `
          INSERT INTO "${tableName}" (${colNames
          .map((c) => `"${c}"`)
          .join(', ')})
          VALUES (${placeholders})
        `;
        const values = colNames.map((c) =>
            rowObj[c] !== null && rowObj[c] !== undefined ? String(rowObj[c]) : null
        );
        await client.query(insertSql, values);
      }

      this.emit(SENTINEL_JOB_COMPLETED(jobId), { jobId, tableName });
      return { tableName };
    } catch (error) {
      this.emit(SENTINEL_JOB_FAILED(jobId), error);
      throw error;
    } finally {
      client.release();
    }
  }

  private setupCleanupTimer(): void {
    this.cleanupTimer = setInterval(() => {
      this.cleanupOldTables().catch((err) => {
        console.error('Error during table cleanup:', err);
      });
    }, this.cleanupIntervalMs);
  }

  private async cleanupOldTables(): Promise<void> {
    const client = await this.dbPool.connect();
    try {
      const oldJobsSql = `
        SELECT job_id, table_name
        FROM job_metadata
        WHERE created_at < NOW() - INTERVAL '${this.tableTTLHours} hour'
      `;
      const result = await client.query(oldJobsSql);

      for (const row of result.rows) {
        const { job_id, table_name } = row;
        try {
          await client.query(`DROP TABLE IF EXISTS "${table_name}"`);
          await client.query(`DELETE FROM job_metadata WHERE job_id = $1`, [job_id]);
        } catch (dropError) {
          console.error('Error dropping table:', table_name, dropError);
        }
      }
    } finally {
      client.release();
    }
  }
}

export const dbSentinel = new DBSentinel();
