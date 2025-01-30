import { dbSentinel, SENTINEL_JOB_COMPLETED, SENTINEL_JOB_FAILED } from './index';

async function main() {
  dbSentinel.initialize({
    db: {
      user: 'postgres',
      host: 'localhost',
      database: 'testdb',
      password: 'zy@1996',
      port: 5432,
    },
    queue: {
      redisHost: '127.0.0.1',
      redisPort: 6379,
      concurrency: 2,
      maxQueueSize: 10,
    },
    cleanup: {
      tableTTLHours: 1, 
      cleanupIntervalMs: 300000, 
    },
  });

  try {
    const jobId = await dbSentinel.createQueryJob(
      'SELECT generate_series(1, 3) as id, $1 as note',
      ['Test Param']
    );

    dbSentinel.once(SENTINEL_JOB_COMPLETED(jobId), async (info) => {
      console.log('Job completed:', info);
      const tableName = await dbSentinel.getTableNameForJob(jobId);
      if (!tableName) {
        console.log('No table found for job:', jobId);
        return;
      }
      const poolClient = (dbSentinel as any).dbPool.connect
        ? await (dbSentinel as any).dbPool.connect()
        : null;
      if (!poolClient) {
        console.log('Unable to connect to DB for final query.');
        return;
      }
      try {
        const result = await poolClient.query(`SELECT * FROM "${tableName}"`);
        console.log('Result from dynamic table:', result.rows);
      } finally {
        poolClient.release();
      }
    });

    dbSentinel.once(SENTINEL_JOB_FAILED(jobId), (error) => {
      console.error('Job failed:', error);
    });
  } catch (error) {
    console.error('Error creating job:', error);
  }

  setTimeout(async () => {
    console.log('Shutting down middleware...');
    await dbSentinel.shutdown();
    process.exit(0);
  }, 20000);
}

main();
