#!/usr/bin/env node

/**
 * ╔═══════════════════════════════════════════════════════════════════════╗
 * ║          BullMQ & Redis Tester — Universal CLI Tool                 ║
 * ╚═══════════════════════════════════════════════════════════════════════╝
 *
 * Auto-discovers ALL BullMQ queues from Redis and runs comprehensive
 * health checks. Works on any project using BullMQ — no configuration needed.
 *
 * Usage:
 *   node index.js                 # Run all tests
 *   node index.js --redis-only    # Redis connectivity + cache only
 *   node index.js --queues-only   # BullMQ queue inspection only
 *   node index.js --roundtrip     # Full roundtrip: add job → consume → verify
 *   node index.js --host 1.2.3.4  # Custom Redis host
 *   node index.js --port 6380     # Custom Redis port
 *   node index.js --password xxx  # Redis password
 *   node index.js --db 2          # Redis DB number
 */

const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const chalk = require('chalk');

// ─── CLI Argument Parsing ────────────────────────────────────────────────────

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(name);
  return idx !== -1 && idx + 1 < args.length ? args[idx + 1] : null;
};
const hasFlag = (name) => args.includes(name);

const REDIS_HOST = getArg('--host') || process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(getArg('--port') || process.env.REDIS_PORT || '6379', 10);
const REDIS_PASSWORD = getArg('--password') || process.env.REDIS_PASSWORD || undefined;
const REDIS_DB = parseInt(getArg('--db') || process.env.REDIS_DB || '0', 10);
const BULLMQ_PREFIX = getArg('--prefix') || 'bull';
const REDIS_ONLY = hasFlag('--redis-only');
const QUEUES_ONLY = hasFlag('--queues-only');
const ROUNDTRIP = hasFlag('--roundtrip');
const HELP = hasFlag('--help') || hasFlag('-h');

if (HELP) {
  console.log(`
${chalk.bold.cyan('BullMQ & Redis Tester')} — Universal CLI Tool

${chalk.yellow('Usage:')}
  node index.js [options]

${chalk.yellow('Options:')}
  --redis-only       Test Redis connectivity and cache ops only
  --queues-only      Test BullMQ queue health only
  --roundtrip        Full roundtrip test (add job → consume → verify)
  --host <host>      Redis host (default: 127.0.0.1 or REDIS_HOST env)
  --port <port>      Redis port (default: 6379 or REDIS_PORT env)
  --password <pass>  Redis password (default: none or REDIS_PASSWORD env)
  --db <number>      Redis DB index (default: 0 or REDIS_DB env)
  --prefix <prefix>  BullMQ key prefix (default: bull)
  --help, -h         Show this help message

${chalk.yellow('Environment variables:')}
  REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB

${chalk.yellow('Examples:')}
  node index.js                                  # Test local Redis
  node index.js --host 192.168.1.50 --port 6380  # Remote Redis
  node index.js --queues-only                    # Only check queues
  node index.js --roundtrip                      # Full lifecycle test
`);
  process.exit(0);
}

// ─── Connection Configs ──────────────────────────────────────────────────────

const REDIS_CONNECTION = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  ...(REDIS_PASSWORD ? { password: REDIS_PASSWORD } : {}),
  db: REDIS_DB,
};

const BULLMQ_CONNECTION = {
  ...REDIS_CONNECTION,
  maxRetriesPerRequest: null,
  enableOfflineQueue: true,
};

// ─── Formatting Helpers ──────────────────────────────────────────────────────

const PASS = chalk.green.bold('✓ PASS');
const FAIL = chalk.red.bold('✗ FAIL');
const WARN = chalk.yellow.bold('⚠ WARN');
const INFO = chalk.blue('ℹ');

const divider = (title) => {
  const line = '─'.repeat(64);
  console.log(`\n${chalk.cyan(line)}`);
  console.log(chalk.cyan.bold(`  ${title}`));
  console.log(chalk.cyan(line));
};

const pad = (str, len = 28) => String(str).padEnd(len);

let totalPass = 0;
let totalFail = 0;
let totalWarn = 0;

const pass = (msg) => { totalPass++; console.log(`  ${PASS}  ${msg}`); };
const fail = (msg) => { totalFail++; console.log(`  ${FAIL}  ${msg}`); };
const warn = (msg) => { totalWarn++; console.log(`  ${WARN}  ${msg}`); };
const info = (msg) => { console.log(`  ${INFO}  ${chalk.gray(msg)}`); };

function formatUptime(seconds) {
  if (seconds < 0) seconds = 0;
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  const parts = [];
  if (d) parts.push(`${d}d`);
  if (h) parts.push(`${h}h`);
  if (m) parts.push(`${m}m`);
  parts.push(`${s}s`);
  return parts.join(' ');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Queue Auto-Discovery ────────────────────────────────────────────────────

async function discoverQueues() {
  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();

    const queueNames = new Set();
    let cursor = '0';

    // Scan for all bull:*:meta keys — each represents a distinct queue
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:meta`, 'COUNT', 500);
      cursor = nextCursor;
      for (const key of keys) {
        // key format: bull:<queueName>:meta
        const parts = key.split(':');
        if (parts.length >= 3) {
          queueNames.add(parts[1]);
        }
      }
    } while (cursor !== '0');

    // Fallback: also scan for bull:*:id keys in case meta doesn't exist
    cursor = '0';
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:id`, 'COUNT', 500);
      cursor = nextCursor;
      for (const key of keys) {
        const parts = key.split(':');
        if (parts.length >= 3) {
          queueNames.add(parts[1]);
        }
      }
    } while (cursor !== '0');

    // Extra fallback: scan all bull:* keys and extract queue names
    if (queueNames.size === 0) {
      cursor = '0';
      do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*`, 'COUNT', 500);
        cursor = nextCursor;
        for (const key of keys) {
          const parts = key.split(':');
          if (parts.length >= 2) {
            queueNames.add(parts[1]);
          }
        }
      } while (cursor !== '0');
    }

    await redis.quit();
    return [...queueNames].sort();
  } catch (err) {
    try { await redis.quit(); } catch {}
    return [];
  }
}

// ─── Test 1: Redis Connectivity ──────────────────────────────────────────────

async function testRedisConnection() {
  divider('1. Redis Connectivity');
  info(`Target: ${REDIS_HOST}:${REDIS_PORT} (db ${REDIS_DB})`);
  if (REDIS_PASSWORD) info('Authentication: password provided');

  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();
    const pong = await redis.ping();
    if (pong === 'PONG') {
      pass(`PING → ${chalk.green('PONG')} (Redis is alive)`);
    } else {
      fail(`PING returned unexpected: ${pong}`);
    }

    // Server info
    const infoStr = await redis.info('server');
    const version = infoStr.match(/redis_version:(.+)/)?.[1]?.trim();
    const uptime = infoStr.match(/uptime_in_seconds:(\d+)/)?.[1];
    const mode = infoStr.match(/redis_mode:(.+)/)?.[1]?.trim();
    if (version) info(`Redis version: ${chalk.white.bold(version)}`);
    if (mode) info(`Mode: ${chalk.white.bold(mode)}`);
    if (uptime) info(`Uptime: ${chalk.white.bold(formatUptime(parseInt(uptime)))}`);

    // Memory
    const memInfo = await redis.info('memory');
    const usedMem = memInfo.match(/used_memory_human:(.+)/)?.[1]?.trim();
    const peakMem = memInfo.match(/used_memory_peak_human:(.+)/)?.[1]?.trim();
    if (usedMem) info(`Memory used: ${chalk.white.bold(usedMem)}${peakMem ? chalk.gray(` (peak: ${peakMem})`) : ''}`);

    // Clients
    const clientInfo = await redis.info('clients');
    const connectedClients = clientInfo.match(/connected_clients:(\d+)/)?.[1];
    if (connectedClients) info(`Connected clients: ${chalk.white.bold(connectedClients)}`);

    // Keyspace
    const keyspace = await redis.info('keyspace');
    const dbMatches = keyspace.matchAll(/db(\d+):keys=(\d+),expires=(\d+)/g);
    let totalKeys = 0;
    for (const m of dbMatches) {
      const dbNum = m[1];
      const keys = m[2];
      const expires = m[3];
      totalKeys += parseInt(keys);
      info(`db${dbNum}: ${chalk.white.bold(keys)} keys (${expires} with TTL)`);
    }
    if (totalKeys === 0) info('No keys found in any database');

    await redis.quit();
    return true;
  } catch (err) {
    fail(`Cannot connect to Redis at ${REDIS_HOST}:${REDIS_PORT}`);
    info(`Error: ${err.message}`);
    if (err.message.includes('NOAUTH') || err.message.includes('AUTH')) {
      info(chalk.yellow('Hint: Redis requires authentication. Use --password <pass>'));
    }
    if (err.message.includes('ECONNREFUSED')) {
      info(chalk.yellow('Hint: Is Redis running? Try: redis-cli ping'));
    }
    try { await redis.quit(); } catch {}
    return false;
  }
}

// ─── Test 2: Redis Cache Operations ──────────────────────────────────────────

async function testRedisCacheOps() {
  divider('2. Redis Cache Operations (GET/SET/DEL/TTL)');

  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();

    const testKey = `__bullmq_tester__:${Date.now()}`;
    const testValue = JSON.stringify({ test: true, ts: Date.now(), pid: process.pid });

    // SET
    const setResult = await redis.set(testKey, testValue, 'EX', 30);
    if (setResult === 'OK') {
      pass(`SET → OK`);
    } else {
      fail(`SET returned: ${setResult}`);
    }

    // GET
    const getResult = await redis.get(testKey);
    if (getResult === testValue) {
      pass(`GET → data matches`);
    } else {
      fail(`GET returned mismatched data`);
    }

    // TTL
    const ttl = await redis.ttl(testKey);
    if (ttl > 0 && ttl <= 30) {
      pass(`TTL → ${ttl}s (expiry working)`);
    } else {
      warn(`TTL returned unexpected value: ${ttl}`);
    }

    // INCR (atomic counter test)
    const counterKey = `${testKey}:counter`;
    await redis.set(counterKey, '0', 'EX', 30);
    const incrResult = await redis.incr(counterKey);
    if (incrResult === 1) {
      pass(`INCR → atomic counter works`);
    } else {
      warn(`INCR returned unexpected: ${incrResult}`);
    }

    // DEL
    const delResult = await redis.del(testKey, counterKey);
    if (delResult === 2) {
      pass(`DEL → cleaned up (${delResult} keys)`);
    } else {
      warn(`DEL returned: ${delResult}`);
    }

    // Verify deletion
    const verifyDel = await redis.get(testKey);
    if (verifyDel === null) {
      pass(`Verify → key is gone`);
    } else {
      fail(`Key still exists after DEL`);
    }

    // Pub/Sub test (BullMQ relies on this)
    info('Testing Pub/Sub (used by BullMQ for job notifications)...');
    const pubsubResult = await new Promise((resolve) => {
      const timeout = setTimeout(() => resolve(false), 3000);
      const sub = redis.duplicate();
      const channel = `__bullmq_tester__:pubsub:${Date.now()}`;

      sub.subscribe(channel, (err) => {
        if (err) { clearTimeout(timeout); resolve(false); return; }
        redis.publish(channel, 'test');
      });

      sub.on('message', (ch, msg) => {
        if (ch === channel && msg === 'test') {
          clearTimeout(timeout);
          sub.unsubscribe();
          sub.quit();
          resolve(true);
        }
      });
    });

    if (pubsubResult) {
      pass(`Pub/Sub → working`);
    } else {
      fail(`Pub/Sub → not working (BullMQ needs this)`);
    }

    await redis.quit();
  } catch (err) {
    fail(`Cache operations failed: ${err.message}`);
    try { await redis.quit(); } catch {}
  }
}

// ─── Test 3: BullMQ Queue Inspection ─────────────────────────────────────────

async function testQueueInspection() {
  divider('3. BullMQ Queue Auto-Discovery & Inspection');

  info('Scanning Redis for BullMQ queues...');
  const queueNames = await discoverQueues();

  if (queueNames.length === 0) {
    info('No BullMQ queues found in Redis.');
    info(`Looked for keys matching: ${BULLMQ_PREFIX}:*:meta / ${BULLMQ_PREFIX}:*:id`);
    info('This is normal if no jobs have been added yet.');
    return;
  }

  info(`Discovered ${chalk.white.bold(queueNames.length)} queue(s)\n`);

  // Table header
  console.log(chalk.gray(
    `  ${pad('Queue Name', 30)} ${pad('Wait', 7)} ${pad('Active', 7)} ${pad('Delay', 7)} ${pad('Failed', 7)} ${pad('Done', 7)} Status`
  ));
  console.log(chalk.gray('  ' + '─'.repeat(78)));

  const queuesWithFailures = [];
  const queuesWithWarnings = [];

  for (const name of queueNames) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});

    try {
      const [waiting, active, delayed, failed, completed] = await Promise.all([
        q.getWaitingCount(),
        q.getActiveCount(),
        q.getDelayedCount(),
        q.getFailedCount(),
        q.getCompletedCount(),
      ]);

      let status;
      if (failed > 0) {
        status = chalk.red('● FAILURES');
        queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, queue: q });
        totalFail++;
      } else if (active > 0) {
        status = chalk.green('● ACTIVE');
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, queue: q, reason: 'active' });
        totalPass++;
      } else if (waiting > 0 || delayed > 0) {
        status = chalk.yellow('● PENDING');
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, queue: q, reason: 'pending' });
        totalWarn++;
      } else {
        status = chalk.gray('○ IDLE');
        totalPass++;
        try { await q.close(); } catch {}
      }

      console.log(
        `  ${pad(name, 30)} ${pad(String(waiting), 7)} ${pad(String(active), 7)} ${pad(String(delayed), 7)} ${pad(String(failed), 7)} ${pad(String(completed), 7)} ${status}`
      );
    } catch (err) {
      console.log(`  ${pad(name, 30)} ${chalk.red('ERROR: ' + err.message)}`);
      totalFail++;
      try { await q.close(); } catch {}
    }
  }

  // ── Detailed Failed Queue Reports ──────────────────────────────────────────
  if (queuesWithFailures.length > 0) {
    console.log('');
    console.log(chalk.red.bold('  ┌──────────────────────────────────────────────────────────────┐'));
    console.log(chalk.red.bold('  │                    FAILED QUEUE DETAILS                      │'));
    console.log(chalk.red.bold('  └──────────────────────────────────────────────────────────────┘'));

    for (const qi of queuesWithFailures) {
      console.log('');
      console.log(`  ${chalk.red.bold('▸ Queue:')}    ${chalk.bold.white(qi.name)}`);
      console.log(`  ${chalk.red.bold('▸ Counts:')}   ${chalk.red(`${qi.failed} failed`)}, ${qi.waiting} waiting, ${qi.active} active, ${qi.delayed} delayed, ${qi.completed} completed`);

      try {
        const failedJobs = await qi.queue.getFailed(0, 5);
        if (failedJobs.length > 0) {
          console.log('');
          console.log(chalk.red(`  ── Failed Jobs (showing up to 5) ${'─'.repeat(38)}`));

          for (let i = 0; i < failedJobs.length; i++) {
            const fj = failedJobs[i];
            const jobTimestamp = fj.timestamp ? new Date(fj.timestamp).toLocaleString() : 'unknown';
            const finishedOn = fj.finishedOn ? new Date(fj.finishedOn).toLocaleString() : 'unknown';
            const processedOn = fj.processedOn ? new Date(fj.processedOn).toLocaleString() : 'unknown';
            const attemptsMade = fj.attemptsMade || 0;
            const maxAttempts = fj.opts?.attempts || 'N/A';
            const age = fj.timestamp ? formatUptime(Math.round((Date.now() - fj.timestamp) / 1000)) : '';

            console.log('');
            console.log(chalk.red(`  ┌─ Job #${i + 1} ${'─'.repeat(55)}`));
            console.log(`  │  ${chalk.gray('Job ID:')}        ${chalk.white.bold(fj.id || 'N/A')}`);
            console.log(`  │  ${chalk.gray('Job Name:')}      ${chalk.white(fj.name || 'N/A')}`);
            console.log(`  │  ${chalk.gray('Created At:')}    ${chalk.white(jobTimestamp)} ${age ? chalk.gray(`(${age} ago)`) : ''}`);
            console.log(`  │  ${chalk.gray('Processed At:')}  ${chalk.white(processedOn)}`);
            console.log(`  │  ${chalk.gray('Failed At:')}     ${chalk.white(finishedOn)}`);
            console.log(`  │  ${chalk.gray('Attempts:')}      ${chalk.white(`${attemptsMade} / ${maxAttempts}`)}`);

            // Job Data (payload)
            if (fj.data && Object.keys(fj.data).length > 0) {
              console.log(`  │`);
              console.log(`  │  ${chalk.gray('Job Payload:')}`);
              const dataStr = JSON.stringify(fj.data, null, 2);
              for (const line of dataStr.split('\n')) {
                console.log(`  │    ${chalk.yellow(line)}`);
              }
            }

            // Job Options (if non-trivial)
            if (fj.opts && Object.keys(fj.opts).length > 0) {
              const relevantOpts = {};
              if (fj.opts.attempts) relevantOpts.attempts = fj.opts.attempts;
              if (fj.opts.delay) relevantOpts.delay = `${fj.opts.delay}ms`;
              if (fj.opts.backoff) relevantOpts.backoff = fj.opts.backoff;
              if (fj.opts.priority) relevantOpts.priority = fj.opts.priority;
              if (fj.opts.repeat) relevantOpts.repeat = fj.opts.repeat;
              if (fj.opts.jobId) relevantOpts.jobId = fj.opts.jobId;
              if (Object.keys(relevantOpts).length > 0) {
                console.log(`  │`);
                console.log(`  │  ${chalk.gray('Job Options:')}`);
                for (const line of JSON.stringify(relevantOpts, null, 2).split('\n')) {
                  console.log(`  │    ${chalk.cyan(line)}`);
                }
              }
            }

            // Error reason
            if (fj.failedReason) {
              console.log(`  │`);
              console.log(`  │  ${chalk.gray('Error Message:')}`);
              console.log(`  │    ${chalk.red.bold(fj.failedReason)}`);
            }

            // Stack trace
            if (fj.stacktrace && fj.stacktrace.length > 0) {
              console.log(`  │`);
              console.log(`  │  ${chalk.gray('Stack Trace:')}`);
              for (const trace of fj.stacktrace) {
                for (const tl of trace.split('\n')) {
                  console.log(`  │    ${chalk.gray(tl)}`);
                }
              }
            }

            // Return value
            if (fj.returnvalue !== undefined && fj.returnvalue !== null) {
              console.log(`  │`);
              console.log(`  │  ${chalk.gray('Return Value:')} ${chalk.white(JSON.stringify(fj.returnvalue))}`);
            }

            console.log(chalk.red(`  └${'─'.repeat(62)}`));
          }

          // Root Cause Analysis
          const errorMessages = failedJobs.map(fj => fj.failedReason || '').filter(Boolean);
          const uniqueErrors = [...new Set(errorMessages)];
          if (uniqueErrors.length > 0) {
            console.log('');
            console.log(chalk.yellow(`  🔍 Root Cause Analysis (${uniqueErrors.length} unique error${uniqueErrors.length > 1 ? 's' : ''}):`));
            for (const errMsg of uniqueErrors) {
              const count = errorMessages.filter(e => e === errMsg).length;
              console.log(`     ${chalk.red('•')} "${chalk.white(errMsg)}" ${chalk.gray(`(${count}x)`)}`);

              // Context-aware suggestions for common errors
              const lower = errMsg.toLowerCase();
              if (lower.includes('econnrefused') || lower.includes('connection refused')) {
                console.log(chalk.gray(`       → The target service/database was unreachable when the job ran`));
              } else if (lower.includes('timeout') || lower.includes('timed out')) {
                console.log(chalk.gray(`       → Operation timed out; check database/service performance`));
              } else if (lower.includes('deadlock')) {
                console.log(chalk.gray(`       → Concurrent DB writes caused a deadlock; consider retry logic`));
              } else if (lower.includes('enotfound') || lower.includes('getaddrinfo')) {
                console.log(chalk.gray(`       → DNS resolution failed; check the hostname configuration`));
              } else if (lower.includes('unauthorized') || lower.includes('forbidden') || lower.includes('401') || lower.includes('403')) {
                console.log(chalk.gray(`       → Authentication/authorization error; check API keys or credentials`));
              } else if (lower.includes('not found') || lower.includes('404')) {
                console.log(chalk.gray(`       → A required resource was not found; check data dependencies`));
              } else if (lower.includes('out of memory') || lower.includes('oom')) {
                console.log(chalk.gray(`       → System ran out of memory; check resource limits`));
              } else if (lower.includes('duplicate') || lower.includes('unique') || lower.includes('23505')) {
                console.log(chalk.gray(`       → Duplicate key violation; check idempotency logic`));
              }
            }
          }

          // Remediation
          console.log('');
          console.log(chalk.yellow('  💡 Remediation:'));
          console.log(chalk.gray(`     • Retry failed jobs programmatically:`));
          console.log(chalk.white(`       const q = new Queue('${qi.name}', { connection })`));
          console.log(chalk.white(`       const failed = await q.getFailed(); for (const j of failed) await j.retry();`));
          console.log(chalk.gray(`     • Clear all failed jobs:`));
          console.log(chalk.white(`       await q.clean(0, 0, 'failed');`));
          console.log(chalk.gray(`     • Inspect via redis-cli:`));
          console.log(chalk.white(`       redis-cli LRANGE ${BULLMQ_PREFIX}:${qi.name}:failed 0 -1`));
        }
      } catch (fetchErr) {
        console.log(chalk.gray(`     Could not fetch failed job details: ${fetchErr.message}`));
      }
      try { await qi.queue.close(); } catch {}
    }
  }

  // ── Detailed Warning Queue Reports ─────────────────────────────────────────
  if (queuesWithWarnings.length > 0) {
    console.log('');
    console.log(chalk.yellow.bold('  ┌──────────────────────────────────────────────────────────────┐'));
    console.log(chalk.yellow.bold('  │                   WARNING QUEUE DETAILS                      │'));
    console.log(chalk.yellow.bold('  └──────────────────────────────────────────────────────────────┘'));

    for (const qi of queuesWithWarnings) {
      console.log('');
      console.log(`  ${chalk.yellow.bold('▸ Queue:')}    ${chalk.bold.white(qi.name)}`);
      console.log(`  ${chalk.yellow.bold('▸ Counts:')}   ${qi.waiting} waiting, ${qi.active} active, ${qi.delayed} delayed, ${qi.failed} failed, ${qi.completed} completed`);

      if (qi.reason === 'active') {
        console.log(`  ${chalk.yellow.bold('▸ Status:')}   ${chalk.green('Jobs are currently being processed')}`);

        try {
          const activeJobs = await qi.queue.getActive(0, 3);
          if (activeJobs.length > 0) {
            console.log('');
            console.log(chalk.yellow(`  ── Active Jobs (showing up to 3) ${'─'.repeat(37)}`));
            for (const aj of activeJobs) {
              const processedOn = aj.processedOn ? new Date(aj.processedOn).toLocaleString() : 'unknown';
              const elapsed = aj.processedOn ? `${Math.round((Date.now() - aj.processedOn) / 1000)}s ago` : '';
              console.log(`  │  ${chalk.gray('Job ID:')} ${chalk.white(aj.id)}  ${chalk.gray('Name:')} ${chalk.white(aj.name)}  ${chalk.gray('Started:')} ${chalk.white(processedOn)} ${chalk.cyan(elapsed)}`);
              if (aj.data && Object.keys(aj.data).length > 0) {
                const preview = JSON.stringify(aj.data);
                console.log(`  │  ${chalk.gray('Payload:')} ${chalk.yellow(preview.length > 120 ? preview.substring(0, 120) + '…' : preview)}`);
              }
            }
          }
        } catch {}

        console.log('');
        console.log(chalk.yellow('  💡 Note:'));
        console.log(chalk.gray('     • Active jobs are expected when a worker is running.'));
        console.log(chalk.gray('     • If jobs stay active for too long, they may be stalled.'));
        console.log(chalk.gray(`     • BullMQ v4+ auto-recovers stalled jobs via the Worker's built-in stalledInterval.`));
      }

      if (qi.reason === 'pending') {
        console.log(`  ${chalk.yellow.bold('▸ Status:')}   ${chalk.yellow('Jobs are waiting but no worker is processing them')}`);

        try {
          const waitingJobs = await qi.queue.getWaiting(0, 3);
          if (waitingJobs.length > 0) {
            console.log('');
            console.log(chalk.yellow(`  ── Waiting Jobs (showing up to 3) ${'─'.repeat(36)}`));
            for (const wj of waitingJobs) {
              const createdAt = wj.timestamp ? new Date(wj.timestamp).toLocaleString() : 'unknown';
              const age = wj.timestamp ? formatUptime(Math.round((Date.now() - wj.timestamp) / 1000)) : '';
              console.log(`  │  ${chalk.gray('Job ID:')} ${chalk.white(wj.id)}  ${chalk.gray('Name:')} ${chalk.white(wj.name)}  ${chalk.gray('Queued:')} ${chalk.white(createdAt)}  ${chalk.red(`(waiting ${age})`)}`);
              if (wj.data && Object.keys(wj.data).length > 0) {
                const preview = JSON.stringify(wj.data);
                console.log(`  │  ${chalk.gray('Payload:')} ${chalk.yellow(preview.length > 120 ? preview.substring(0, 120) + '…' : preview)}`);
              }
            }
          }
        } catch {}

        try {
          const delayedJobs = await qi.queue.getDelayed(0, 3);
          if (delayedJobs.length > 0) {
            console.log('');
            console.log(chalk.yellow(`  ── Delayed Jobs (showing up to 3) ${'─'.repeat(36)}`));
            for (const dj of delayedJobs) {
              const delay = dj.opts?.delay ? `${dj.opts.delay}ms delay` : '';
              const createdAt = dj.timestamp ? new Date(dj.timestamp).toLocaleString() : 'unknown';
              console.log(`  │  ${chalk.gray('Job ID:')} ${chalk.white(dj.id)}  ${chalk.gray('Name:')} ${chalk.white(dj.name)}  ${chalk.gray('Queued:')} ${chalk.white(createdAt)}  ${chalk.cyan(delay)}`);
              if (dj.data && Object.keys(dj.data).length > 0) {
                const preview = JSON.stringify(dj.data);
                console.log(`  │  ${chalk.gray('Payload:')} ${chalk.yellow(preview.length > 120 ? preview.substring(0, 120) + '…' : preview)}`);
              }
            }
          }
        } catch {}

        console.log('');
        console.log(chalk.yellow('  💡 Possible Causes:'));
        console.log(chalk.gray(`     • The worker for queue "${qi.name}" is not running or has crashed.`));
        console.log(chalk.gray(`     • The worker is connected to a different Redis instance.`));
        console.log(chalk.gray(`     • The worker's concurrency limit has been reached.`));
        console.log(chalk.yellow('  💡 Remediation:'));
        console.log(chalk.gray(`     • Start/restart the service that hosts the "${qi.name}" worker.`));
        console.log(chalk.gray(`     • Verify REDIS_HOST and REDIS_PORT in the worker's environment.`));
      }

      try { await qi.queue.close(); } catch {}
    }
  }
}

// ─── Test 4: BullMQ Roundtrip ────────────────────────────────────────────────

async function testRoundtrip() {
  divider('4. BullMQ Roundtrip Test (Add → Process → Verify)');
  info('Testing full job lifecycle on a temporary queue\n');

  const TEST_QUEUE = `__bullmq_tester__${Date.now()}`;

  const queue = new Queue(TEST_QUEUE, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  queue.on('error', () => {});

  try {
    // Add a test job
    const testPayload = {
      test: true,
      timestamp: Date.now(),
      message: 'BullMQ roundtrip test',
      pid: process.pid,
    };

    const job = await queue.add('roundtrip-test', testPayload, {
      removeOnComplete: true,
      removeOnFail: true,
    });
    pass(`Job added → ID: ${chalk.gray(job.id)}`);

    // Verify it's in waiting
    const waitingBefore = await queue.getWaitingCount();
    if (waitingBefore >= 1) {
      pass(`Job is in waiting state (count: ${waitingBefore})`);
    } else {
      fail(`Expected waiting count ≥ 1, got ${waitingBefore}`);
    }

    // Start a temporary worker to consume it
    let workerResolved = false;
    const processingPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (!workerResolved) {
          reject(new Error('Worker did not process job within 10s'));
        }
      }, 10000);

      const worker = new Worker(TEST_QUEUE, async (j) => {
        if (j.data.test === true && j.data.message === testPayload.message) {
          workerResolved = true;
          clearTimeout(timeout);
          resolve({ jobId: j.id, data: j.data });
        } else {
          throw new Error('Payload mismatch');
        }
      }, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });

      worker.on('completed', async () => { await worker.close(); });
      worker.on('failed', async (j, err) => { clearTimeout(timeout); await worker.close(); reject(err); });
      worker.on('error', () => {});
    });

    const result = await processingPromise;
    pass(`Worker consumed job → ID: ${chalk.gray(result.jobId)}`);
    pass(`Payload integrity → verified`);

    // Verify queue is drained
    await sleep(500);
    const waitingAfter = await queue.getWaitingCount();
    const activeAfter = await queue.getActiveCount();
    if (waitingAfter === 0 && activeAfter === 0) {
      pass(`Queue drained → waiting: 0, active: 0`);
    } else {
      warn(`Queue not fully drained (waiting: ${waitingAfter}, active: ${activeAfter})`);
    }

    // Test offline buffering behavior
    info('Testing offline queue buffering (enableOfflineQueue: true)...');
    const offlineQueue = new Queue(`${TEST_QUEUE}__offline`, {
      connection: {
        host: REDIS_HOST,
        port: REDIS_PORT + 1000, // Intentionally wrong port
        maxRetriesPerRequest: null,
        enableOfflineQueue: true,
        connectTimeout: 1000,
      },
      prefix: BULLMQ_PREFIX,
    });
    offlineQueue.on('error', () => {});

    let offlineBuffered = false;
    try {
      offlineQueue.add('test', { data: 'buffered' }).catch(() => {});
      offlineBuffered = true;
    } catch {
      offlineBuffered = false;
    }

    if (offlineBuffered) {
      pass(`Offline buffering → job held in memory (no crash)`);
    } else {
      fail(`Offline buffering → call threw synchronously`);
    }

    try { await offlineQueue.close(); } catch {}

    // Cleanup
    await queue.obliterate({ force: true });
    await queue.close();
    pass(`Cleanup → test queue obliterated`);

  } catch (err) {
    fail(`Roundtrip failed: ${err.message}`);
    info(`This could mean BullMQ is not correctly installed or Redis is misconfigured.`);
    try { await queue.obliterate({ force: true }); } catch {}
    try { await queue.close(); } catch {}
  }
}

// ─── Test 5: Redis Key Namespace Scan ────────────────────────────────────────

async function testRedisKeysScan() {
  divider('5. Redis Key Namespace Scan');

  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();

    // Scan for bull: keys
    const namespaces = {};
    let cursor = '0';
    let totalKeys = 0;

    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*`, 'COUNT', 500);
      cursor = nextCursor;

      for (const key of keys) {
        totalKeys++;
        const parts = key.split(':');
        if (parts.length >= 2) {
          const queueName = parts[1];
          namespaces[queueName] = (namespaces[queueName] || 0) + 1;
        }
      }
    } while (cursor !== '0');

    info(`Total BullMQ keys in Redis: ${chalk.white.bold(totalKeys)} (prefix: "${BULLMQ_PREFIX}")\n`);

    if (Object.keys(namespaces).length > 0) {
      console.log(chalk.gray(`  ${pad('Queue Namespace', 34)} Keys`));
      console.log(chalk.gray('  ' + '─'.repeat(45)));

      const sorted = Object.entries(namespaces).sort((a, b) => b[1] - a[1]);
      for (const [ns, count] of sorted) {
        console.log(`  ${pad(chalk.white(ns), 34)} ${count}`);
      }
    } else {
      info('No BullMQ keys found (queues may not have been used yet)');
    }

    // Scan for non-BullMQ keys too, for context
    let otherKeys = 0;
    const otherPrefixes = {};
    cursor = '0';
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'COUNT', 500);
      cursor = nextCursor;
      for (const key of keys) {
        if (!key.startsWith(`${BULLMQ_PREFIX}:`)) {
          otherKeys++;
          const prefix = key.split(':')[0] || '(no prefix)';
          otherPrefixes[prefix] = (otherPrefixes[prefix] || 0) + 1;
        }
      }
    } while (cursor !== '0');

    if (otherKeys > 0) {
      console.log('');
      info(`Other keys in Redis: ${chalk.white.bold(otherKeys)}`);
      const sortedOther = Object.entries(otherPrefixes).sort((a, b) => b[1] - a[1]).slice(0, 10);
      for (const [prefix, count] of sortedOther) {
        console.log(`    ${chalk.gray('•')} ${chalk.white(prefix)}:* → ${count} key(s)`);
      }
    }

    await redis.quit();
  } catch (err) {
    fail(`Key scan failed: ${err.message}`);
    try { await redis.quit(); } catch {}
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('');
  console.log(chalk.cyan.bold('╔═══════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.cyan.bold('║           BullMQ & Redis Tester — Universal CLI Tool             ║'));
  console.log(chalk.cyan.bold('╚═══════════════════════════════════════════════════════════════════╝'));
  console.log(chalk.gray(`  Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}   |   ${new Date().toLocaleString()}`));
  console.log(chalk.gray(`  Key prefix: "${BULLMQ_PREFIX}"   |   PID: ${process.pid}`));

  const redisOk = await testRedisConnection();

  if (!redisOk) {
    console.log('');
    fail(chalk.red.bold('Redis is not reachable. Cannot continue with further tests.'));
    console.log(`\n  ${chalk.yellow('Troubleshooting:')}`);
    console.log(`    1. Check if Redis is running:  ${chalk.white('redis-cli ping')}`);
    console.log(`    2. Start Redis:                ${chalk.white('sudo systemctl start redis')}`);
    console.log(`    3. Custom host/port:           ${chalk.white('node index.js --host <host> --port <port>')}`);
    console.log(`    4. Auth required:              ${chalk.white('node index.js --password <pass>')}`);
    printSummary();
    process.exit(1);
  }

  if (REDIS_ONLY) {
    await testRedisCacheOps();
    printSummary();
    process.exit(totalFail > 0 ? 1 : 0);
  }

  if (QUEUES_ONLY) {
    await testQueueInspection();
    await testRedisKeysScan();
    printSummary();
    process.exit(totalFail > 0 ? 1 : 0);
  }

  if (ROUNDTRIP) {
    await testRedisCacheOps();
    await testRoundtrip();
    printSummary();
    process.exit(totalFail > 0 ? 1 : 0);
  }

  // Default: run everything
  await testRedisCacheOps();
  await testQueueInspection();
  await testRoundtrip();
  await testRedisKeysScan();

  printSummary();
  process.exit(totalFail > 0 ? 1 : 0);
}

function printSummary() {
  divider('Summary');
  console.log(`  ${chalk.green.bold(`${totalPass} passed`)}  ${chalk.red.bold(`${totalFail} failed`)}  ${chalk.yellow.bold(`${totalWarn} warnings`)}`);

  if (totalFail === 0 && totalWarn === 0) {
    console.log(`\n  ${chalk.green.bold('🎉 All tests passed! Redis and BullMQ are healthy.')}`);
  } else if (totalFail === 0) {
    console.log(`\n  ${chalk.yellow.bold('⚠  All tests passed but some queues need attention. Review warnings above.')}`);
  } else {
    console.log(`\n  ${chalk.red.bold('✗  Some tests failed. Review the detailed output above.')}`);
  }
  console.log('');
}

main().catch((err) => {
  console.error(chalk.red(`\nFatal error: ${err.message}`));
  process.exit(1);
});
