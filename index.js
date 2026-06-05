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
 *   node index.js tui             # Start Terminal User Interface (TUI) Dashboard
 *   node index.js -it             # Start interactive action control center
 *   node index.js live            # Start live monitoring dashboard
 *   node index.js --redis-only    # Redis connectivity + cache only
 *   node index.js --queues-only   # BullMQ queue inspection only
 *   node index.js --roundtrip     # Full roundtrip: add job → consume → verify
 *   node index.js --url redis://  # Connect via Redis URL (e.g. rediss:// for TLS)
 *   node index.js --tls           # Force SSL/TLS connection
 *   node index.js --retry-failed  # Direct CLI action: retry all failed jobs
 *   node index.js --pause queue   # Direct CLI action: pause a queue
 *   node index.js --resume queue  # Direct CLI action: resume a queue
 */

const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const chalk = require('chalk');
const readline = require('readline');
const fs = require('fs');
const path = require('path');

// ─── CLI Argument Parsing ────────────────────────────────────────────────────

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(name);
  if (idx !== -1 && idx + 1 < args.length) {
    const val = args[idx + 1];
    if (val.startsWith('-')) return null; // Avoid capturing another flag as a value
    return val;
  }
  return null;
};
const hasFlag = (name) => args.includes(name);

let REDIS_HOST = getArg('--host') || process.env.REDIS_HOST || '127.0.0.1';
let REDIS_PORT = parseInt(getArg('--port') || process.env.REDIS_PORT || '6379', 10);
let REDIS_PASSWORD = getArg('--password') || process.env.REDIS_PASSWORD || undefined;
let REDIS_DB = parseInt(getArg('--db') || process.env.REDIS_DB || '0', 10);
let REDIS_TLS = hasFlag('--tls');
const REDIS_URL = getArg('--url') || process.env.REDIS_URL || null;

if (REDIS_URL) {
  try {
    const parsed = new URL(REDIS_URL);
    REDIS_HOST = parsed.hostname || REDIS_HOST;
    if (parsed.port) {
      REDIS_PORT = parseInt(parsed.port, 10);
    }
    if (parsed.password) {
      REDIS_PASSWORD = decodeURIComponent(parsed.password);
    } else if (parsed.username && !parsed.password) {
      REDIS_PASSWORD = decodeURIComponent(parsed.username);
    }
    if (parsed.pathname && parsed.pathname.length > 1) {
      REDIS_DB = parseInt(parsed.pathname.substring(1), 10);
    }
    if (parsed.protocol === 'rediss:') {
      REDIS_TLS = true;
    }
  } catch (err) {
    console.error(chalk.red(`Invalid Redis URL format: ${REDIS_URL}`));
    process.exit(1);
  }
}

const BULLMQ_PREFIX = getArg('--prefix') || 'bull';
const REDIS_ONLY = hasFlag('--redis-only');
const QUEUES_ONLY = hasFlag('--queues-only');
const ROUNDTRIP = hasFlag('--roundtrip');
const LIVE = hasFlag('--live') || hasFlag('-l') || args.includes('live');
const TUI_MODE = hasFlag('--tui') || hasFlag('-t') || args.includes('tui');
const INTERVAL_ARG = getArg('--interval') || getArg('-i');
const INTERVAL = parseInt(INTERVAL_ARG || '2', 10) * 1000;
const HELP = hasFlag('--help') || hasFlag('-h');

// Enhanced options
const INTERACTIVE = hasFlag('--interactive') || hasFlag('-it') || args.includes('interactive');
const JSON_OUTPUT = hasFlag('--json');
const OUTPUT_FILE = getArg('--output') || getArg('-o');

// Queue admin actions
const RETRY_FAILED = hasFlag('--retry-failed') || hasFlag('--retry-all-failed');
const RETRY_QUEUE = getArg('--retry-failed');
const CLEAN_FAILED = hasFlag('--clean-failed');
const CLEAN_FAILED_QUEUE = getArg('--clean-failed');
const CLEAN_COMPLETED = hasFlag('--clean-completed');
const CLEAN_COMPLETED_QUEUE = getArg('--clean-completed');
const DRAIN_QUEUE_FLAG = hasFlag('--drain');
const DRAIN_QUEUE_NAME = getArg('--drain');
const OBLITERATE_QUEUE_FLAG = hasFlag('--obliterate');
const OBLITERATE_QUEUE_NAME = getArg('--obliterate');
const ADD_JOB_FLAG = hasFlag('--add-job');
const ADD_JOB_QUEUE = getArg('--add-job');

// Pause / Resume Actions
const PAUSE_QUEUE_FLAG = hasFlag('--pause');
const PAUSE_QUEUE_NAME = getArg('--pause');
const RESUME_QUEUE_FLAG = hasFlag('--resume');
const RESUME_QUEUE_NAME = getArg('--resume');

// Maintenance / Pruning Settings
const PRUNE_COMPLETED_HOURS = getArg('--prune-completed');
const PRUNE_FAILED_HOURS = getArg('--prune-failed');
const AUTO_RETRY_FAILED_FLAG = hasFlag('--auto-retry');

// Queue Health Thresholds
const MAX_WAIT = parseInt(getArg('--max-wait') || '100', 10);
const MAX_FAILED = parseInt(getArg('--max-failed') || '50', 10);

if (HELP) {
  console.log(`
${chalk.bold.cyan('BullMQ & Redis Tester')} — Universal CLI Tool

${chalk.yellow('Usage:')}
  node index.js [options]
  node index.js tui                              # Start Terminal User Interface (TUI) Dashboard
  node index.js live                             # Start live monitoring dashboard
  node index.js interactive                      # Start interactive action control center
  node index.js -it                              # Short flag for interactive mode

${chalk.yellow('Options:')}
  --tui, -t           Start Terminal User Interface (TUI) Dashboard
  --interactive, -it  Interactive queue management & inspection menu
  --live, -l          Live monitoring mode (scans and reports continuously)
  --interval, -i <s > Interval in seconds for live mode (default: 2)
  --redis-only        Test Redis connectivity and cache ops only
  --queues-only       Test BullMQ queue health only
  --roundtrip         Full roundtrip test (add job → consume → verify)
  --url <url>         Redis connection URL (e.g., redis://:pass@host:port/db)
  --tls               Enable SSL/TLS connection (e.g. for production Redis)
  --host <host>       Redis host (default: 127.0.0.1 or REDIS_HOST env)
  --port <port>       Redis port (default: 6379 or REDIS_PORT env)
  --password <pass>   Redis password (default: none or REDIS_PASSWORD env)
  --db <number>       Redis DB index (default: 0 or REDIS_DB env)
  --prefix <prefix>   BullMQ key prefix (default: bull)
  --json              Output raw health check data as JSON to stdout
  --output, -o <file> Save health check report to file (supports .json, .md)
  --help, -h          Show this help message

${chalk.yellow('Queue Actions:')}
  --retry-failed [q]  Retry failed jobs for queue [q] (or all queues if no q specified)
  --clean-failed [q]  Clear failed jobs for queue [q] (or all queues if no q specified)
  --clean-completed[q]Clear completed jobs for queue [q] (or all queues if no q specified)
  --drain [q]         Remove all waiting, active, delayed, failed jobs from queue [q]
  --obliterate [q]    Completely delete queue [q] and all its keys/jobs from Redis
  --add-job [q]       Add a test job to queue [q] (use --job-name and --job-data)
  --job-name <name>   Name for the test job (default: test-job)
  --job-data <json>   JSON string payload for the test job
  --pause [q]         Pause queue [q] (globally blocks picking up new jobs)
  --resume [q]        Resume paused queue [q]

${chalk.yellow('Automated Maintenance & Alerting:')}
  --prune-completed <h> Clean completed jobs older than <h> hours
  --prune-failed <h>    Clean failed jobs older than <h> hours
  --auto-retry          Automatically retry failed jobs for all queues
  --max-wait <number>   Max waiting jobs allowed before triggering Alert (default: 100)
  --max-failed <number> Max failed jobs allowed before triggering Alert (default: 50)

${chalk.yellow('Environment variables:')}
  REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB, REDIS_URL

${chalk.yellow('Examples:')}
  node index.js                                  # Test local Redis once
  node index.js tui                              # Open Dashboard TUI Mode
  node index.js -it                              # Launch interactive menu
  node index.js --url redis://:mysecret@127.0.0.1:6379/1 --tls
  node index.js --retry-failed billing-sync       # Retry failed billing jobs
  node index.js --json -o report.json            # Run and save to JSON file
`);
  process.exit(0);
}

// ─── Connection Configs ──────────────────────────────────────────────────────

const REDIS_CONNECTION = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  ...(REDIS_PASSWORD ? { password: REDIS_PASSWORD } : {}),
  db: REDIS_DB,
  ...(REDIS_TLS ? { tls: {} } : {}),
};

const BULLMQ_CONNECTION = {
  ...REDIS_CONNECTION,
  maxRetriesPerRequest: null,
  enableOfflineQueue: true,
};

// ─── Global JSON Report Data Structure ───────────────────────────────────────

const reportData = {
  timestamp: new Date().toISOString(),
  target: `redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}`,
  prefix: BULLMQ_PREFIX,
  summary: {
    passed: 0,
    failed: 0,
    warnings: 0,
    status: 'UNKNOWN',
  },
  redis: {
    connection: false,
    version: null,
    mode: null,
    uptime: null,
    memory_used: null,
    memory_peak: null,
    connected_clients: null,
    keyspace: [],
    latency: {
      avg: null,
      min: null,
      max: null,
      raw: []
    },
    cache_ops: {
      set: false,
      get: false,
      ttl: false,
      incr: false,
      del: false,
      pubsub: false
    }
  },
  queues: [],
  keys_scan: {
    total_bullmq_keys: 0,
    total_bullmq_memory_bytes: 0,
    namespaces: {},
    other_keys: 0,
    other_prefixes: {}
  },
  roundtrip: {
    run: false,
    job_added: false,
    job_waiting: false,
    job_consumed: false,
    payload_integrity: false,
    queue_drained: false,
    offline_buffering: false,
    cleanup: false
  }
};

// ─── Formatting Helpers ──────────────────────────────────────────────────────

const PASS = chalk.green.bold('✓ PASS');
const FAIL = chalk.red.bold('✗ FAIL');
const WARN = chalk.yellow.bold('⚠ WARN');
const INFO = chalk.blue('ℹ');

const printLog = (msg) => {
  if (!JSON_OUTPUT) console.log(msg);
};

const divider = (title) => {
  const line = '─'.repeat(64);
  printLog(`\n${chalk.cyan(line)}`);
  printLog(chalk.cyan.bold(`  ${title}`));
  printLog(chalk.cyan(line));
};

const pad = (str, len = 28) => String(str).padEnd(len);

let totalPass = 0;
let totalFail = 0;
let totalWarn = 0;

const pass = (msg) => { totalPass++; printLog(`  ${PASS}  ${msg}`); };
const fail = (msg) => { totalFail++; printLog(`  ${FAIL}  ${msg}`); };
const warn = (msg) => { totalWarn++; printLog(`  ${WARN}  ${msg}`); };
const info = (msg) => { printLog(`  ${INFO}  ${chalk.gray(msg)}`); };

function formatUptime(seconds) {
  if (seconds < 0 || isNaN(seconds)) seconds = 0;
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

function formatBytes(bytes) {
  if (bytes === 0 || isNaN(bytes)) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
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
        const parts = key.split(':');
        if (parts.length >= 3) {
          queueNames.add(parts[1]);
        }
      }
    } while (cursor !== '0');

    // Fallback: scan for bull:*:id keys in case meta doesn't exist
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
  if (REDIS_TLS) info('Connection Security: SSL/TLS Enabled');

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

    reportData.redis.connection = true;

    // Server info
    const infoStr = await redis.info('server');
    const version = infoStr.match(/redis_version:(.+)/)?.[1]?.trim();
    const uptime = infoStr.match(/uptime_in_seconds:(\d+)/)?.[1];
    const mode = infoStr.match(/redis_mode:(.+)/)?.[1]?.trim();
    if (version) {
      info(`Redis version: ${chalk.white.bold(version)}`);
      reportData.redis.version = version;
    }
    if (mode) {
      info(`Mode: ${chalk.white.bold(mode)}`);
      reportData.redis.mode = mode;
    }
    if (uptime) {
      info(`Uptime: ${chalk.white.bold(formatUptime(parseInt(uptime)))}`);
      reportData.redis.uptime = parseInt(uptime);
    }

    // Memory
    const memInfo = await redis.info('memory');
    const usedMem = memInfo.match(/used_memory_human:(.+)/)?.[1]?.trim();
    const peakMem = memInfo.match(/used_memory_peak_human:(.+)/)?.[1]?.trim();
    if (usedMem) {
      info(`Memory used: ${chalk.white.bold(usedMem)}${peakMem ? chalk.gray(` (peak: ${peakMem})`) : ''}`);
      reportData.redis.memory_used = usedMem;
      reportData.redis.memory_peak = peakMem;
    }

    // Clients
    const clientInfo = await redis.info('clients');
    const connectedClients = clientInfo.match(/connected_clients:(\d+)/)?.[1];
    if (connectedClients) {
      info(`Connected clients: ${chalk.white.bold(connectedClients)}`);
      reportData.redis.connected_clients = parseInt(connectedClients, 10);
    }

    // Keyspace
    const keyspace = await redis.info('keyspace');
    const dbMatches = keyspace.matchAll(/db(\d+):keys=(\d+),expires=(\d+)/g);
    let totalKeys = 0;
    for (const m of dbMatches) {
      const dbNum = m[1];
      const keys = parseInt(m[2], 10);
      const expires = parseInt(m[3], 10);
      totalKeys += keys;
      info(`db${dbNum}: ${chalk.white.bold(keys)} keys (${expires} with TTL)`);
      reportData.redis.keyspace.push({ db: parseInt(dbNum, 10), keys, expires });
    }
    if (totalKeys === 0) info('No keys found in any database');

    // Latency Check
    info('Measuring Redis response latency (5 PINGs)...');
    const latencies = [];
    for (let i = 0; i < 5; i++) {
      const start = Date.now();
      await redis.ping();
      latencies.push(Date.now() - start);
    }
    const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const min = Math.min(...latencies);
    const max = Math.max(...latencies);
    info(`Latency: avg=${avg.toFixed(1)}ms (min=${min}ms, max=${max}ms)`);
    reportData.redis.latency = { avg, min, max, raw: latencies };

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
      reportData.redis.cache_ops.set = true;
    } else {
      fail(`SET returned: ${setResult}`);
    }

    // GET
    const getResult = await redis.get(testKey);
    if (getResult === testValue) {
      pass(`GET → data matches`);
      reportData.redis.cache_ops.get = true;
    } else {
      fail(`GET returned mismatched data`);
    }

    // TTL
    const ttl = await redis.ttl(testKey);
    if (ttl > 0 && ttl <= 30) {
      pass(`TTL → ${ttl}s (expiry working)`);
      reportData.redis.cache_ops.ttl = true;
    } else {
      warn(`TTL returned unexpected value: ${ttl}`);
    }

    // INCR (atomic counter test)
    const counterKey = `${testKey}:counter`;
    await redis.set(counterKey, '0', 'EX', 30);
    const incrResult = await redis.incr(counterKey);
    if (incrResult === 1) {
      pass(`INCR → atomic counter works`);
      reportData.redis.cache_ops.incr = true;
    } else {
      warn(`INCR returned unexpected: ${incrResult}`);
    }

    // DEL
    const delResult = await redis.del(testKey, counterKey);
    if (delResult === 2) {
      pass(`DEL → cleaned up (${delResult} keys)`);
      reportData.redis.cache_ops.del = true;
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
      reportData.redis.cache_ops.pubsub = true;
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
  printLog(chalk.gray(
    `  ${pad('Queue Name', 30)} ${pad('Wait', 7)} ${pad('Active', 7)} ${pad('Delay', 7)} ${pad('Failed', 7)} ${pad('Done', 7)} ${pad('Workers', 8)} Status`
  ));
  printLog(chalk.gray('  ' + '─'.repeat(88)));

  const queuesWithFailures = [];
  const queuesWithWarnings = [];

  for (const name of queueNames) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});

    try {
      const [waiting, active, delayed, failed, completed, workersCount, isPaused] = await Promise.all([
        q.getWaitingCount(),
        q.getActiveCount(),
        q.getDelayedCount(),
        q.getFailedCount(),
        q.getCompletedCount(),
        q.getWorkersCount().catch(() => 0),
        q.isPaused().catch(() => false)
      ]);

      let status;
      let statusStr = 'IDLE';
      let reason = '';

      if (failed > MAX_FAILED) {
        status = chalk.red.bold('🚨 CRITICAL');
        statusStr = 'CRITICAL_FAILURES';
        queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'critical' });
        totalFail++;
      } else if (waiting > MAX_WAIT) {
        status = chalk.red.bold('🚨 OVERLOAD');
        statusStr = 'OVERLOADED';
        queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'overload' });
        totalFail++;
      } else if (failed > 0) {
        status = chalk.red('● FAILURES');
        statusStr = 'FAILURES';
        queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'failed' });
        totalFail++;
      } else if (isPaused) {
        status = chalk.yellow('⏸ PAUSED');
        statusStr = 'PAUSED';
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'paused' });
        totalWarn++;
      } else if (active > 0) {
        status = chalk.green('● ACTIVE');
        statusStr = 'ACTIVE';
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'active' });
        totalPass++;
      } else if (workersCount === 0 && (waiting > 0 || delayed > 0)) {
        status = chalk.red('● NO WORKERS');
        statusStr = 'NO_WORKERS';
        reason = 'no-workers';
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'no-workers' });
        totalWarn++;
      } else if (waiting > 0 || delayed > 0) {
        status = chalk.yellow('● PENDING');
        statusStr = 'PENDING';
        reason = 'pending';
        queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'pending' });
        totalWarn++;
      } else {
        status = chalk.gray('○ IDLE');
        statusStr = 'IDLE';
        totalPass++;
        try { await q.close(); } catch {}
      }

      printLog(
        `  ${pad(name, 30)} ${pad(String(waiting), 7)} ${pad(String(active), 7)} ${pad(String(delayed), 7)} ${pad(String(failed), 7)} ${pad(String(completed), 7)} ${pad(String(workersCount), 8)} ${status}`
      );

      // Fetch workers if we want to store detailed worker info in the JSON report
      let workersDetails = [];
      try {
        if (workersCount > 0) {
          workersDetails = await q.getWorkers();
        }
      } catch {}

      reportData.queues.push({
        name,
        waiting,
        active,
        delayed,
        failed,
        completed,
        workersCount,
        isPaused,
        status: statusStr,
        reason,
        workers: workersDetails
      });

    } catch (err) {
      printLog(`  ${pad(name, 30)} ${chalk.red('ERROR: ' + err.message)}`);
      totalFail++;
      try { await q.close(); } catch {}
    }
  }

  // ── Detailed Failed Queue Reports ──────────────────────────────────────────
  if (queuesWithFailures.length > 0 && !JSON_OUTPUT) {
    console.log('');
    console.log(chalk.red.bold('  ┌──────────────────────────────────────────────────────────────┐'));
    console.log(chalk.red.bold('  │                    FAILED QUEUE DETAILS                      │'));
    console.log(chalk.red.bold('  └──────────────────────────────────────────────────────────────┘'));

    for (const qi of queuesWithFailures) {
      console.log('');
      console.log(`  ${chalk.red.bold('▸ Queue:')}    ${chalk.bold.white(qi.name)} ${qi.isPaused ? chalk.yellow('(PAUSED ⏸)') : ''}`);
      console.log(`  ${chalk.red.bold('▸ Counts:')}   ${chalk.red(`${qi.failed} failed`)}, ${qi.waiting} waiting, ${qi.active} active, ${qi.delayed} delayed, ${qi.completed} completed, ${qi.workersCount} workers`);

      if (qi.reason === 'critical') {
        console.log(`  ${chalk.red.bold('🚨 Alert:')}    ${chalk.red.bold(`CRITICAL failed jobs (${qi.failed} > threshold of ${MAX_FAILED})`)}`);
      } else if (qi.reason === 'overload') {
        console.log(`  ${chalk.red.bold('🚨 Alert:')}    ${chalk.red.bold(`OVERLOADED waiting queue (${qi.waiting} > threshold of ${MAX_WAIT})`)}`);
      }

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

            // Job Options
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
  if (queuesWithWarnings.length > 0 && !JSON_OUTPUT) {
    console.log('');
    console.log(chalk.yellow.bold('  ┌──────────────────────────────────────────────────────────────┐'));
    console.log(chalk.yellow.bold('  │                   WARNING QUEUE DETAILS                      │'));
    console.log(chalk.yellow.bold('  └──────────────────────────────────────────────────────────────┘'));

    for (const qi of queuesWithWarnings) {
      console.log('');
      console.log(`  ${chalk.yellow.bold('▸ Queue:')}    ${chalk.bold.white(qi.name)} ${qi.isPaused ? chalk.yellow('(PAUSED ⏸)') : ''}`);
      console.log(`  ${chalk.yellow.bold('▸ Counts:')}   ${qi.waiting} waiting, ${qi.active} active, ${qi.delayed} delayed, ${qi.failed} failed, ${qi.completed} completed, ${qi.workersCount} workers`);

      if (qi.reason === 'paused') {
        console.log(`  ${chalk.yellow.bold('▸ Status:')}   ${chalk.yellow('Queue is PAUSED (no new jobs will be processed)')}`);
        console.log('');
        console.log(chalk.yellow('  💡 Remediation:'));
        console.log(chalk.gray(`     • Resume the queue via command line:`));
        console.log(chalk.white(`       node index.js --resume "${qi.name}"`));
        console.log(chalk.gray(`     • Or resume the queue in interactive mode.`));
      }

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

      if (qi.reason === 'no-workers') {
        console.log(`  ${chalk.yellow.bold('▸ Status:')}   ${chalk.red.bold('Jobs are waiting/delayed, but ZERO workers are connected!')}`);
        console.log('');
        console.log(chalk.yellow('  💡 Possible Causes:'));
        console.log(chalk.gray(`     • The worker processes for queue "${qi.name}" have not been started or have crashed.`));
        console.log(chalk.gray(`     • The worker processes are connected to a different Redis DB or host.`));
        console.log(chalk.yellow('  💡 Remediation:'));
        console.log(chalk.gray(`     • Restart the application worker process listening to queue "${qi.name}".`));
        console.log(chalk.gray(`     • Check host, port, DB configuration in your worker codebase.`));
      }

      if (qi.reason === 'pending') {
        console.log(`  ${chalk.yellow.bold('▸ Status:')}   ${chalk.yellow('Jobs are waiting and worker(s) are connected, but not processing them')}`);

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
        console.log(chalk.gray(`     • Connected workers are saturated or paused.`));
        console.log(chalk.gray(`     • Concurrency setting on the workers is too low for the current volume.`));
      }

      try { await qi.queue.close(); } catch {}
    }
  }
}

// ─── Test 4: BullMQ Roundtrip ────────────────────────────────────────────────

async function testRoundtrip() {
  divider('4. BullMQ Roundtrip Test (Add → Process → Verify)');
  info('Testing full job lifecycle on a temporary queue\n');

  reportData.roundtrip.run = true;
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
    reportData.roundtrip.job_added = true;

    // Verify it's in waiting
    const waitingBefore = await queue.getWaitingCount();
    if (waitingBefore >= 1) {
      pass(`Job is in waiting state (count: ${waitingBefore})`);
      reportData.roundtrip.job_waiting = true;
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
    reportData.roundtrip.job_consumed = true;
    
    pass(`Payload integrity → verified`);
    reportData.roundtrip.payload_integrity = true;

    // Verify queue is drained
    await sleep(500);
    const waitingAfter = await queue.getWaitingCount();
    const activeAfter = await queue.getActiveCount();
    if (waitingAfter === 0 && activeAfter === 0) {
      pass(`Queue drained → waiting: 0, active: 0`);
      reportData.roundtrip.queue_drained = true;
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
        ...(REDIS_TLS ? { tls: {} } : {}),
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
      reportData.roundtrip.offline_buffering = true;
    } else {
      fail(`Offline buffering → call threw synchronously`);
    }

    try { await offlineQueue.close(); } catch {}

    // Cleanup
    await queue.obliterate({ force: true });
    await queue.close();
    pass(`Cleanup → test queue obliterated`);
    reportData.roundtrip.cleanup = true;

  } catch (err) {
    fail(`Roundtrip failed: ${err.message}`);
    info(`This could mean BullMQ is not correctly installed or Redis is misconfigured.`);
    try { await queue.obliterate({ force: true }); } catch {}
    try { await queue.close(); } catch {}
  }
}

// ─── Test 5: Redis Key Namespace Scan ────────────────────────────────────────

async function testRedisKeysScan() {
  divider('5. Redis Key Namespace Scan & Memory Size');

  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();

    const namespaces = {};
    let cursor = '0';
    let totalKeys = 0;
    let totalMemoryBytes = 0;

    // Scan for bull: keys and measure their memory usage
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*`, 'COUNT', 500);
      cursor = nextCursor;

      for (const key of keys) {
        totalKeys++;
        let keyMem = 0;
        try {
          keyMem = await redis.memory('USAGE', key) || 0;
        } catch {}
        totalMemoryBytes += keyMem;

        const parts = key.split(':');
        if (parts.length >= 2) {
          const queueName = parts[1];
          if (!namespaces[queueName]) {
            namespaces[queueName] = { count: 0, memory: 0 };
          }
          namespaces[queueName].count++;
          namespaces[queueName].memory += keyMem;
        }
      }
    } while (cursor !== '0');

    info(`Total BullMQ keys in Redis: ${chalk.white.bold(totalKeys)} (prefix: "${BULLMQ_PREFIX}")`);
    info(`Total BullMQ memory size:  ${chalk.white.bold(formatBytes(totalMemoryBytes))}\n`);

    if (Object.keys(namespaces).length > 0) {
      printLog(chalk.gray(`  ${pad('Queue Namespace', 34)} ${pad('Keys', 10)} Memory Size`));
      printLog(chalk.gray('  ' + '─'.repeat(58)));

      const sorted = Object.entries(namespaces).sort((a, b) => b[1].memory - a[1].memory);
      for (const [ns, data] of sorted) {
        printLog(`  ${pad(chalk.white(ns), 34)} ${pad(String(data.count), 10)} ${formatBytes(data.memory)}`);
      }
    } else {
      info('No BullMQ keys found (queues may not have been used yet)');
    }

    reportData.keys_scan.total_bullmq_keys = totalKeys;
    reportData.keys_scan.total_bullmq_memory_bytes = totalMemoryBytes;
    reportData.keys_scan.namespaces = {};
    for (const [ns, data] of Object.entries(namespaces)) {
      reportData.keys_scan.namespaces[ns] = {
        keys: data.count,
        memory_bytes: data.memory,
        memory_formatted: formatBytes(data.memory)
      };
    }

    // Scan for non-BullMQ keys
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
      printLog('');
      info(`Other keys in Redis: ${chalk.white.bold(otherKeys)}`);
      const sortedOther = Object.entries(otherPrefixes).sort((a, b) => b[1] - a[1]).slice(0, 10);
      for (const [prefix, count] of sortedOther) {
        printLog(`    ${chalk.gray('•')} ${chalk.white(prefix)}:* → ${count} key(s)`);
      }
    }

    reportData.keys_scan.other_keys = otherKeys;
    reportData.keys_scan.other_prefixes = otherPrefixes;

    await redis.quit();
  } catch (err) {
    fail(`Key scan failed: ${err.message}`);
    try { await redis.quit(); } catch {}
  }
}

// ─── Live Monitoring Mode (Interactive UI Dashboard) ────────────────────────

async function startLiveMode(fromInteractive = false) {
  let iterations = 0;
  let isPausedLive = false;
  let liveInterval = INTERVAL;
  const startTime = Date.now();
  const SPINNER_FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

  // Enable keypress events on stdin
  readline.emitKeypressEvents(process.stdin);
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(true);
  }

  const handleKeypress = async (str, key) => {
    if (key.ctrl && key.name === 'c') {
      cleanupAndExit();
    }

    if (key.name === 'q' || key.name === 'escape') {
      cleanupAndReturn();
    }

    if (key.name === 'space' || key.name === 'p') {
      isPausedLive = !isPausedLive;
      await renderScreen();
      await runScan();
    }

    if (key.name === 'plus' || str === '+') {
      liveInterval = Math.max(1000, liveInterval - 1000); // Speed up
      await renderScreen();
      await runScan();
    }

    if (key.name === 'minus' || str === '-') {
      liveInterval = Math.min(60000, liveInterval + 1000); // Slow down
      await renderScreen();
      await runScan();
    }

    if (key.name === 'r' || key.name === 'c' || key.name === 'f' || key.name === 'i') {
      const wasPaused = isPausedLive;
      isPausedLive = true; // freeze updates during prompting

      if (process.stdin.isTTY) process.stdin.setRawMode(false);
      process.stdin.removeListener('keypress', handleKeypress);

      console.log('\n');
      if (key.name === 'r') {
        const qName = await selectQueuePrompt('Select a queue to retry failed jobs: ');
        if (qName) {
          const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
          q.on('error', () => {});
          try {
            console.log(chalk.cyan(`\n  Retrying failed jobs on "${qName}"...`));
            await q.retryJobs();
            console.log(chalk.green('  Success! Jobs retried.'));
          } catch (err) {
            console.log(chalk.red(`  Error: ${err.message}`));
          } finally {
            try { await q.close(); } catch {}
          }
        }
      } else if (key.name === 'c') {
        const qName = await selectQueuePrompt('Select a queue to clean completed jobs: ');
        if (qName) {
          const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
          q.on('error', () => {});
          try {
            console.log(chalk.cyan(`\n  Cleaning completed jobs on "${qName}"...`));
            await q.clean(0, 0, 'completed');
            console.log(chalk.green('  Success! Completed jobs cleaned.'));
          } catch (err) {
            console.log(chalk.red(`  Error: ${err.message}`));
          } finally {
            try { await q.close(); } catch {}
          }
        }
      } else if (key.name === 'f') {
        const qName = await selectQueuePrompt('Select a queue to clean failed jobs: ');
        if (qName) {
          const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
          q.on('error', () => {});
          try {
            console.log(chalk.cyan(`\n  Cleaning failed jobs on "${qName}"...`));
            await q.clean(0, 0, 'failed');
            console.log(chalk.green('  Success! Failed jobs cleaned.'));
          } catch (err) {
            console.log(chalk.red(`  Error: ${err.message}`));
          } finally {
            try { await q.close(); } catch {}
          }
        }
      } else if (key.name === 'i') {
        await interactiveInspectQueue();
      }

      await askQuestion('\n  Press [Enter] to resume live monitoring.');
      
      isPausedLive = wasPaused; // restore previous state
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdin.on('keypress', handleKeypress);
      
      await renderScreen();
      await runScan();
    }
  };

  process.stdin.on('keypress', handleKeypress);

  function cleanupAndExit() {
    if (process.stdin.isTTY) process.stdin.setRawMode(false);
    console.clear();
    console.log(chalk.cyan.bold('\n  Live monitoring stopped. Goodbye!\n'));
    process.exit(0);
  }

  function cleanupAndReturn() {
    running = false;
    if (process.stdin.isTTY) process.stdin.setRawMode(false);
    process.stdin.removeListener('keypress', handleKeypress);
    console.clear();
    if (!fromInteractive) {
      console.log(chalk.cyan.bold('\n  Live monitoring stopped. Goodbye!\n'));
      process.exit(0);
    }
  }

  async function renderScreen() {
    iterations++;
    console.clear();
    const elapsedSeconds = Math.round((Date.now() - startTime) / 1000);
    const spinner = SPINNER_FRAMES[iterations % SPINNER_FRAMES.length];
    const pauseStateStr = isPausedLive ? chalk.yellow('⏸ FROZEN (Press Space to Resume)') : chalk.cyan(spinner);

    console.log(chalk.cyan.bold('╔══════════════════════════════════════════════════════════════════════════════╗'));
    console.log(chalk.cyan.bold('║          BullMQ & Redis Tester — Live Monitoring Dashboard                   ║'));
    console.log(chalk.cyan.bold('╚══════════════════════════════════════════════════════════════════════════════╝'));
    console.log(chalk.gray(`  Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}   |   Last Scan: ${new Date().toLocaleTimeString()} ${pauseStateStr}`));
    console.log(chalk.gray(`  Interval: ${liveInterval / 1000}s   |   Uptime: ${formatUptime(elapsedSeconds)}   |   Scan #${iterations}   |   PID: ${process.pid}`));
    console.log(chalk.gray(`  Hotkeys: [Space] Pause/Freeze | [+/-] Cycle Speed | [r] Retry | [c] Clean Done`));
    console.log(chalk.gray(`           [f] Clean Fail       | [i] Inspect Queue | [q/Esc] Back/Quit`));
    console.log(chalk.cyan('─'.repeat(79)));
  }

  async function runScan() {
    const redis = new Redis({
      ...REDIS_CONNECTION,
      lazyConnect: true,
      connectTimeout: 2000,
      maxRetriesPerRequest: 1,
      retryStrategy: () => null,
    });

    let redisOk = false;
    let redisVersion = 'Unknown';
    let redisMem = 'Unknown';
    let redisClients = 'Unknown';
    let queueNames = [];

    try {
      await redis.connect();
      const pong = await redis.ping();
      if (pong === 'PONG') redisOk = true;

      const infoStr = await redis.info();
      redisVersion = infoStr.match(/redis_version:(.+)/)?.[1]?.trim() || 'Unknown';
      redisMem = infoStr.match(/used_memory_human:(.+)/)?.[1]?.trim() || 'Unknown';
      redisClients = infoStr.match(/connected_clients:(\d+)/)?.[1] || 'Unknown';

      const discoveredSet = new Set();
      let cursor = '0';
      do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:meta`, 'COUNT', 250);
        cursor = nextCursor;
        for (const key of keys) {
          const parts = key.split(':');
          if (parts.length >= 3) discoveredSet.add(parts[1]);
        }
      } while (cursor !== '0');

      if (discoveredSet.size === 0) {
        cursor = '0';
        do {
          const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:id`, 'COUNT', 250);
          cursor = nextCursor;
          for (const key of keys) {
            const parts = key.split(':');
            if (parts.length >= 3) discoveredSet.add(parts[1]);
          }
        } while (cursor !== '0');
      }

      queueNames = [...discoveredSet].sort();
    } catch (err) {
      redisOk = false;
    }

    if (!redisOk) {
      console.log(`  Redis Status: ${chalk.red.bold('✗ OFFLINE')} (Cannot reach Redis at ${REDIS_HOST}:${REDIS_PORT})`);
      try { await redis.quit(); } catch {}
      return;
    }

    console.log(`  Redis Status: ${chalk.green.bold('● ONLINE')}   Version: ${chalk.white.bold(redisVersion)}   Memory: ${chalk.white.bold(redisMem)}   Clients: ${chalk.white.bold(redisClients)}`);
    console.log(chalk.cyan('─'.repeat(79)));

    if (queueNames.length === 0) {
      console.log(`\n  ${chalk.yellow('No BullMQ queues found in Redis.')}`);
      console.log(chalk.gray(`  (Looked for keys matching prefix: "${BULLMQ_PREFIX}")`));
    } else {
      console.log(chalk.bold(`  Discovered Queues (${queueNames.length}):`));
      console.log('');
      console.log(chalk.gray(
        `  ${pad('Queue Name', 24)} ${pad('Wait', 6)} ${pad('Active', 6)} ${pad('Delay', 6)} ${pad('Failed', 6)} ${pad('Done', 6)} ${pad('Workers', 8)} Status`
      ));
      console.log(chalk.gray('  ' + '─'.repeat(78)));

      const queuesWithFailures = [];
      const queuesWithWarnings = [];

      for (const name of queueNames) {
        const q = new Queue(name, { connection: redis, prefix: BULLMQ_PREFIX });
        q.on('error', () => {});

        try {
          const [waiting, active, delayed, failed, completed, workersCount, isPaused] = await Promise.all([
            q.getWaitingCount(),
            q.getActiveCount(),
            q.getDelayedCount(),
            q.getFailedCount(),
            q.getCompletedCount(),
            q.getWorkersCount().catch(() => 0),
            q.isPaused().catch(() => false)
          ]);

          let status;
          if (failed > MAX_FAILED) {
            status = chalk.red.bold('🚨 CRITICAL');
            queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'critical' });
          } else if (waiting > MAX_WAIT) {
            status = chalk.red.bold('🚨 OVERLOAD');
            queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'overload' });
          } else if (failed > 0) {
            status = chalk.red('● FAILURES');
            queuesWithFailures.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'failed' });
          } else if (isPaused) {
            status = chalk.yellow('⏸ PAUSED');
            queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'paused' });
          } else if (active > 0) {
            status = chalk.green('● ACTIVE');
            queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'active' });
          } else if (workersCount === 0 && (waiting > 0 || delayed > 0)) {
            status = chalk.red('● NO WORKERS');
            queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'no-workers' });
          } else if (waiting > 0 || delayed > 0) {
            status = chalk.yellow('● PENDING');
            queuesWithWarnings.push({ name, waiting, active, delayed, failed, completed, workersCount, isPaused, queue: q, reason: 'pending' });
          } else {
            status = chalk.gray('○ IDLE');
            try { await q.close(); } catch {}
          }

          console.log(
            `  ${pad(name, 24)} ${pad(String(waiting), 6)} ${pad(String(active), 6)} ${pad(String(delayed), 6)} ${pad(String(failed), 6)} ${pad(String(completed), 6)} ${pad(String(workersCount), 8)} ${status}`
          );
        } catch (err) {
          console.log(`  ${pad(name, 24)} ${chalk.red('ERROR: ' + err.message.substring(0, 30))}`);
          try { await q.close(); } catch {}
        }
      }

      // Compact issue summaries
      if (queuesWithFailures.length > 0) {
        console.log('');
        console.log(chalk.red.bold('  ── DETECTED QUEUE ISSUES ──────────────────────────────────────────'));
        let issueCount = 0;
        for (const qi of queuesWithFailures) {
          if (issueCount >= 3) {
            try { await qi.queue.close(); } catch {}
            continue;
          }
          if (qi.reason === 'critical') {
            console.log(`  ${chalk.red.bold('🚨')} ${chalk.white.bold(qi.name)}: CRITICAL failed jobs (${qi.failed} > threshold of ${MAX_FAILED})`);
          } else if (qi.reason === 'overload') {
            console.log(`  ${chalk.red.bold('🚨')} ${chalk.white.bold(qi.name)}: OVERLOADED wait queue (${qi.waiting} > threshold of ${MAX_WAIT})`);
          } else {
            try {
              const failedJobs = await qi.queue.getFailed(0, 1);
              if (failedJobs.length > 0) {
                const fj = failedJobs[0];
                const age = fj.timestamp ? formatUptime(Math.round((Date.now() - fj.timestamp) / 1000)) : 'unknown';
                console.log(`  ${chalk.red('•')} ${chalk.white.bold(qi.name)} (Job #${fj.id || 'N/A'}: "${fj.name || 'N/A'}")`);
                console.log(`    ${chalk.gray('Error:')} ${chalk.red.bold(fj.failedReason || 'Unknown error')}`);
                console.log(`    ${chalk.gray('Failed:')} ${age} ago`);
              }
            } catch {}
          }
          issueCount++;
          try { await qi.queue.close(); } catch {}
        }
      }

      // Compact paused summaries
      const pausedQueues = queuesWithWarnings.filter(w => w.reason === 'paused');
      if (pausedQueues.length > 0) {
        console.log('');
        console.log(chalk.yellow.bold('  ── PAUSED QUEUES ──────────────────────────────────────────────────'));
        for (const qi of pausedQueues) {
          console.log(`  ${chalk.yellow('⏸')} ${chalk.white.bold(qi.name)} is paused. Jobs waiting: ${qi.waiting}, delayed: ${qi.delayed}.`);
          try { await qi.queue.close(); } catch {}
        }
      }

      // Compact no workers summaries
      const idleQueuesWithJobs = queuesWithWarnings.filter(w => w.reason === 'no-workers');
      if (idleQueuesWithJobs.length > 0) {
        console.log('');
        console.log(chalk.red.bold('  ── QUEUES WITH NO WORKERS ─────────────────────────────────────────'));
        let warningCount = 0;
        for (const qi of idleQueuesWithJobs) {
          if (warningCount >= 3) {
            try { await qi.queue.close(); } catch {}
            continue;
          }
          console.log(`  ${chalk.red('•')} ${chalk.white.bold(qi.name)} has ${chalk.red(qi.waiting)} waiting and ${chalk.cyan(qi.delayed)} delayed jobs but ZERO connected workers!`);
          warningCount++;
          try { await qi.queue.close(); } catch {}
        }
      }
    }

    try { await redis.quit(); } catch {}
  }

  // Polling recursion
  let running = true;
  async function poll() {
    if (!running) return;

    if (!isPausedLive) {
      await renderScreen();
      await runScan();
    }

    setTimeout(poll, liveInterval);
  }

  poll();
}

// ─── Maintenance & Pruning Operations ────────────────────────────────────────

async function runPruningAndAutoRetry() {
  if (!AUTO_RETRY_FAILED_FLAG && !PRUNE_COMPLETED_HOURS && !PRUNE_FAILED_HOURS) {
    return;
  }

  divider('Maintenance Tasks');
  const queueNames = await discoverQueues();
  if (queueNames.length === 0) {
    info('No queues found for maintenance.');
    return;
  }

  for (const name of queueNames) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    try {
      if (AUTO_RETRY_FAILED_FLAG) {
        const failedCount = await q.getFailedCount();
        if (failedCount > 0) {
          info(`Queue "${name}": Auto-retrying ${failedCount} failed jobs...`);
          await q.retryJobs();
          pass(`Queue "${name}": Retried ${failedCount} jobs.`);
        }
      }

      if (PRUNE_COMPLETED_HOURS) {
        const hours = parseInt(PRUNE_COMPLETED_HOURS, 10);
        if (!isNaN(hours)) {
          const ms = hours * 3600000;
          info(`Queue "${name}": Pruning completed jobs older than ${hours}h...`);
          const cleaned = await q.clean(ms, 0, 'completed');
          pass(`Queue "${name}": Pruned ${cleaned.length} completed jobs.`);
        }
      }

      if (PRUNE_FAILED_HOURS) {
        const hours = parseInt(PRUNE_FAILED_HOURS, 10);
        if (!isNaN(hours)) {
          const ms = hours * 3600000;
          info(`Queue "${name}": Pruning failed jobs older than ${hours}h...`);
          const cleaned = await q.clean(ms, 0, 'failed');
          pass(`Queue "${name}": Pruned ${cleaned.length} failed jobs.`);
        }
      }
    } catch (err) {
      fail(`Queue "${name}": Maintenance error: ${err.message}`);
    } finally {
      try { await q.close(); } catch {}
    }
  }
}

// ─── TUI Terminal User Interface Dashboard ───────────────────────────────────

const tuiState = {
  activeQueueIndex: 0,
  queues: [],
  focus: 'queues', // 'queues' or 'actions'
  redisInfo: { version: '', memory: '', clients: '', latency: undefined },
  redisOk: false,
  logMessage: 'Initializing TUI...',
  logColor: 'cyan',
  selectedActionIndex: 0,
  actions: [
    { label: 'Retry', key: 'r' },
    { label: 'Clean Ok', key: 'c' },
    { label: 'Clean Fail', key: 'f' },
    { label: 'Pause/Resume', key: 't' },
    { label: 'Add Job', key: 'a' },
    { label: 'Search', key: 's' },
    { label: 'Drain', key: 'd' },
    { label: 'Obliterate', key: 'o' }
  ]
};

let tuiScrollTop = 0;

function drawTUI() {
  const cols = process.stdout.columns || 80;
  const rows = process.stdout.rows || 24;

  if (cols < 80 || rows < 20) {
    console.clear();
    console.log(chalk.red('Terminal window too small for TUI. Please resize to at least 80x20.'));
    return;
  }

  const lines = [];

  // Header border
  const title = ` BullMQ & Redis Tester TUI `;
  const targetStr = ` Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB} `;
  const statusStr = tuiState.redisOk ? chalk.green.bold('● ONLINE') : chalk.red.bold('✗ OFFLINE');
  
  const cleanHeaderLen = 1 + title.length + 3 + targetStr.length + 3 + 8 + 1;
  const rightPad = Math.max(0, cols - 2 - cleanHeaderLen);
  
  lines.push(chalk.cyan('┌' + '─'.repeat(cols - 2) + '┐'));
  lines.push(chalk.cyan('│') + chalk.bold.cyan(title) + chalk.cyan('│') + targetStr + chalk.cyan('│') + ` Status: ${statusStr}` + ' '.repeat(rightPad) + chalk.cyan('│'));
  
  // Second header line: Redis metrics info
  let infoLineText = '';
  if (tuiState.redisOk) {
    const rtt = tuiState.redisInfo.latency !== undefined ? `${tuiState.redisInfo.latency}ms` : '...';
    infoLineText = ` Redis v${tuiState.redisInfo.version}  |  Memory: ${tuiState.redisInfo.memory}  |  Clients: ${tuiState.redisInfo.clients}  |  RTT: ${rtt}`;
  } else {
    infoLineText = ' Redis: Disconnected';
  }
  const cleanInfoLen = infoLineText.length;
  const infoPad = Math.max(0, cols - 2 - cleanInfoLen);
  lines.push(chalk.cyan('│') + chalk.gray(infoLineText) + ' '.repeat(infoPad) + chalk.cyan('│'));
  
  lines.push(chalk.cyan('├' + '─'.repeat(28) + '┬' + '─'.repeat(cols - 31) + '┤'));

  // Sidebar List
  const sidebarWidth = 28;
  const mainWidth = cols - sidebarWidth - 3;
  const activeQueue = tuiState.queues[tuiState.activeQueueIndex] || null;

  const maxListRows = rows - 8; // Spacing logic (Header: 4, Separator: 1, Content, Separator: 1, Help: 1, Log: 1, Border: 1 -> Total = 8)
  if (tuiState.activeQueueIndex < tuiScrollTop) {
    tuiScrollTop = tuiState.activeQueueIndex;
  } else if (tuiState.activeQueueIndex >= tuiScrollTop + maxListRows) {
    tuiScrollTop = tuiState.activeQueueIndex - maxListRows + 1;
  }

  for (let i = 0; i < maxListRows; i++) {
    const qIdx = tuiScrollTop + i;
    let sidebarText = '';
    
    if (qIdx < tuiState.queues.length) {
      const q = tuiState.queues[qIdx];
      const displayName = q.name.length > 13 ? q.name.substring(0, 13) + '..' : q.name;
      const countInfo = `[${q.waiting + q.active + q.failed}]`;
      
      let statusIcon = '●';
      let coloredIcon = statusIcon;
      if (q.status === 'FAILURES' || q.status === 'CRITICAL_FAILURES') {
        statusIcon = '✗';
        coloredIcon = chalk.red(statusIcon);
      } else if (q.status === 'PAUSED') {
        statusIcon = '⏸';
        coloredIcon = chalk.yellow(statusIcon);
      } else if (q.status === 'ACTIVE') {
        statusIcon = '▶';
        coloredIcon = chalk.green(statusIcon);
      } else if (q.status === 'NO_WORKERS') {
        statusIcon = '⚠';
        coloredIcon = chalk.yellow.italic(statusIcon);
      } else {
        coloredIcon = chalk.gray(statusIcon);
      }

      const cleanItemStr = ` ${statusIcon} ${qIdx + 1}. ${displayName} ${countInfo}`;
      const sidebarPad = Math.max(0, sidebarWidth - cleanItemStr.length);
      
      if (qIdx === tuiState.activeQueueIndex) {
        const paddedClean = cleanItemStr + ' '.repeat(sidebarPad);
        if (tuiState.focus === 'queues') {
          sidebarText = chalk.bgCyan.black(paddedClean);
        } else {
          sidebarText = chalk.bgGray.white(paddedClean);
        }
      } else {
        const itemStr = ` ${coloredIcon} ${qIdx + 1}. ${displayName} ${countInfo}` + ' '.repeat(sidebarPad);
        if (q.status === 'FAILURES' || q.status === 'CRITICAL_FAILURES') {
          sidebarText = chalk.red(itemStr);
        } else if (q.status === 'PAUSED') {
          sidebarText = chalk.yellow(itemStr);
        } else if (q.status === 'NO_WORKERS') {
          sidebarText = chalk.red.italic(itemStr);
        } else if (q.status === 'ACTIVE') {
          sidebarText = chalk.green(itemStr);
        } else {
          sidebarText = chalk.white(itemStr);
        }
      }
    } else {
      sidebarText = ' '.repeat(sidebarWidth);
    }

    let mainText = '';
    if (activeQueue) {
      mainText = getQueueDetailsRow(activeQueue, i, mainWidth);
    } else {
      if (i === 2) {
        mainText = chalk.gray('  Scanning for BullMQ queues...');
      } else {
        mainText = ' '.repeat(mainWidth);
      }
    }

    const cleanMainText = mainText.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, '');
    const mainPad = Math.max(0, mainWidth - cleanMainText.length);
    const mainTextPadded = mainText + ' '.repeat(mainPad);

    lines.push(chalk.cyan('│') + sidebarText + chalk.cyan('│') + mainTextPadded + chalk.cyan('│'));
  }

  // Footer separator
  lines.push(chalk.cyan('├' + '─'.repeat(28) + '┴' + '─'.repeat(cols - 31) + '┤'));
  
  // Help Legend Line
  const helpLegend = ' Keys: [Tab] Focus | [↑/↓] Queues | [←/→] Actions | [Space] Pause | [Enter] Run | [R] Refresh | [Q] Exit';
  const cleanLegendLen = helpLegend.length;
  const legendPad = Math.max(0, cols - 2 - cleanLegendLen);
  lines.push(chalk.cyan('│') + chalk.gray(helpLegend) + ' '.repeat(legendPad) + chalk.cyan('│'));
  
  // Log message line
  const logText = ` Log: ${tuiState.logMessage}`;
  const cleanLogLen = logText.length;
  const logPad = Math.max(0, cols - 2 - cleanLogLen);
  let styledLog = chalk.gray(' Log: ') + chalk[tuiState.logColor || 'white'](tuiState.logMessage);
  
  lines.push(chalk.cyan('│') + styledLog + ' '.repeat(logPad) + chalk.cyan('│'));
  lines.push(chalk.cyan('└' + '─'.repeat(cols - 2) + '┘'));

  process.stdout.write('\x1b[H' + lines.join('\n'));
}

function getQueueDetailsRow(q, rowIdx, width) {
  const isPaused = q.isPaused;
  const pauseStr = isPaused ? chalk.yellow('PAUSED ⏸') : chalk.green('ACTIVE ▶');
  
  switch (rowIdx) {
    case 0: {
      const maxNameLen = Math.max(10, width - 22);
      const displayName = q.name.length > maxNameLen ? q.name.substring(0, maxNameLen - 2) + '..' : q.name;
      return `  ${chalk.bold.white('Queue:')} ${chalk.bold.cyan(displayName)}  [${pauseStr}]`;
    }
    case 1:
      return ' ' + '─'.repeat(Math.max(0, width - 2));
    case 2:
      return `  ${chalk.bold('Job Counts:')}`;
    case 3:
      return `    Waiting:   ${chalk.cyan(q.waiting.toString().padEnd(10))} Active:    ${chalk.green(q.active.toString().padEnd(10))}`;
    case 4:
      return `    Delayed:   ${chalk.blue(q.delayed.toString().padEnd(10))} Failed:    ${chalk.red(q.failed.toString().padEnd(10))}`;
    case 5:
      return `    Completed: ${chalk.green(q.completed.toString().padEnd(10))}`;
    case 6: {
      const keysStr = q.keysCount !== undefined ? q.keysCount.toString() : '...';
      const memStr = q.memory !== undefined ? formatBytes(q.memory) : '...';
      return `    Keys (Redis):  ${chalk.cyan(keysStr.padEnd(8))} Memory:    ${chalk.cyan(memStr)}`;
    }
    case 7:
      return '';
    case 8:
      return `  ${chalk.bold(`Workers Connected (${q.workersCount}):`)}`;
    case 9:
      if (q.workers && q.workers.length > 0) {
        const w = q.workers[0];
        const idVal = w.id || 'N/A';
        const addrVal = w.addr || 'N/A';
        const libVal = `${w['lib-name'] || 'bullmq'}@${w['lib-ver'] || ''}`;
        const line = `    • ID: ${idVal} | IP: ${addrVal} | Lib: ${libVal}`;
        return line.length > width - 1 ? line.substring(0, width - 3) + '..' : line;
      } else {
        return chalk.yellow('    No active workers connected.');
      }
    case 10:
      if (q.workers && q.workers.length > 1) {
        const w = q.workers[1];
        const idVal = w.id || 'N/A';
        const addrVal = w.addr || 'N/A';
        const libVal = `${w['lib-name'] || 'bullmq'}@${w['lib-ver'] || ''}`;
        const line = `    • ID: ${idVal} | IP: ${addrVal} | Lib: ${libVal}`;
        return line.length > width - 1 ? line.substring(0, width - 3) + '..' : line;
      }
      return '';
    case 11:
      return '';
    case 12:
      return `  ${chalk.bold('Quick Actions (Tab to focus):')}`;
    case 13: {
      let buttons = '   ';
      for (let idx = 0; idx < 4; idx++) {
        const act = tuiState.actions[idx];
        if (!act) continue;
        const label = ` ${act.label} `;
        if (tuiState.focus === 'actions' && idx === tuiState.selectedActionIndex) {
          buttons += chalk.bgCyan.black(label) + '  ';
        } else {
          buttons += chalk.bgGray.black(label) + '  ';
        }
      }
      return buttons;
    }
    case 14: {
      let buttons = '   ';
      for (let idx = 4; idx < tuiState.actions.length; idx++) {
        const act = tuiState.actions[idx];
        if (!act) continue;
        const label = ` ${act.label} `;
        if (tuiState.focus === 'actions' && idx === tuiState.selectedActionIndex) {
          buttons += chalk.bgCyan.black(label) + '  ';
        } else {
          buttons += chalk.bgGray.black(label) + '  ';
        }
      }
      return buttons;
    }
    case 15:
      return '';
    default:
      if (q.failed > 0) {
        const failedOffset = 16;
        if (rowIdx === failedOffset) {
          return `  ${chalk.red.bold('Recent Failures:')}`;
        }
        if (q.recentFailed && q.recentFailed.length > 0) {
          const fIdx = rowIdx - failedOffset - 1;
          if (fIdx < q.recentFailed.length) {
            const fj = q.recentFailed[fIdx];
            const line = `    • Job #${fj.id}: ${fj.name} → "${fj.failedReason}"`;
            return chalk.red(line.length > width - 1 ? line.substring(0, width - 3) + '..' : line);
          }
        }
      }
      return '';
  }
}

async function startTUIScanner() {
  tuiState.redisOk = false;
  
  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();
    tuiState.redisOk = true;
    
    // Measure Redis response RTT
    const rttStart = Date.now();
    await redis.ping();
    tuiState.redisInfo.latency = Date.now() - rttStart;
    
    const infoStr = await redis.info();
    tuiState.redisInfo.version = infoStr.match(/redis_version:(.+)/)?.[1]?.trim() || 'Unknown';
    tuiState.redisInfo.memory = infoStr.match(/used_memory_human:(.+)/)?.[1]?.trim() || 'Unknown';
    tuiState.redisInfo.clients = infoStr.match(/connected_clients:(\d+)/)?.[1] || 'Unknown';

    const discoveredSet = new Set();
    let cursor = '0';
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:meta`, 'COUNT', 250);
      cursor = nextCursor;
      for (const key of keys) {
        const parts = key.split(':');
        if (parts.length >= 3) discoveredSet.add(parts[1]);
      }
    } while (cursor !== '0');

    if (discoveredSet.size === 0) {
      cursor = '0';
      do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', `${BULLMQ_PREFIX}:*:id`, 'COUNT', 250);
        cursor = nextCursor;
        for (const key of keys) {
          const parts = key.split(':');
          if (parts.length >= 3) discoveredSet.add(parts[1]);
        }
      } while (cursor !== '0');
    }

    const queueNames = [...discoveredSet].sort();
    const updatedQueues = [];
    
    // Find name of the active queue to scan its memory
    const activeQueueName = queueNames[tuiState.activeQueueIndex] || null;
    
    for (const name of queueNames) {
      const q = new Queue(name, { connection: redis, prefix: BULLMQ_PREFIX });
      q.on('error', () => {});
      
      try {
        const [waiting, active, delayed, failed, completed, workersCount, isPaused] = await Promise.all([
          q.getWaitingCount(),
          q.getActiveCount(),
          q.getDelayedCount(),
          q.getFailedCount(),
          q.getCompletedCount(),
          q.getWorkersCount().catch(() => 0),
          q.isPaused().catch(() => false)
        ]);

        let status = 'IDLE';
        if (failed > MAX_FAILED) status = 'CRITICAL_FAILURES';
        else if (waiting > MAX_WAIT) status = 'OVERLOADED';
        else if (failed > 0) status = 'FAILURES';
        else if (isPaused) status = 'PAUSED';
        else if (active > 0) status = 'ACTIVE';
        else if (workersCount === 0 && (waiting > 0 || delayed > 0)) status = 'NO_WORKERS';
        else if (waiting > 0 || delayed > 0) status = 'PENDING';

        let workers = [];
        if (workersCount > 0) {
          workers = await q.getWorkers().catch(() => []);
        }

        let recentFailed = [];
        if (failed > 0) {
          const jobs = await q.getFailed(0, 3).catch(() => []);
          recentFailed = jobs.map(fj => ({
            id: fj.id,
            name: fj.name,
            failedReason: fj.failedReason ? (fj.failedReason.length > 40 ? fj.failedReason.substring(0, 40) + '...' : fj.failedReason) : 'Unknown'
          }));
        }

        let queueMemory = undefined;
        let queueKeysCount = undefined;
        if (name === activeQueueName) {
          queueMemory = 0;
          queueKeysCount = 0;
          let qCursor = '0';
          do {
            const [nextQCursor, qKeys] = await redis.scan(qCursor, 'MATCH', `${BULLMQ_PREFIX}:${name}:*`, 'COUNT', 100);
            qCursor = nextQCursor;
            for (const qKey of qKeys) {
              queueKeysCount++;
              try {
                const mem = await redis.memory('USAGE', qKey) || 0;
                queueMemory += mem;
              } catch {}
            }
          } while (qCursor !== '0');
        }

        updatedQueues.push({
          name,
          waiting,
          active,
          delayed,
          failed,
          completed,
          workersCount,
          isPaused,
          status,
          workers,
          recentFailed,
          queue: q,
          memory: queueMemory,
          keysCount: queueKeysCount
        });
      } catch (err) {
        try { await q.close(); } catch {}
      }
    }
    
    // Close old queue connections in background
    tuiState.queues.forEach(oq => {
      try { oq.queue.close(); } catch {}
    });

    tuiState.queues = updatedQueues;
    
    if (tuiState.activeQueueIndex >= tuiState.queues.length) {
      tuiState.activeQueueIndex = Math.max(0, tuiState.queues.length - 1);
    }

  } catch (err) {
    tuiState.redisOk = false;
    tuiState.logMessage = `Redis connection error: ${err.message}`;
    tuiState.logColor = 'red';
  } finally {
    try { await redis.quit(); } catch {}
  }
}

async function refreshActiveQueueMemory() {
  const activeQ = tuiState.queues[tuiState.activeQueueIndex];
  if (!activeQ) return;
  
  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });
  
  try {
    await redis.connect();
    let queueMemory = 0;
    let queueKeysCount = 0;
    let qCursor = '0';
    do {
      const [nextQCursor, qKeys] = await redis.scan(qCursor, 'MATCH', `${BULLMQ_PREFIX}:${activeQ.name}:*`, 'COUNT', 100);
      qCursor = nextQCursor;
      for (const qKey of qKeys) {
        queueKeysCount++;
        try {
          const mem = await redis.memory('USAGE', qKey) || 0;
          queueMemory += mem;
        } catch {}
      }
    } while (qCursor !== '0');
    
    if (tuiState.queues[tuiState.activeQueueIndex]?.name === activeQ.name) {
      tuiState.queues[tuiState.activeQueueIndex].memory = queueMemory;
      tuiState.queues[tuiState.activeQueueIndex].keysCount = queueKeysCount;
      drawTUI();
    }
  } catch {} finally {
    try { await redis.quit(); } catch {}
  }
}

async function runTUIAction(qName, actionKey) {
  tuiState.logMessage = `Running action on "${qName}"...`;
  tuiState.logColor = 'cyan';
  drawTUI();

  const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});

  try {
    if (actionKey === 'r') {
      const count = await q.getFailedCount();
      if (count === 0) {
        tuiState.logMessage = `Queue "${qName}" has no failed jobs.`;
        tuiState.logColor = 'yellow';
      } else {
        await q.retryJobs();
        tuiState.logMessage = `Retried ${count} failed jobs on "${qName}".`;
        tuiState.logColor = 'green';
      }
    } else if (actionKey === 'c') {
      await q.clean(0, 0, 'completed');
      tuiState.logMessage = `Cleaned completed jobs on "${qName}".`;
      tuiState.logColor = 'green';
    } else if (actionKey === 'f') {
      await q.clean(0, 0, 'failed');
      tuiState.logMessage = `Cleaned failed jobs on "${qName}".`;
      tuiState.logColor = 'green';
    } else if (actionKey === 't') {
      const isPaused = await q.isPaused();
      if (isPaused) {
        await q.resume();
        tuiState.logMessage = `Resumed queue "${qName}".`;
        tuiState.logColor = 'green';
      } else {
        await q.pause();
        tuiState.logMessage = `Paused queue "${qName}".`;
        tuiState.logColor = 'yellow';
      }
    } else if (actionKey === 'a') {
      if (process.stdin.isTTY) process.stdin.setRawMode(false);
      process.stdout.write('\x1b[?25h\x1b[2J\x1b[H');
      
      console.log(chalk.cyan.bold(`  --- Add Test Job to Queue: ${qName} ---`));
      const jobName = await askQuestion('\n  Enter job name (default: test-job): ') || 'test-job';
      const jobDataStr = await askQuestion('  Enter payload JSON string (default: {}): ');
      let jobData = {};
      try {
        if (jobDataStr.trim()) jobData = JSON.parse(jobDataStr);
      } catch {
        console.log(chalk.red('  Invalid JSON. Using empty object payload.'));
      }
      
      await q.add(jobName, jobData);
      console.log(chalk.green(`\n  Success! Job added.`));
      await sleep(1500);
      
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdout.write('\x1b[?25l');
      tuiState.logMessage = `Added test job "${jobName}" to "${qName}".`;
      tuiState.logColor = 'green';
    } else if (actionKey === 's') {
      if (process.stdin.isTTY) process.stdin.setRawMode(false);
      process.stdout.write('\x1b[?25h\x1b[2J\x1b[H');
      
      console.log(chalk.cyan.bold(`  --- Search Jobs in Queue: ${qName} ---`));
      const query = await askQuestion('\n  Enter job ID, name, or payload search term: ');
      if (query.trim()) {
        console.log(chalk.cyan('\n  Searching jobs...'));
        
        const states = ['failed', 'completed', 'active', 'waiting', 'delayed'];
        let foundCount = 0;
        
        for (const state of states) {
          const jobs = await q.getJobs([state], 0, 100);
          for (const job of jobs) {
            const matchesId = job.id && String(job.id).includes(query);
            const matchesName = job.name && String(job.name).includes(query);
            const matchesPayload = job.data && JSON.stringify(job.data).includes(query);
            
            if (matchesId || matchesName || matchesPayload) {
              foundCount++;
              console.log(`\n  ${chalk.cyan(`[${state.toUpperCase()}]`)} Job ID: ${chalk.white.bold(job.id)} | Name: ${chalk.white(job.name)}`);
              console.log(`    Created: ${new Date(job.timestamp).toLocaleString()}`);
              if (job.failedReason) console.log(`    Error: ${chalk.red(job.failedReason)}`);
              console.log(`    Payload: ${chalk.yellow(JSON.stringify(job.data, null, 2))}`);
              if (foundCount >= 10) {
                console.log(chalk.yellow('\n  Showing first 10 matches. Search truncated.'));
                break;
              }
            }
          }
          if (foundCount >= 10) break;
        }
        if (foundCount === 0) {
          console.log(chalk.yellow('  No matching jobs found.'));
        }
        await askQuestion('\n  Press [Enter] to continue.');
      }
      
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdout.write('\x1b[?25l');
      tuiState.logMessage = `Searched jobs in "${qName}".`;
      tuiState.logColor = 'cyan';
    } else if (actionKey === 'd') {
      if (process.stdin.isTTY) process.stdin.setRawMode(false);
      process.stdout.write('\x1b[?25h\x1b[2J\x1b[H');
      
      console.log(chalk.yellow.bold(`  --- DRAIN QUEUE: ${qName} ---`));
      const confirm = await askQuestion(`\n  Are you sure you want to delete ALL jobs in "${qName}"? (y/N): `);
      if (confirm.toLowerCase() === 'y') {
        await q.drain(true);
        console.log(chalk.green('\n  Success! Queue drained.'));
        tuiState.logMessage = `Drained queue "${qName}".`;
        tuiState.logColor = 'green';
      } else {
        tuiState.logMessage = 'Drain cancelled.';
        tuiState.logColor = 'gray';
      }
      await sleep(1500);
      
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdout.write('\x1b[?25l');
    } else if (actionKey === 'o') {
      if (process.stdin.isTTY) process.stdin.setRawMode(false);
      process.stdout.write('\x1b[?25h\x1b[2J\x1b[H');
      
      console.log(chalk.red.bold(`  --- OBLITERATE QUEUE: ${qName} ---`));
      console.log(chalk.red('  This will delete all jobs, events, state, and keys associated with this queue.'));
      const confirm = await askQuestion(`\n  Type "${qName}" to confirm obliteration: `);
      if (confirm === qName) {
        await q.obliterate({ force: true });
        console.log(chalk.green('\n  Success! Queue obliterated.'));
        tuiState.logMessage = `Obliterated queue "${qName}".`;
        tuiState.logColor = 'red';
      } else {
        tuiState.logMessage = 'Obliteration cancelled.';
        tuiState.logColor = 'gray';
      }
      await sleep(1500);
      
      if (process.stdin.isTTY) process.stdin.setRawMode(true);
      process.stdout.write('\x1b[?25l');
    }
    
    await startTUIScanner();
    drawTUI();
  } catch (err) {
    tuiState.logMessage = `Action failed: ${err.message}`;
    tuiState.logColor = 'red';
    drawTUI();
  } finally {
    try { await q.close(); } catch {}
  }
}

async function startTUI() {
  console.clear();
  tuiState.logMessage = 'Connecting to Redis...';
  tuiState.logColor = 'cyan';
  
  await startTUIScanner();
  tuiState.logMessage = 'Discovered ' + tuiState.queues.length + ' queues. Ready.';
  tuiState.logColor = 'green';
  
  drawTUI();

  let running = true;
  const pollInterval = setInterval(async () => {
    if (!running) return;
    await startTUIScanner();
    drawTUI();
  }, 3000);

  readline.emitKeypressEvents(process.stdin);
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(true);
  }

  process.stdout.write('\x1b[?25l');

  const handleTUIKeypress = async (str, key) => {
    if (key.ctrl && key.name === 'c') {
      cleanupTUI();
      process.exit(0);
    }
    
    if (key.name === 'q' || key.name === 'escape') {
      cleanupTUI();
      return;
    }

    if (key.name === 'up' || key.name === 'w') {
      if (tuiState.focus === 'queues') {
        if (tuiState.activeQueueIndex > 0) {
          tuiState.activeQueueIndex--;
          drawTUI();
          refreshActiveQueueMemory();
        }
      } else {
        if (tuiState.selectedActionIndex >= 4) {
          tuiState.selectedActionIndex -= 4;
          drawTUI();
        }
      }
    }

    if (key.name === 'down' || key.name === 's') {
      if (tuiState.focus === 'queues') {
        if (tuiState.activeQueueIndex < tuiState.queues.length - 1) {
          tuiState.activeQueueIndex++;
          drawTUI();
          refreshActiveQueueMemory();
        }
      } else {
        if (tuiState.selectedActionIndex < 4 && tuiState.selectedActionIndex + 4 < tuiState.actions.length) {
          tuiState.selectedActionIndex += 4;
          drawTUI();
        }
      }
    }

    if (key.name === 'tab') {
      tuiState.focus = tuiState.focus === 'queues' ? 'actions' : 'queues';
      drawTUI();
    }

    if (key.name === 'left' || key.name === 'a') {
      if (tuiState.focus === 'actions') {
        if (tuiState.selectedActionIndex > 0) {
          tuiState.selectedActionIndex--;
          drawTUI();
        }
      }
    }

    if (key.name === 'right' || key.name === 'd') {
      if (tuiState.focus === 'actions') {
        if (tuiState.selectedActionIndex < tuiState.actions.length - 1) {
          tuiState.selectedActionIndex++;
          drawTUI();
        }
      }
    }

    if (key.name === 'r') {
      tuiState.logMessage = 'Refreshing data...';
      tuiState.logColor = 'cyan';
      drawTUI();
      await startTUIScanner();
      tuiState.logMessage = 'Refreshed successfully.';
      tuiState.logColor = 'green';
      drawTUI();
    }

    if (key.name === 'space' || key.name === 'p') {
      const activeQ = tuiState.queues[tuiState.activeQueueIndex];
      if (activeQ) {
        const q = new Queue(activeQ.name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
        q.on('error', () => {});
        try {
          if (activeQ.isPaused) {
            tuiState.logMessage = `Resuming queue "${activeQ.name}"...`;
            tuiState.logColor = 'cyan';
            drawTUI();
            await q.resume();
            tuiState.logMessage = `Queue "${activeQ.name}" resumed.`;
            tuiState.logColor = 'green';
          } else {
            tuiState.logMessage = `Pausing queue "${activeQ.name}"...`;
            tuiState.logColor = 'cyan';
            drawTUI();
            await q.pause();
            tuiState.logMessage = `Queue "${activeQ.name}" paused.`;
            tuiState.logColor = 'yellow';
          }
          await startTUIScanner();
          drawTUI();
        } catch (err) {
          tuiState.logMessage = `Toggle failed: ${err.message}`;
          tuiState.logColor = 'red';
          drawTUI();
        } finally {
          try { await q.close(); } catch {}
        }
      }
    }

    if (key.name === 'return') {
      if (tuiState.focus === 'actions') {
        const activeQ = tuiState.queues[tuiState.activeQueueIndex];
        const action = tuiState.actions[tuiState.selectedActionIndex];
        if (activeQ && action) {
          await runTUIAction(activeQ.name, action.key);
        }
      }
    }
  };

  process.stdin.on('keypress', handleTUIKeypress);

  const handleResize = () => {
    drawTUI();
  };
  process.stdout.on('resize', handleResize);

  function cleanupTUI() {
    running = false;
    clearInterval(pollInterval);
    process.stdout.off('resize', handleResize);
    process.stdin.removeListener('keypress', handleTUIKeypress);
    
    if (process.stdin.isTTY) {
      process.stdin.setRawMode(false);
    }
    
    process.stdout.write('\x1b[?25h\x1b[2J\x1b[H');
    
    tuiState.queues.forEach(oq => {
      try { oq.queue.close(); } catch {}
    });
  }
}

// ─── Queue Administrative Action Helpers ─────────────────────────────────────

async function retryFailedJobs(queueName) {
  divider(`Action: Retry Failed Jobs`);
  const queues = queueName ? [queueName] : await discoverQueues();
  if (queues.length === 0) {
    info('No queues found to retry.');
    return;
  }

  for (const name of queues) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    try {
      const failedCount = await q.getFailedCount();
      if (failedCount === 0) {
        info(`Queue "${name}": 0 failed jobs (skipping)`);
        await q.close();
        continue;
      }
      info(`Queue "${name}": Retrying ${failedCount} failed jobs...`);
      await q.retryJobs();
      pass(`Queue "${name}": Successfully retried all ${failedCount} jobs.`);
    } catch (err) {
      fail(`Queue "${name}": Failed to retry jobs: ${err.message}`);
    } finally {
      try { await q.close(); } catch {}
    }
  }
}

async function cleanJobsAction(qName, type) {
  divider(`Action: Clean ${type === 'completed' ? 'Completed' : 'Failed'} Jobs`);
  const queues = qName ? [qName] : await discoverQueues();
  if (queues.length === 0) {
    info('No queues found to clean.');
    return;
  }
  for (const name of queues) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    try {
      await q.clean(0, 0, type);
      pass(`Queue "${name}": Successfully cleaned ${type} jobs.`);
    } catch (err) {
      fail(`Queue "${name}": Failed to clean jobs: ${err.message}`);
    } finally {
      try { await q.close(); } catch {}
    }
  }
}

async function drainQueueAction(name) {
  divider(`Action: Drain Queue`);
  if (!name) {
    console.error(chalk.red('Please specify queue name to drain. Example: --drain <queueName>'));
    process.exit(1);
  }
  const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    await q.drain(true);
    pass(`Queue "${name}": Successfully drained.`);
  } catch (err) {
    fail(`Queue "${name}": Failed to drain queue: ${err.message}`);
  } finally {
    try { await q.close(); } catch {}
  }
}

async function obliterateQueueAction(name) {
  divider(`Action: Obliterate Queue`);
  if (!name) {
    console.error(chalk.red('Please specify queue name to obliterate. Example: --obliterate <queueName>'));
    process.exit(1);
  }
  const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    await q.obliterate({ force: true });
    pass(`Queue "${name}": Successfully obliterated queue and all keys.`);
  } catch (err) {
    fail(`Queue "${name}": Failed to obliterate queue: ${err.message}`);
  } finally {
    try { await q.close(); } catch {}
  }
}

async function addJobAction(name) {
  divider(`Action: Add Job`);
  if (!name) {
    console.error(chalk.red('Please specify queue name to add job. Example: --add-job <queueName>'));
    process.exit(1);
  }
  const jobName = getArg('--job-name') || 'test-job';
  const jobDataStr = getArg('--job-data') || '{}';
  let jobData = {};
  try {
    jobData = JSON.parse(jobDataStr);
  } catch (e) {
    console.error(chalk.red(`Invalid JSON string for --job-data: ${jobDataStr}`));
    process.exit(1);
  }

  const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    const job = await q.add(jobName, jobData);
    pass(`Queue "${name}": Successfully added job "${jobName}" with ID "${job.id}".`);
  } catch (err) {
    fail(`Queue "${name}": Failed to add job: ${err.message}`);
  } finally {
    try { await q.close(); } catch {}
  }
}

async function pauseQueueAction(name) {
  divider(`Action: Pause Queue`);
  if (!name) {
    console.error(chalk.red('Please specify queue name to pause. Example: --pause <queueName>'));
    process.exit(1);
  }
  const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    await q.pause();
    pass(`Queue "${name}": Successfully paused globally.`);
  } catch (err) {
    fail(`Queue "${name}": Failed to pause queue: ${err.message}`);
  } finally {
    try { await q.close(); } catch {}
  }
}

async function resumeQueueAction(name) {
  divider(`Action: Resume Queue`);
  if (!name) {
    console.error(chalk.red('Please specify queue name to resume. Example: --resume <queueName>'));
    process.exit(1);
  }
  const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    await q.resume();
    pass(`Queue "${name}": Successfully resumed globally.`);
  } catch (err) {
    fail(`Queue "${name}": Failed to resume queue: ${err.message}`);
  } finally {
    try { await q.close(); } catch {}
  }
}

async function executeDirectActions() {
  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();
    await redis.ping();
    await redis.quit();
  } catch (err) {
    console.error(chalk.red(`\nCannot connect to Redis to perform action: ${err.message}`));
    try { await redis.quit(); } catch {}
    process.exit(1);
  }

  if (RETRY_FAILED) {
    await retryFailedJobs(RETRY_QUEUE);
  } else if (CLEAN_FAILED) {
    await cleanJobsAction(CLEAN_FAILED_QUEUE, 'failed');
  } else if (CLEAN_COMPLETED) {
    await cleanJobsAction(CLEAN_COMPLETED_QUEUE, 'completed');
  } else if (DRAIN_QUEUE_FLAG) {
    await drainQueueAction(DRAIN_QUEUE_NAME);
  } else if (OBLITERATE_QUEUE_FLAG) {
    await obliterateQueueAction(OBLITERATE_QUEUE_NAME);
  } else if (ADD_JOB_FLAG) {
    await addJobAction(ADD_JOB_QUEUE);
  } else if (PAUSE_QUEUE_FLAG) {
    await pauseQueueAction(PAUSE_QUEUE_NAME);
  } else if (RESUME_QUEUE_FLAG) {
    await resumeQueueAction(RESUME_QUEUE_NAME);
  }
  process.exit(totalFail > 0 ? 1 : 0);
}

// ─── Interactive Control Center Helpers ──────────────────────────────────────

function askQuestion(query) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve) => rl.question(query, (ans) => {
    rl.close();
    resolve(ans);
  }));
}

async function selectQueuePrompt(message = 'Select a queue by number or type its name: ') {
  const queueNames = await discoverQueues();
  if (queueNames.length === 0) {
    console.log(chalk.yellow('\n  No BullMQ queues found in Redis.'));
    return null;
  }

  console.log('\n  Discovered Queues:');
  queueNames.forEach((name, idx) => {
    console.log(`    ${chalk.cyan(`${idx + 1}`)}: ${name}`);
  });

  const response = await askQuestion(`\n  ${message}`);
  const trimmed = response.trim();

  if (!trimmed) return null;

  const num = parseInt(trimmed, 10);
  if (!isNaN(num) && num >= 1 && num <= queueNames.length) {
    return queueNames[num - 1];
  }

  if (queueNames.includes(trimmed)) {
    return trimmed;
  }

  console.log(chalk.red(`  Invalid queue selection: "${trimmed}"`));
  return null;
}

async function listQueuesWithWorkers() {
  console.log(chalk.cyan('\n  Scanning for BullMQ queues and workers...'));
  const queueNames = await discoverQueues();
  if (queueNames.length === 0) {
    console.log(chalk.yellow('  No BullMQ queues found in Redis.'));
    return;
  }

  console.log('\n  Active Queues & Registered Workers:');
  console.log(chalk.gray('  ' + '─'.repeat(45)));
  for (const name of queueNames) {
    const q = new Queue(name, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    try {
      const workersCount = await q.getWorkersCount().catch(() => 0);
      const isPaused = await q.isPaused().catch(() => false);
      const pauseStr = isPaused ? chalk.yellow(' (PAUSED ⏸)') : '';
      console.log(`    ${chalk.white.bold(name)}: ${chalk.cyan(workersCount)} worker(s)${pauseStr}`);
    } catch (err) {
      console.log(`    ${name}: ${chalk.red('error: ' + err.message)}`);
    } finally {
      try { await q.close(); } catch {}
    }
  }
}

async function interactiveInspectQueue() {
  const qName = await selectQueuePrompt('Select a queue to inspect: ');
  if (!qName) return;

  while (true) {
    const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    
    let waiting, active, delayed, failed, completed, workerCount, workers, isPaused;
    
    try {
      console.log(chalk.cyan('\n  Fetching queue details from Redis...'));
      [waiting, active, delayed, failed, completed, workerCount, workers, isPaused] = await Promise.all([
        q.getWaitingCount(),
        q.getActiveCount(),
        q.getDelayedCount(),
        q.getFailedCount(),
        q.getCompletedCount(),
        q.getWorkersCount().catch(() => 0),
        q.getWorkers().catch(() => []),
        q.isPaused().catch(() => false)
      ]);
    } catch (err) {
      console.log(chalk.red(`\n  Error inspecting queue: ${err.message}`));
      try { await q.close(); } catch {}
      await askQuestion('\n  Press [Enter] to return.');
      break;
    }

    console.clear();
    divider(`Queue Details: ${qName}`);
    console.log(`  Status:    ${isPaused ? chalk.yellow('⏸ PAUSED') : chalk.green('▶ ACTIVE')}`);
    console.log(`  ${chalk.bold('Job Counts:')}`);
    console.log(`    Waiting:   ${chalk.cyan(waiting)}`);
    console.log(`    Active:    ${chalk.green(active)}`);
    console.log(`    Delayed:   ${chalk.blue(delayed)}`);
    console.log(`    Failed:    ${chalk.red(failed)}`);
    console.log(`    Completed: ${chalk.green(completed)}`);
    console.log(`    Total:     ${waiting + active + delayed + failed + completed}`);

    console.log(`\n  ${chalk.bold(`Workers Connected (${workerCount}):`)}`);
    if (workerCount > 0 && workers && workers.length > 0) {
      workers.forEach((w, idx) => {
        console.log(`    ${chalk.cyan(`#${idx + 1}`)} ID: ${w.id} | IP/Port: ${w.addr} | Library: ${w['lib-name'] || 'ioredis'} (v${w['lib-ver'] || 'N/A'})`);
        if (w.name) console.log(`       Worker Name: ${w.name}`);
        console.log(`       Age: ${formatUptime(parseInt(w.age || '0'))} | Idle: ${w.idle}s | Command: ${w.cmd || 'N/A'}`);
      });
    } else {
      console.log(chalk.yellow('    No active workers listening to this queue.'));
    }

    if (failed > 0) {
      console.log(`\n  ${chalk.red.bold(`Failed Jobs (showing up to 3):`)}`);
      try {
        const failedJobs = await q.getFailed(0, 2);
        failedJobs.forEach((fj) => {
          console.log(`    ${chalk.red(`•`)} Job ID: ${fj.id} | Name: ${fj.name} | Failed Reason: ${chalk.red.bold(fj.failedReason || 'Unknown')}`);
          if (fj.stacktrace && fj.stacktrace.length > 0) {
            console.log(`      Stack trace snippet: ${chalk.gray(fj.stacktrace[0].split('\n')[0])}`);
          }
        });
      } catch {}
    }

    if (active > 0) {
      console.log(`\n  ${chalk.green.bold(`Active Jobs (showing up to 3):`)}`);
      try {
        const activeJobs = await q.getActive(0, 2);
        activeJobs.forEach((aj) => {
          const elapsed = aj.processedOn ? `${Math.round((Date.now() - aj.processedOn) / 1000)}s ago` : 'unknown';
          console.log(`    ${chalk.green(`•`)} Job ID: ${aj.id} | Name: ${aj.name} | Started: ${elapsed}`);
        });
      } catch {}
    }

    console.log(`\n  ${chalk.bold('Quick Actions:')}`);
    console.log(`    ${chalk.cyan('r)')} Retry all failed jobs     ${chalk.cyan('c)')} Clean completed jobs`);
    console.log(`    ${chalk.cyan('f)')} Clean failed jobs         ${chalk.cyan('t)')} Toggle pause/resume`);
    console.log(`    ${chalk.cyan('a)')} Add a test job            ${chalk.cyan('d)')} Drain queue`);
    console.log(`    ${chalk.cyan('o)')} Obliterate queue          ${chalk.cyan('s)')} Search/Filter jobs`);
    console.log(`    ${chalk.cyan('Enter)')} Back to main menu`);

    const action = await askQuestion('\n  Select a quick action: ');
    const trimmedAction = action.trim().toLowerCase();

    if (!trimmedAction) {
      try { await q.close(); } catch {}
      break;
    }

    try {
      if (trimmedAction === 'r') {
        const count = await q.getFailedCount();
        if (count === 0) {
          console.log(chalk.yellow('\n  No failed jobs to retry.'));
        } else {
          console.log(chalk.cyan(`\n  Retrying ${count} failed jobs...`));
          await q.retryJobs();
          console.log(chalk.green('  Jobs retried!'));
        }
        await sleep(1000);
      } else if (trimmedAction === 'c') {
        console.log(chalk.cyan('\n  Cleaning completed jobs...'));
        await q.clean(0, 0, 'completed');
        console.log(chalk.green('  Completed jobs cleaned!'));
        await sleep(1000);
      } else if (trimmedAction === 'f') {
        console.log(chalk.cyan('\n  Cleaning failed jobs...'));
        await q.clean(0, 0, 'failed');
        console.log(chalk.green('  Failed jobs cleaned!'));
        await sleep(1000);
      } else if (trimmedAction === 't') {
        if (isPaused) {
          console.log(chalk.cyan('\n  Resuming queue...'));
          await q.resume();
          console.log(chalk.green('  Queue resumed!'));
        } else {
          console.log(chalk.cyan('\n  Pausing queue...'));
          await q.pause();
          console.log(chalk.green('  Queue paused!'));
        }
        await sleep(1000);
      } else if (trimmedAction === 'a') {
        const jobName = await askQuestion('\n  Enter job name (default: test-job): ') || 'test-job';
        const jobData = await askQuestion('  Enter payload JSON string (default: {}): ');
        let parsedData = {};
        try {
          if (jobData.trim()) parsedData = JSON.parse(jobData);
        } catch {
          console.log(chalk.red('  Invalid JSON payload. Using default empty object.'));
        }
        await q.add(jobName, parsedData);
        console.log(chalk.green('  Test job added!'));
        await sleep(1000);
      } else if (trimmedAction === 'd') {
        const confirmDrain = await askQuestion(chalk.yellow('\n  Are you sure you want to drain this queue? ALL pending jobs will be deleted. (y/N): '));
        if (confirmDrain.toLowerCase() === 'y') {
          await q.drain(true);
          console.log(chalk.green('  Queue drained successfully!'));
        }
        await sleep(1000);
      } else if (trimmedAction === 'o') {
        const confirmObliterate = await askQuestion(chalk.red.bold(`\n  Type "${qName}" to confirm OBLITERATION: `));
        if (confirmObliterate === qName) {
          await q.obliterate({ force: true });
          console.log(chalk.green('  Queue obliterated!'));
          try { await q.close(); } catch {}
          await sleep(1500);
          break;
        }
      } else if (trimmedAction === 's') {
        const query = await askQuestion('\n  Enter job ID, name, or payload search term: ');
        if (query.trim()) {
          console.log(chalk.cyan('\n  Searching jobs...'));
          
          const states = ['failed', 'completed', 'active', 'waiting', 'delayed'];
          let foundCount = 0;
          
          for (const state of states) {
            const jobs = await q.getJobs([state], 0, 100);
            for (const job of jobs) {
              const matchesId = job.id && String(job.id).includes(query);
              const matchesName = job.name && String(job.name).includes(query);
              const matchesPayload = job.data && JSON.stringify(job.data).includes(query);
              
              if (matchesId || matchesName || matchesPayload) {
                foundCount++;
                console.log(`\n  ${chalk.cyan(`[${state.toUpperCase()}]`)} Job ID: ${chalk.white.bold(job.id)} | Name: ${chalk.white(job.name)}`);
                console.log(`    Created: ${new Date(job.timestamp).toLocaleString()}`);
                if (job.failedReason) console.log(`    Error: ${chalk.red(job.failedReason)}`);
                console.log(`    Payload: ${chalk.yellow(JSON.stringify(job.data, null, 2))}`);
                if (foundCount >= 10) {
                  console.log(chalk.yellow('\n  Showing first 10 matches. Search truncated.'));
                  break;
                }
              }
            }
            if (foundCount >= 10) break;
          }
          if (foundCount === 0) {
            console.log(chalk.yellow('  No matching jobs found.'));
          }
          await askQuestion('\n  Press [Enter] to continue.');
        }
      }
    } catch (actErr) {
      console.log(chalk.red(`\n  Action failed: ${actErr.message}`));
      await sleep(2000);
    } finally {
      try { await q.close(); } catch {}
    }
  }
}

async function interactiveRetryJobs() {
  console.log('\n  1) Retry failed jobs for a specific queue');
  console.log('  2) Retry failed jobs for ALL queues');
  console.log('  3) Back to main menu');

  const choice = await askQuestion('\n  Choose option (1-3): ');

  if (choice.trim() === '1') {
    const qName = await selectQueuePrompt('Select queue to retry failed jobs: ');
    if (qName) {
      const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
      q.on('error', () => {});
      try {
        const failedCount = await q.getFailedCount();
        if (failedCount === 0) {
          console.log(chalk.yellow(`\n  Queue "${qName}" has no failed jobs.`));
        } else {
          console.log(chalk.cyan(`\n  Retrying ${failedCount} failed jobs...`));
          await q.retryJobs();
          console.log(chalk.green(`  Success! All failed jobs in "${qName}" have been retried.`));
        }
      } catch (err) {
        console.log(chalk.red(`  Error retrying jobs: ${err.message}`));
      } finally {
        try { await q.close(); } catch {}
      }
    }
  } else if (choice.trim() === '2') {
    const queueNames = await discoverQueues();
    if (queueNames.length === 0) {
      console.log(chalk.yellow('\n  No queues found.'));
      await askQuestion('\n  Press [Enter] to continue.');
      return;
    }

    let totalRetried = 0;
    for (const qName of queueNames) {
      const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
      q.on('error', () => {});
      try {
        const failedCount = await q.getFailedCount();
        if (failedCount > 0) {
          console.log(chalk.cyan(`  Retrying ${failedCount} jobs in "${qName}"...`));
          await q.retryJobs();
          totalRetried += failedCount;
        }
      } catch (err) {
        console.log(chalk.red(`  Error retrying jobs on "${qName}": ${err.message}`));
      } finally {
        try { await q.close(); } catch {}
      }
    }
    console.log(chalk.green(`\n  Success! Retried jobs across all queues. Total retried: ${totalRetried}`));
  }
  await askQuestion('\n  Press [Enter] to continue.');
}

async function interactiveCleanJobs() {
  const qName = await selectQueuePrompt('Select a queue to clean: ');
  if (!qName) return;

  console.log(`\n  What jobs would you like to clean in queue "${qName}"?`);
  console.log(`  ${chalk.cyan('1)')} Completed jobs`);
  console.log(`  ${chalk.cyan('2)')} Failed jobs`);
  console.log(`  ${chalk.cyan('3)')} Wait/Delayed/Active/Failed (Drain entire queue)`);
  console.log(`  ${chalk.cyan('4)')} Cancel`);

  const choice = await askQuestion('\n  Choose option (1-4): ');

  const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});

  try {
    switch (choice.trim()) {
      case '1':
        console.log(chalk.cyan(`\n  Cleaning completed jobs...`));
        await q.clean(0, 0, 'completed');
        console.log(chalk.green('  Success! Completed jobs cleared.'));
        break;
      case '2':
        console.log(chalk.cyan(`\n  Cleaning failed jobs...`));
        await q.clean(0, 0, 'failed');
        console.log(chalk.green('  Success! Failed jobs cleared.'));
        break;
      case '3':
        const confirm = await askQuestion(chalk.yellow(`\n  Are you sure you want to drain "${qName}"? This will delete ALL pending jobs! (y/N): `));
        if (confirm.toLowerCase() === 'y') {
          console.log(chalk.cyan(`\n  Draining queue "${qName}"...`));
          await q.drain(true);
          console.log(chalk.green('  Success! Queue drained.'));
        } else {
          console.log(chalk.gray('  Cancelled.'));
        }
        break;
      default:
        console.log(chalk.gray('  Action cancelled.'));
    }
  } catch (err) {
    console.log(chalk.red(`  Error cleaning queue: ${err.message}`));
  } finally {
    try { await q.close(); } catch {}
  }
  await askQuestion('\n  Press [Enter] to continue.');
}

async function interactivePauseResumeQueue() {
  const qName = await selectQueuePrompt('Select a queue to pause/resume: ');
  if (!qName) return;

  const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});

  try {
    const isPaused = await q.isPaused();
    console.log(`\n  Queue "${qName}" is currently: ${isPaused ? chalk.yellow('PAUSED ⏸') : chalk.green('ACTIVE ▶')}`);
    console.log(`  1) ${isPaused ? 'Resume' : 'Pause'} the queue`);
    console.log(`  2) Cancel`);

    const choice = await askQuestion('\n  Choose option (1-2): ');
    if (choice.trim() === '1') {
      if (isPaused) {
        console.log(chalk.cyan(`\n  Resuming queue "${qName}"...`));
        await q.resume();
        console.log(chalk.green('  Success! Queue resumed.'));
      } else {
        console.log(chalk.cyan(`\n  Pausing queue "${qName}"...`));
        await q.pause();
        console.log(chalk.green('  Success! Queue paused.'));
      }
    }
  } catch (err) {
    console.log(chalk.red(`  Error changing queue pause state: ${err.message}`));
  } finally {
    try { await q.close(); } catch {}
  }
  await askQuestion('\n  Press [Enter] to continue.');
}

async function interactiveObliterateQueue() {
  const qName = await selectQueuePrompt('Select a queue to OBLITERATE (delete all keys/data): ');
  if (!qName) return;

  console.log(chalk.red.bold(`\n  ⚠ WARNING: Obliterating queue "${qName}" will delete ALL database keys associated with it.`));
  console.log(`  This action cannot be undone. Active workers might crash if they are currently working on it.`);

  const confirm = await askQuestion(chalk.red.bold(`\n  Type the queue name "${qName}" to confirm deletion: `));

  if (confirm === qName) {
    console.log(chalk.red(`\n  Obliterating queue "${qName}"...`));
    const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
    q.on('error', () => {});
    try {
      await q.obliterate({ force: true });
      console.log(chalk.green(`  Success! Queue "${qName}" has been obliterated.`));
    } catch (err) {
      console.log(chalk.red(`  Error obliterating queue: ${err.message}`));
    } finally {
      try { await q.close(); } catch {}
    }
  } else {
    console.log(chalk.gray('\n  Confirmation mismatch. Cancelled.'));
  }
  await askQuestion('\n  Press [Enter] to continue.');
}

async function interactiveAddJob() {
  const qName = await selectQueuePrompt('Select queue to add a job to: ');
  if (!qName) return;

  const jobName = await askQuestion('\n  Enter job name (default: test-job): ') || 'test-job';
  console.log('\n  Enter job payload as JSON (default: { "test": true }): ');
  const payloadStr = await askQuestion('  > ');

  let payload = { test: true };
  if (payloadStr.trim()) {
    try {
      payload = JSON.parse(payloadStr);
    } catch (err) {
      console.log(chalk.red(`\n  Invalid JSON format. Using default payload: { "test": true }`));
    }
  }

  const q = new Queue(qName, { connection: { ...BULLMQ_CONNECTION }, prefix: BULLMQ_PREFIX });
  q.on('error', () => {});
  try {
    const job = await q.add(jobName, payload);
    console.log(chalk.green(`\n  Success! Job added with ID: ${job.id}`));
  } catch (err) {
    console.log(chalk.red(`  Error adding job: ${err.message}`));
  } finally {
    try { await q.close(); } catch {}
  }
  await askQuestion('\n  Press [Enter] to continue.');
}

async function startInteractiveMode() {
  console.clear();
  console.log(chalk.cyan.bold('╔═══════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.cyan.bold('║         BullMQ & Redis Tester — Interactive Control Center        ║'));
  console.log(chalk.cyan.bold('╚═══════════════════════════════════════════════════════════════════╝'));
  console.log(chalk.gray(`  Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}`));
  if (REDIS_TLS) console.log(chalk.gray('  SSL/TLS Enabled'));

  const redis = new Redis({
    ...REDIS_CONNECTION,
    lazyConnect: true,
    connectTimeout: 3000,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });

  try {
    await redis.connect();
    await redis.ping();
    await redis.quit();
  } catch (err) {
    console.log(chalk.red(`\n  Cannot connect to Redis: ${err.message}`));
    try { await redis.quit(); } catch {}
    await askQuestion('\n  Press [Enter] to exit.');
    return;
  }

  while (true) {
    console.clear();
    console.log(chalk.cyan.bold('╔═══════════════════════════════════════════════════════════════════╗'));
    console.log(chalk.cyan.bold('║         BullMQ & Redis Tester — Interactive Control Center        ║'));
    console.log(chalk.cyan.bold('╚═══════════════════════════════════════════════════════════════════╝'));
    console.log(chalk.gray(`  Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}`));
    if (REDIS_TLS) console.log(chalk.gray('  SSL/TLS Enabled'));
    
    console.log('\n' + chalk.bold.white('  --- MAIN MENU ---'));
    console.log(`  ${chalk.cyan('1)')} Run Full Health Check Suite`);
    console.log(`  ${chalk.cyan('2)')} List All Queues & Worker Counts`);
    console.log(`  ${chalk.cyan('3)')} Inspect Specific Queue (Interactive Panel)`);
    console.log(`  ${chalk.cyan('4)')} Retry Failed Jobs`);
    console.log(`  ${chalk.cyan('5)')} Clean/Purge Queue Jobs`);
    console.log(`  ${chalk.cyan('6)')} Pause/Resume a Queue`);
    console.log(`  ${chalk.cyan('7)')} Obliterate a Queue`);
    console.log(`  ${chalk.cyan('8)')} Add Test Job to Queue`);
    console.log(`  ${chalk.cyan('9)')} Start Live Dashboard (Interactive UI)`);
    console.log(`  ${chalk.cyan('10)')} Open Dashboard TUI Mode`);
    console.log(`  ${chalk.cyan('11)')} Exit`);

    const choice = await askQuestion('\n  Choose an option (1-11): ');

    switch (choice.trim()) {
      case '1':
        totalPass = 0; totalFail = 0; totalWarn = 0;
        await testRedisConnection();
        await testRedisCacheOps();
        await testQueueInspection();
        await testRoundtrip();
        await testRedisKeysScan();
        printSummary();
        await askQuestion('\n  Press [Enter] to return to the menu.');
        break;
      case '2':
        await listQueuesWithWorkers();
        await askQuestion('\n  Press [Enter] to return to the menu.');
        break;
      case '3':
        await interactiveInspectQueue();
        break;
      case '4':
        await interactiveRetryJobs();
        break;
      case '5':
        await interactiveCleanJobs();
        break;
      case '6':
        await interactivePauseResumeQueue();
        break;
      case '7':
        await interactiveObliterateQueue();
        break;
      case '8':
        await interactiveAddJob();
        break;
      case '9':
        await startLiveMode(true);
        break;
      case '10':
        await startTUI();
        break;
      case '11':
      case 'exit':
      case 'q':
        console.log(chalk.cyan('\n  Goodbye!\n'));
        process.exit(0);
      default:
        console.log(chalk.yellow('\n  Invalid option. Please try again.'));
        await sleep(1000);
    }
  }
}

// ─── Reporting and Document Exports ──────────────────────────────────────────

function updateReportSummary() {
  reportData.summary = {
    passed: totalPass,
    failed: totalFail,
    warnings: totalWarn,
    status: totalFail > 0 ? 'FAILED' : totalWarn > 0 ? 'WARNINGS' : 'PASSED'
  };
}

function generateMarkdownReport() {
  const s = reportData.summary;
  const statusColor = s.status === 'PASSED' ? '🟢' : s.status === 'WARNINGS' ? '🟡' : '🔴';

  let md = `# BullMQ & Redis Health Report\n\n`;
  md += `Generated on: ${new Date(reportData.timestamp).toLocaleString()}\n`;
  md += `Target Redis: \`${reportData.target}\`\n`;
  md += `Key Prefix: \`${reportData.prefix}\`\n\n`;

  md += `## Summary\n`;
  md += `Status: ${statusColor} **${s.status}**\n`;
  md += `- **Passed:** ${s.passed}\n`;
  md += `- **Failed:** ${s.failed}\n`;
  md += `- **Warnings:** ${s.warnings}\n\n`;

  md += `## 1. Redis Connection & Info\n`;
  if (reportData.redis.connection) {
    const r = reportData.redis;
    md += `- **Connection:** Success\n`;
    md += `- **Version:** ${r.version || 'Unknown'}\n`;
    md += `- **Mode:** ${r.mode || 'Unknown'}\n`;
    md += `- **Uptime:** ${r.uptime ? formatUptime(parseInt(r.uptime)) : 'Unknown'}\n`;
    md += `- **Memory Used:** ${r.memory_used || 'Unknown'}\n`;
    md += `- **Connected Clients:** ${r.connected_clients || 'Unknown'}\n`;
    if (r.latency.avg !== null) {
      md += `- **Ping Latency:** avg=${r.latency.avg.toFixed(1)}ms (min=${r.latency.min}ms, max=${r.latency.max}ms)\n`;
    }
  } else {
    md += `⚠️ **Connection Failed**\n`;
  }
  md += `\n`;

  md += `## 2. Redis Cache Operations\n`;
  const co = reportData.redis.cache_ops;
  md += `| Test | Status |\n`;
  md += `| :--- | :--- |\n`;
  md += `| SET | ${co.set ? '✅ PASS' : '❌ FAIL'} |\n`;
  md += `| GET | ${co.get ? '✅ PASS' : '❌ FAIL'} |\n`;
  md += `| TTL | ${co.ttl ? '✅ PASS' : '❌ FAIL'} |\n`;
  md += `| INCR | ${co.incr ? '✅ PASS' : '❌ FAIL'} |\n`;
  md += `| DEL | ${co.del ? '✅ PASS' : '❌ FAIL'} |\n`;
  md += `| Pub/Sub | ${co.pubsub ? '✅ PASS' : '❌ FAIL'} |\n\n`;

  md += `## 3. Discovered BullMQ Queues\n`;
  if (reportData.queues.length === 0) {
    md += `No BullMQ queues found in Redis.\n`;
  } else {
    md += `| Queue Name | Waiting | Active | Delayed | Failed | Completed | Workers | Status |\n`;
    md += `| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |\n`;
    for (const q of reportData.queues) {
      const qStatus = q.status === 'ACTIVE' ? '🟢 ACTIVE' : q.status === 'FAILURES' ? '🔴 FAILURES' : q.status === 'PAUSED' ? '⏸ PAUSED' : q.status === 'PENDING' ? '🟡 PENDING' : q.status === 'NO_WORKERS' ? '🔴 NO WORKERS' : q.status === 'CRITICAL_FAILURES' ? '🚨 CRITICAL' : q.status === 'OVERLOADED' ? '🚨 OVERLOAD' : '⚪ IDLE';
      md += `| \`${q.name}\` | ${q.waiting} | ${q.active} | ${q.delayed} | ${q.failed} | ${q.completed} | ${q.workersCount} | ${qStatus} |\n`;
    }
  }
  md += `\n`;

  if (reportData.roundtrip.run) {
    md += `## 4. Lifecycle Roundtrip Test\n`;
    const rt = reportData.roundtrip;
    md += `- **Job Added:** ${rt.job_added ? '✅' : '❌'}\n`;
    md += `- **Job Queued (Waiting):** ${rt.job_waiting ? '✅' : '❌'}\n`;
    md += `- **Worker Consumed Job:** ${rt.job_consumed ? '✅' : '❌'}\n`;
    md += `- **Payload Integrity:** ${rt.payload_integrity ? '✅' : '❌'}\n`;
    md += `- **Queue Drained:** ${rt.queue_drained ? '✅' : '❌'}\n`;
    md += `- **Offline Buffering:** ${rt.offline_buffering ? '✅' : '❌'}\n`;
    md += `- **Cleanup:** ${rt.cleanup ? '✅' : '❌'}\n\n`;
  }

  md += `## 5. Key Scan & Namespaces\n`;
  const ks = reportData.keys_scan;
  md += `- **Total BullMQ Keys:** ${ks.total_bullmq_keys}\n`;
  md += `- **Total BullMQ Memory:** ${formatBytes(ks.total_bullmq_memory_bytes || 0)}\n`;
  md += `- **Other Non-BullMQ Keys:** ${ks.other_keys}\n\n`;

  if (Object.keys(ks.namespaces).length > 0) {
    md += `### BullMQ Keys by Queue:\n`;
    md += `| Queue | Keys Count | Memory Size |\n`;
    md += `| :--- | :---: | :---: |\n`;
    for (const [ns, data] of Object.entries(ks.namespaces).sort((a, b) => b[1].memory_bytes - a[1].memory_bytes)) {
      md += `| \`${ns}\` | ${data.keys} | ${data.memory_formatted} |\n`;
    }
  }

  return md;
}

function saveReport() {
  if (!OUTPUT_FILE) return;

  const ext = path.extname(OUTPUT_FILE).toLowerCase();
  let content = '';

  if (ext === '.json') {
    content = JSON.stringify(reportData, null, 2);
  } else {
    content = generateMarkdownReport();
  }

  try {
    fs.writeFileSync(path.resolve(OUTPUT_FILE), content, 'utf8');
    printLog(`\n  ${chalk.green.bold('✓')} Report saved to ${chalk.white.bold(OUTPUT_FILE)}`);
  } catch (err) {
    if (!JSON_OUTPUT) {
      console.error(chalk.red(`\n  Failed to save report: ${err.message}`));
    }
  }
}

function printSummary() {
  updateReportSummary();
  if (JSON_OUTPUT) return;

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

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  // Run maintenance / pruning tasks if requested
  await runPruningAndAutoRetry();

  if (RETRY_FAILED || CLEAN_FAILED || CLEAN_COMPLETED || DRAIN_QUEUE_FLAG || OBLITERATE_QUEUE_FLAG || ADD_JOB_FLAG || PAUSE_QUEUE_FLAG || RESUME_QUEUE_FLAG) {
    await executeDirectActions();
    return;
  }

  if (TUI_MODE) {
    await startTUI();
    return;
  }

  if (INTERACTIVE) {
    await startInteractiveMode();
    return;
  }

  if (LIVE) {
    await startLiveMode();
    return;
  }

  if (!JSON_OUTPUT) {
    console.log('');
    console.log(chalk.cyan.bold('╔═══════════════════════════════════════════════════════════════════╗'));
    console.log(chalk.cyan.bold('║           BullMQ & Redis Tester — Universal CLI Tool             ║'));
    console.log(chalk.cyan.bold('╚═══════════════════════════════════════════════════════════════════╝'));
    console.log(chalk.gray(`  Target: redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}   |   ${new Date().toLocaleString()}`));
    console.log(chalk.gray(`  Key prefix: "${BULLMQ_PREFIX}"   |   PID: ${process.pid}`));
  }

  const redisOk = await testRedisConnection();

  if (!redisOk) {
    if (!JSON_OUTPUT) {
      console.log('');
      fail(chalk.red.bold('Redis is not reachable. Cannot continue with further tests.'));
      console.log(`\n  ${chalk.yellow('Troubleshooting:')}`);
      console.log(`    1. Check if Redis is running:  ${chalk.white('redis-cli ping')}`);
      console.log(`    2. Start Redis:                ${chalk.white('sudo systemctl start redis')}`);
      console.log(`    3. Custom host/port:           ${chalk.white('node index.js --host <host> --port <port>')}`);
      console.log(`    4. Auth required:              ${chalk.white('node index.js --password <pass>')}`);
      printSummary();
    } else {
      reportData.summary.status = 'FAILED';
      console.log(JSON.stringify(reportData, null, 2));
    }
    process.exit(1);
  }

  if (REDIS_ONLY) {
    await testRedisCacheOps();
    printSummary();
    saveReport();
    if (JSON_OUTPUT) console.log(JSON.stringify(reportData, null, 2));
    process.exit(totalFail > 0 ? 1 : 0);
  }

  if (QUEUES_ONLY) {
    await testQueueInspection();
    await testRedisKeysScan();
    printSummary();
    saveReport();
    if (JSON_OUTPUT) console.log(JSON.stringify(reportData, null, 2));
    process.exit(totalFail > 0 ? 1 : 0);
  }

  if (ROUNDTRIP) {
    await testRedisCacheOps();
    await testRoundtrip();
    printSummary();
    saveReport();
    if (JSON_OUTPUT) console.log(JSON.stringify(reportData, null, 2));
    process.exit(totalFail > 0 ? 1 : 0);
  }

  // Default: run everything
  await testRedisCacheOps();
  await testQueueInspection();
  await testRoundtrip();
  await testRedisKeysScan();

  printSummary();
  saveReport();

  if (JSON_OUTPUT) {
    console.log(JSON.stringify(reportData, null, 2));
  }
  process.exit(totalFail > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(chalk.red(`\nFatal error: ${err.message}`));
  process.exit(1);
});
