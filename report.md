# BullMQ & Redis Health Report

Generated on: 6/3/2026, 10:50:13 PM
Target Redis: `redis://127.0.0.1:6379/0`
Key Prefix: `bull`

## Summary
Status: 🔴 **FAILED**
- **Passed:** 27
- **Failed:** 1
- **Warnings:** 1

## 1. Redis Connection & Info
- **Connection:** Success
- **Version:** 8.8.0
- **Mode:** standalone
- **Uptime:** 36m 33s
- **Memory Used:** 2.49M
- **Connected Clients:** 117
- **Ping Latency:** avg=0.4ms (min=0ms, max=1ms)

## 2. Redis Cache Operations
| Test | Status |
| :--- | :--- |
| SET | ✅ PASS |
| GET | ✅ PASS |
| TTL | ✅ PASS |
| INCR | ✅ PASS |
| DEL | ✅ PASS |
| Pub/Sub | ✅ PASS |

## 3. Discovered BullMQ Queues
| Queue Name | Waiting | Active | Delayed | Failed | Completed | Workers | Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `attendance-export` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `billing-demand-callback` | 0 | 0 | 0 | 0 | 0 | 0 | ⚪ IDLE |
| `billing-demand-sync` | 0 | 0 | 0 | 3 | 0 | 1 | 🔴 FAILURES |
| `billing-update` | 0 | 0 | 0 | 0 | 0 | 0 | ⚪ IDLE |
| `bulk-academics` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `bulk-photos` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `bulk-resumes` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `bulk-status` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `email-otp` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `student-export` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `student-import` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `student-pdf-export` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |
| `test-stalled-jobs` | 1 | 0 | 0 | 0 | 0 | 0 | 🔴 NO WORKERS |
| `transport-demand-sync` | 0 | 0 | 0 | 0 | 0 | 1 | ⚪ IDLE |

## 4. Lifecycle Roundtrip Test
- **Job Added:** ✅
- **Job Queued (Waiting):** ✅
- **Worker Consumed Job:** ✅
- **Payload Integrity:** ✅
- **Queue Drained:** ✅
- **Offline Buffering:** ✅
- **Cleanup:** ✅

## 5. Key Scan & Namespaces
- **Total BullMQ Keys:** 35
- **Total BullMQ Memory:** 11.13 KB
- **Other Non-BullMQ Keys:** 1

### BullMQ Keys by Queue:
| Queue | Keys Count | Memory Size |
| :--- | :---: | :---: |
| `billing-demand-sync` | 8 | 6.28 KB |
| `test-stalled-jobs` | 6 | 3.02 KB |
| `transport-demand-sync` | 2 | 198 B |
| `student-pdf-export` | 2 | 182 B |
| `student-import` | 2 | 166 B |
| `student-export` | 2 | 166 B |
| `bulk-resumes` | 2 | 166 B |
| `email-otp` | 2 | 166 B |
| `bulk-academics` | 2 | 166 B |
| `bulk-photos` | 2 | 166 B |
| `bulk-status` | 2 | 166 B |
| `billing-demand-callback` | 1 | 118 B |
| `billing-update` | 1 | 102 B |
| `attendance-export` | 1 | 102 B |
