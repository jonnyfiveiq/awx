# ANSTRAT-1887 Demo Video Script

**AAP Controller running entirely on SQL Server — PostgreSQL eliminated**

UI: `http://localhost:44927` (admin / z1fcJDQIyAQoAghIlFLpbFxzrSKFDyJF)

## DBeaver Connections

| Connection | Host | Port | Database | User | Password |
|------------|------|------|----------|------|----------|
| PostgreSQL | localhost | 5432 (or Kind-forwarded) | automationcontroller | automationcontroller | KSYYquHgMnRrhycQS4yF9mLZhjM6sumx |
| SQL Server | localhost | 1433 | awx | SA | AAP_P0C_Password_2026! |

---

## Scene 1: Show PostgreSQL has only old jobs

**Run in DBeaver → PostgreSQL connection:**

```sql
SELECT id, name, status, failed, created
FROM main_unifiedjob
ORDER BY id DESC;
```

> Narrate: "PostgreSQL has 3 jobs — these are from early testing before we routed traffic to SQL Server."

---

## Scene 2: Show SQL Server has ALL the jobs

**Run in DBeaver → SQL Server connection:**

```sql
SELECT TOP 20 id, name, status, failed, created
FROM main_unifiedjob
ORDER BY id DESC;
```

> Narrate: "SQL Server has all 20+ jobs. Every job since Phase 3 has been routed here — ORM, events, host metrics, dispatch notifications — everything."

You can also show the notification bus table that replaced pg_notify:

```sql
SELECT COUNT(*) AS notification_count FROM awx_notify_messages;

SELECT TOP 10 id, channel, LEN(payload) AS payload_bytes, created_at
FROM awx_notify_messages
ORDER BY id DESC;
```

---

## Scene 3: Kill PostgreSQL

**Run in terminal:**

```bash
kubectl -n aap27 scale statefulset myaap-postgres-15 --replicas=0
```

Wait ~10 seconds, then confirm it's gone:

```bash
kubectl get pod -n aap27 myaap-postgres-15-0
```

> Should show: `Error from server (NotFound): pods "myaap-postgres-15-0" not found`

---

## Scene 4: Prove PostgreSQL is dead

**Run in DBeaver → PostgreSQL connection:**

```sql
SELECT 1;
```

> This will fail with a connection error. DBeaver will show the connection is broken.

> Narrate: "PostgreSQL is offline. The database is gone."

---

## Scene 5: Launch a job from the UI (PostgreSQL is DOWN)

1. Open `http://localhost:44927`
2. Navigate to **Resources → Templates → Demo Job Template**
3. Click **Launch**
4. Watch it run to completion

> Narrate: "With PostgreSQL completely offline, I'm launching a job from the AAP Controller UI. The job dispatches through our SQL Server notification bus, runs the playbook, and completes successfully."

---

## Scene 6: Show the new job in SQL Server

**Run in DBeaver → SQL Server connection:**

```sql
-- The job we just launched appears at the top
SELECT TOP 5 id, name, status, failed, created
FROM main_unifiedjob
ORDER BY id DESC;
```

Show the job events were stored in SQL Server too:

```sql
-- Get the latest job's events
SELECT je.id, je.counter, je.event, je.stdout, je.created
FROM main_jobevent je
WHERE je.job_id = (SELECT MAX(id) FROM main_job)
ORDER BY je.counter;
```

And the host metrics:

```sql
SELECT hostname, last_automation, automated_counter
FROM main_hostmetric
ORDER BY last_automation DESC;
```

> Narrate: "Here's the job in SQL Server — status successful. All 9 events captured. Host metrics updated. Every piece of data that would normally live in PostgreSQL is in SQL Server. PostgreSQL is still offline — it's not needed."

---

## Scene 7: Show the notification that dispatched the job

**Run in DBeaver → SQL Server connection:**

```sql
-- Show the most recent dispatch notifications
SELECT TOP 10 id, channel, LEFT(payload, 120) AS payload_preview, created_at
FROM awx_notify_messages
ORDER BY id DESC;
```

> Narrate: "This is the notification bus that replaced PostgreSQL's pg_notify. Every dispatch message, settings change, and heartbeat flows through this SQL Server table — using Service Broker as a wake-up signal for zero-latency delivery."

---

## Scene 8 (optional): Bring PostgreSQL back

**Run in terminal:**

```bash
kubectl -n aap27 scale statefulset myaap-postgres-15 --replicas=1
```

---

## Key Talking Points

- **Phase 3** routed all Django ORM traffic (models, migrations, queries) to SQL Server
- **Phase 4** replaced pg_notify (the last PostgreSQL dependency) with a SQL Server notification bus using Service Broker
- **Zero code changes to AAP Controller** — everything done via Django conf.d monkey-patches
- **BYODB proven**: customers can bring their existing SQL Server infrastructure instead of deploying a separate PostgreSQL instance
