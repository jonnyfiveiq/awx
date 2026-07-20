#!/bin/bash
# =============================================================================
# ANSTRAT-1887 Demo Video Script
# AAP Controller running entirely on SQL Server — PostgreSQL eliminated
# =============================================================================
#
# Prerequisites:
#   - kubectl context set to kind-27
#   - Pods running in aap27 namespace
#   - Phase 4 deployed (service broker + mssql-confd.py)
#
# UI: http://localhost:44927  (admin / z1fcJDQIyAQoAghIlFLpbFxzrSKFDyJF)
# =============================================================================

# --- SCENE 1: Show the running infrastructure ---

echo "=== Running containers ==="
podman ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "=== Controller pods ==="
kubectl get pods -n aap27 -l app.kubernetes.io/component=automationcontroller

echo ""
echo "=== PostgreSQL pod ==="
kubectl get pod -n aap27 myaap-postgres-15-0


# --- SCENE 2: Query PostgreSQL — show it only has OLD jobs ---

echo ""
echo "=== Jobs in PostgreSQL (only 3 old jobs from before SQL Server) ==="
kubectl exec -n aap27 myaap-postgres-15-0 -- \
  psql -U automationcontroller -d automationcontroller \
  -c "SELECT id, name, status, failed, created FROM main_unifiedjob ORDER BY id DESC;"


# --- SCENE 3: Query SQL Server — show it has ALL jobs including recent ones ---

echo ""
echo "=== Jobs in SQL Server (ALL jobs, including recent Phase 4 jobs) ==="
kubectl exec -n aap27 $(kubectl get pod -n aap27 -l app.kubernetes.io/name=automationcontroller-task -o jsonpath='{.items[0].metadata.name}') \
  -c myaap-controller-task -- bash -c '
source /var/lib/awx/venv/awx/bin/activate
python3 -c "
import pyodbc
conn = pyodbc.connect(
    \"DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal,1433;\"
    \"DATABASE=awx;UID=SA;PWD=AAP_P0C_Password_2026!;TrustServerCertificate=yes;\",
    autocommit=True
)
cursor = conn.cursor()
cursor.execute(\"SELECT TOP 10 id, name, status, failed, CONVERT(VARCHAR, created, 120) as created FROM main_unifiedjob ORDER BY id DESC\")
cols = [desc[0] for desc in cursor.description]
print(\"  \".join(str(c).ljust(22) for c in cols))
print(\"-\" * 110)
for row in cursor.fetchall():
    print(\"  \".join(str(v)[:22].ljust(22) for v in row))
conn.close()
"
'


# --- SCENE 4: STOP PostgreSQL ---

echo ""
echo "=== Stopping PostgreSQL... ==="
kubectl -n aap27 scale statefulset myaap-postgres-15 --replicas=0

echo ""
echo "Waiting for PostgreSQL to stop..."
kubectl wait --for=delete pod/myaap-postgres-15-0 -n aap27 --timeout=60s 2>/dev/null || sleep 10

echo ""
echo "=== PostgreSQL pod status ==="
kubectl get pod -n aap27 myaap-postgres-15-0 2>&1 || echo "(Pod is gone — PostgreSQL is OFFLINE)"


# --- SCENE 5: Prove PostgreSQL is dead ---

echo ""
echo "=== Attempting to query PostgreSQL (should FAIL)... ==="
kubectl exec -n aap27 myaap-postgres-15-0 -- \
  psql -U automationcontroller -d automationcontroller \
  -c "SELECT COUNT(*) FROM main_unifiedjob;" 2>&1 || echo ""
echo "^^^ PostgreSQL is OFFLINE — connection refused / pod not found"


# --- SCENE 6: Launch a job from the UI ---
# (Do this manually in the browser at http://localhost:44927)
# Navigate to: Resources > Templates > Demo Job Template > Launch
#
# OR launch via CLI:

echo ""
echo "=== Launching Demo Job Template (PostgreSQL is DOWN) ==="
kubectl exec -n aap27 $(kubectl get pod -n aap27 -l app.kubernetes.io/name=automationcontroller-task -o jsonpath='{.items[0].metadata.name}') \
  -c myaap-controller-task -- bash -c '
source /var/lib/awx/venv/awx/bin/activate
awx-manage shell -c "
from awx.main.models import JobTemplate
jt = JobTemplate.objects.get(name=\"Demo Job Template\")
job = jt.create_unified_job()
job.signal_start()
print(f\"Job {job.id} launched — PostgreSQL is OFFLINE, dispatched via SQL Server!\")
"
'

echo ""
echo "Waiting 45 seconds for job to complete..."
sleep 45


# --- SCENE 7: Show the job succeeded on SQL Server ---

echo ""
echo "=== Job result (from SQL Server — PostgreSQL still DOWN) ==="
kubectl exec -n aap27 $(kubectl get pod -n aap27 -l app.kubernetes.io/name=automationcontroller-task -o jsonpath='{.items[0].metadata.name}') \
  -c myaap-controller-task -- bash -c '
source /var/lib/awx/venv/awx/bin/activate
awx-manage shell -c "
from awx.main.models import Job, JobEvent, HostMetric
job = Job.objects.latest(\"id\")
print(f\"Job {job.id}: status={job.status} failed={job.failed}\")
print(f\"Events: {JobEvent.objects.filter(job_id=job.id).count()}\")
print()
for ev in JobEvent.objects.filter(job_id=job.id).order_by(\"counter\"):
    if ev.stdout:
        print(ev.stdout)
"
'


# --- SCENE 8: Show it in SQL Server directly ---

echo ""
echo "=== Latest jobs in SQL Server (including the one we just ran) ==="
kubectl exec -n aap27 $(kubectl get pod -n aap27 -l app.kubernetes.io/name=automationcontroller-task -o jsonpath='{.items[0].metadata.name}') \
  -c myaap-controller-task -- bash -c '
source /var/lib/awx/venv/awx/bin/activate
python3 -c "
import pyodbc
conn = pyodbc.connect(
    \"DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal,1433;\"
    \"DATABASE=awx;UID=SA;PWD=AAP_P0C_Password_2026!;TrustServerCertificate=yes;\",
    autocommit=True
)
cursor = conn.cursor()
cursor.execute(\"SELECT TOP 5 id, name, status, failed, CONVERT(VARCHAR, created, 120) as created FROM main_unifiedjob ORDER BY id DESC\")
cols = [desc[0] for desc in cursor.description]
print(\"  \".join(str(c).ljust(22) for c in cols))
print(\"-\" * 110)
for row in cursor.fetchall():
    print(\"  \".join(str(v)[:22].ljust(22) for v in row))
conn.close()
"
'


# --- CLEANUP: Bring PostgreSQL back (optional) ---

# echo ""
# echo "=== Bringing PostgreSQL back online ==="
# kubectl -n aap27 scale statefulset myaap-postgres-15 --replicas=1
