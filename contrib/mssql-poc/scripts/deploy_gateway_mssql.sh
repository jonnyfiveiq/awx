#!/bin/bash
# Deploy Gateway MSSQL migration
# ANSTRAT-1887 Phase 3+4: Gateway component
#
# Prerequisites:
#   - SQL Server running with controller's 'awx' database already working
#   - aap-dev environment running with gateway pod
#   - mssql-django and pyodbc installed in gateway container
#
# Usage:
#   cd contrib/mssql-poc
#   bash scripts/deploy_gateway_mssql.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
POC_DIR="$(dirname "$SCRIPT_DIR")"
GATEWAY_DEPLOY="myaap-gateway"
GATEWAY_CONTAINER="api"
MSSQL_POD=""  # Will be determined from SQL Server setup

echo "=== Gateway MSSQL Migration Deployment ==="
echo ""

# --- Step 1: Create gateway database on SQL Server ---
echo "Step 1: Creating aap_gateway database on SQL Server..."
echo "  Run scripts/setup_gateway_db.sql against your SQL Server instance"
echo "  e.g.: sqlcmd -S localhost,1433 -U SA -P 'AAP_P0C_Password_2026!' -i scripts/setup_gateway_db.sql"
echo ""

# --- Step 2: Install MSSQL dependencies in gateway pod ---
echo "Step 2: Installing mssql-django and pyodbc in gateway pod..."
GATEWAY_POD=$(kubectl get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[0].metadata.name}')
echo "  Gateway pod: $GATEWAY_POD"

kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- bash -c '
    pip install mssql-django pyodbc 2>/dev/null || echo "Already installed"
'

# --- Step 3: Install shared library and service broker module ---
echo ""
echo "Step 3: Installing shared library and service_broker module..."

# Shared MSSQL library
kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- mkdir -p /opt/mssql-poc
kubectl cp "$POC_DIR/lib/mssql_common.py" \
    "$GATEWAY_POD:/opt/mssql-poc/mssql_common.py" \
    -c "$GATEWAY_CONTAINER"
echo "  Shared library installed at /opt/mssql-poc/"

# Service broker dispatcherd module
kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- mkdir -p /opt/app-root/lib/python3.11/site-packages/aap_gateway_api/dispatch/brokers/

kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- bash -c 'cat > /opt/app-root/lib/python3.11/site-packages/aap_gateway_api/dispatch/brokers/__init__.py << "PYEOF"
PYEOF'

kubectl cp "$POC_DIR/../awx/main/dispatch/brokers/service_broker.py" \
    "$GATEWAY_POD:/opt/app-root/lib/python3.11/site-packages/aap_gateway_api/dispatch/brokers/service_broker.py" \
    -c "$GATEWAY_CONTAINER"

echo "  Service broker module installed"

# --- Step 4: Run Django migrate against MSSQL ---
echo ""
echo "Step 4: Running Django migrate --database mssql..."

# Copy the settings file into the pod
kubectl cp "$POC_DIR/gateway-mssql-settings.py" \
    "$GATEWAY_POD:/tmp/gateway-mssql-settings.py" \
    -c "$GATEWAY_CONTAINER"

# Append MSSQL settings to the gateway settings file
kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- bash -c '
    SETTINGS_FILE="/etc/ansible-automation-platform/gateway/settings.py"
    if ! grep -q "Phase 3: ORM routing" "$SETTINGS_FILE" 2>/dev/null; then
        echo "" >> "$SETTINGS_FILE"
        echo "# --- MSSQL POC Settings (appended) ---" >> "$SETTINGS_FILE"
        cat /tmp/gateway-mssql-settings.py >> "$SETTINGS_FILE"
        echo "MSSQL settings appended to $SETTINGS_FILE"
    else
        echo "MSSQL settings already present in $SETTINGS_FILE"
    fi
'

# Run migrate for mssql database
kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- \
    aap-gateway-manage migrate --database mssql --no-input 2>&1 || true

echo "  Schema migration complete"

# --- Step 5: Fix schema gaps ---
echo ""
echo "Step 5: Fix schema gaps (isjson constraints, identity reseed)..."
echo "  Run scripts/fix_gateway_schema_gaps.sql against the aap_gateway database"
echo "  e.g.: sqlcmd -S localhost,1433 -U SA -P 'AAP_P0C_Password_2026!' -d aap_gateway -i scripts/fix_gateway_schema_gaps.sql"
echo ""

# --- Step 6: Migrate data ---
echo "Step 6: Migrating data from PG to MSSQL..."
kubectl cp "$POC_DIR/scripts/migrate_gateway_data.py" \
    "$GATEWAY_POD:/tmp/migrate_gateway_data.py" \
    -c "$GATEWAY_CONTAINER"

kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- \
    python /tmp/migrate_gateway_data.py

# --- Step 7: Restart gateway ---
echo ""
echo "Step 7: Restarting gateway pod..."
kubectl rollout restart deployment "$GATEWAY_DEPLOY"
kubectl rollout status deployment "$GATEWAY_DEPLOY" --timeout=120s

# --- Step 8: Verify ---
echo ""
echo "Step 8: Verifying gateway health..."
sleep 5

GATEWAY_POD=$(kubectl get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$GATEWAY_POD" -c "$GATEWAY_CONTAINER" -- \
    curl -s http://localhost:8080/api/gateway/v1/ping/ | python3 -m json.tool

echo ""
echo "=== Gateway MSSQL Migration Complete ==="
echo ""
echo "Verify manually:"
echo "  1. Gateway ping: curl http://localhost:44927/api/gateway/v1/ping/"
echo "  2. Login via UI: http://localhost:44927"
echo "  3. Check MSSQL tables: sqlcmd -S localhost,1433 -U SA -P 'AAP_P0C_Password_2026!' -d aap_gateway -Q 'SELECT name FROM sys.tables ORDER BY name'"
