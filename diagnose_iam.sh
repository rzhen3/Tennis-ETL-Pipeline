#!/bin/bash
#
# diagnose_iam.sh — audit all IAM bindings for the Tennis ETL Pipeline
#
# Checks:
#   1. Required GCP APIs are enabled
#   2. Service accounts exist
#   3. Project-level IAM roles for both SAs
#   4. SA-level impersonation bindings (serviceAccountUser)
#   5. Which Airflow key file is actually in use
#
# Usage:
#     chmod +x diagnose_iam.sh
#     ./diagnose_iam.sh
#
# Prerequisites:
#     - gcloud CLI authenticated (`gcloud auth login`)
#     - sufficient IAM permissions to read policies (resourcemanager.projects.getIamPolicy)

set -uo pipefail

# ============================================================================
# CONFIGURATION — must match your project values
# ============================================================================
PROJECT_ID="tennis-etl-pipeline"
PROJECT_NUMBER="451091983898"

DATAPROC_SA="dataproc-tennis-etl@${PROJECT_ID}.iam.gserviceaccount.com"
AIRFLOW_SA="airflow-orchestrator@${PROJECT_ID}.iam.gserviceaccount.com"

# GCP agents that need impersonation rights on the Dataproc SA
DATAPROC_AGENT="service-${PROJECT_NUMBER}@dataproc-accounts.iam.gserviceaccount.com"
COMPUTE_AGENT="service-${PROJECT_NUMBER}@compute-system.iam.gserviceaccount.com"

# counters
PASS=0
FAIL=0
WARN=0

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
pass_msg()  { echo "  ✅ PASS: $1"; ((PASS++)); }
fail_msg()  { echo "  ❌ FAIL: $1"; ((FAIL++)); }
warn_msg()  { echo "  ⚠️  WARN: $1"; ((WARN++)); }
header()    { echo ""; echo "═══════════════════════════════════════════════════════════"; echo "  $1"; echo "═══════════════════════════════════════════════════════════"; }

# ============================================================================
# SECTION 1: API ENABLEMENT
# ============================================================================
header "1. REQUIRED GCP APIs"

REQUIRED_APIS=(
    "dataproc.googleapis.com"
    "bigquery.googleapis.com"
    "storage.googleapis.com"
    "bigquerystorage.googleapis.com"
    "iam.googleapis.com"
)

ENABLED_APIS=$(gcloud services list --enabled --project="${PROJECT_ID}" --format="value(config.name)" 2>/dev/null)

for api in "${REQUIRED_APIS[@]}"; do
    if echo "${ENABLED_APIS}" | grep -q "^${api}$"; then
        pass_msg "${api}"
    else
        fail_msg "${api} — NOT ENABLED"
    fi
done

# ============================================================================
# SECTION 2: SERVICE ACCOUNTS EXIST
# ============================================================================
header "2. SERVICE ACCOUNTS EXIST"

for sa in "${DATAPROC_SA}" "${AIRFLOW_SA}"; do
    if gcloud iam service-accounts describe "${sa}" --project="${PROJECT_ID}" &>/dev/null; then
        pass_msg "${sa}"
    else
        fail_msg "${sa} — DOES NOT EXIST"
    fi
done


# ============================================================================
# SECTION 3: PROJECT-LEVEL IAM ROLES
# ============================================================================
header "3. PROJECT-LEVEL IAM ROLES"

# Fetch the full IAM policy once (avoids repeated API calls)
IAM_POLICY=$(gcloud projects get-iam-policy "${PROJECT_ID}" --format=json 2>/dev/null)

check_project_role() {
    local sa_email="$1"
    local role="$2"
    local label="$3"

    # check if the SA is a member of the given role in the policy JSON
    local found
    found=$(echo "${IAM_POLICY}" | python3 -c "
import sys, json
policy = json.load(sys.stdin)
sa = 'serviceAccount:${sa_email}'
for b in policy.get('bindings', []):
    if b['role'] == '${role}' and sa in b.get('members', []):
        print('yes')
        sys.exit(0)
print('no')
" 2>/dev/null)

    if [[ "${found}" == "yes" ]]; then
        pass_msg "${label}: ${role}"
    else
        fail_msg "${label}: ${role} — MISSING"
    fi
}

echo ""
echo "  --- Dataproc Job Runner SA (${DATAPROC_SA}) ---"
check_project_role "${DATAPROC_SA}" "roles/dataproc.worker"       "dataproc-sa"
check_project_role "${DATAPROC_SA}" "roles/bigquery.dataEditor"   "dataproc-sa"
check_project_role "${DATAPROC_SA}" "roles/bigquery.jobUser"      "dataproc-sa"
check_project_role "${DATAPROC_SA}" "roles/storage.objectViewer"  "dataproc-sa"

echo ""
echo "  --- Airflow Orchestrator SA (${AIRFLOW_SA}) ---"
check_project_role "${AIRFLOW_SA}" "roles/dataproc.editor"       "airflow-sa"
check_project_role "${AIRFLOW_SA}" "roles/storage.objectAdmin"   "airflow-sa"
check_project_role "${AIRFLOW_SA}" "roles/bigquery.dataViewer"   "airflow-sa"
check_project_role "${AIRFLOW_SA}" "roles/bigquery.jobUser"      "airflow-sa"


# ============================================================================
# SECTION 4: SA-LEVEL IMPERSONATION BINDINGS
# ============================================================================
header "4. IMPERSONATION BINDINGS ON DATAPROC SA"

# Fetch the IAM policy on the Dataproc SA itself
DATAPROC_SA_POLICY=$(gcloud iam service-accounts get-iam-policy "${DATAPROC_SA}" --format=json 2>/dev/null)

check_sa_binding() {
    local member_sa="$1"
    local role="$2"
    local label="$3"

    local found
    found=$(echo "${DATAPROC_SA_POLICY}" | python3 -c "
import sys, json
policy = json.load(sys.stdin)
member = 'serviceAccount:${member_sa}'
for b in policy.get('bindings', []):
    if b['role'] == '${role}' and member in b.get('members', []):
        print('yes')
        sys.exit(0)
print('no')
" 2>/dev/null)

    if [[ "${found}" == "yes" ]]; then
        pass_msg "${label}"
    else
        fail_msg "${label} — MISSING"
    fi
}

echo ""
echo "  Who can impersonate ${DATAPROC_SA}?"
check_sa_binding "${DATAPROC_AGENT}" "roles/iam.serviceAccountUser" \
    "Dataproc agent (service-${PROJECT_NUMBER}@dataproc-accounts) → serviceAccountUser"

check_sa_binding "${COMPUTE_AGENT}" "roles/iam.serviceAccountUser" \
    "Compute agent (service-${PROJECT_NUMBER}@compute-system) → serviceAccountUser"

check_sa_binding "${AIRFLOW_SA}" "roles/iam.serviceAccountUser" \
    "Airflow SA (${AIRFLOW_SA}) → serviceAccountUser"


# ============================================================================
# SECTION 5: AIRFLOW KEY FILE CHECK
# ============================================================================
header "5. AIRFLOW CREDENTIAL FILE"

echo "  docker-compose.yaml sets:"
echo "    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/gcs.json"
echo ""

# check which key files exist locally
if [[ -f "./keys/gcs.json" ]]; then
    # extract the SA email from the key file
    KEY_SA=$(python3 -c "import json; print(json.load(open('./keys/gcs.json'))['client_email'])" 2>/dev/null || echo "UNREADABLE")
    pass_msg "keys/gcs.json exists — SA: ${KEY_SA}"

    # warn if the key doesn't match either known SA
    if [[ "${KEY_SA}" != "${DATAPROC_SA}" && "${KEY_SA}" != "${AIRFLOW_SA}" ]]; then
        warn_msg "Key SA '${KEY_SA}' does not match either known SA."
        echo "         Verify this SA has the necessary permissions."
    fi
else
    fail_msg "keys/gcs.json NOT FOUND in project root"
fi

if [[ -f "./keys/airflow-sa-key.json" ]]; then
    KEY_SA2=$(python3 -c "import json; print(json.load(open('./keys/airflow-sa-key.json'))['client_email'])" 2>/dev/null || echo "UNREADABLE")
    echo "  ℹ️  Also found keys/airflow-sa-key.json — SA: ${KEY_SA2}"
fi

if [[ -f "./keys/dataproc-sa-key.json" ]]; then
    KEY_SA3=$(python3 -c "import json; print(json.load(open('./keys/dataproc-sa-key.json'))['client_email'])" 2>/dev/null || echo "UNREADABLE")
    echo "  ℹ️  Also found keys/dataproc-sa-key.json — SA: ${KEY_SA3}"
fi


# ============================================================================
# SUMMARY
# ============================================================================
header "SUMMARY"
echo ""
echo "  ✅ Passed: ${PASS}"
echo "  ❌ Failed: ${FAIL}"
echo "  ⚠️  Warnings: ${WARN}"
echo ""

if [[ ${FAIL} -gt 0 ]]; then
    echo "  ACTION REQUIRED: Fix the ${FAIL} failed check(s) above."
    echo ""
    echo "  To fix missing project-level roles, run:"
    echo "    gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
    echo "      --member=\"serviceAccount:<SA_EMAIL>\" \\"
    echo "      --role=\"<MISSING_ROLE>\" --quiet"
    echo ""
    echo "  To fix missing impersonation bindings, run:"
    echo "    gcloud iam service-accounts add-iam-policy-binding ${DATAPROC_SA} \\"
    echo "      --member=\"serviceAccount:<AGENT_EMAIL>\" \\"
    echo "      --role=\"roles/iam.serviceAccountUser\""
    echo ""
else
    echo "  All checks passed! IAM configuration looks correct."
fi

echo ""