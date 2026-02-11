#!/usr/bin/env bash
#
# build_wheel.sh - builds the etl_utils wheel and stage for upload.
#
# 1. builds the wheel from pyproject.toml configuration
# 2. copies .whl file to artifacts/wheels/ where the DAG's 
#     upload_jobs_to_GCP_bucket task will find and upload it
# 3. cleans up build artifacts
#
# Usage:
#     ./scripts/build_wheel.sh
#
# Prerequisites:
#     pip install build 


set -euo pipefail
# -e exit immediately
# -u treat unset variables as errors
# -o pipefail fails if any command fails

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PKG_DIR="${PROJECT_ROOT}/artifacts/etl_utils_pkg"
WHEELS_DIR="${PROJECT_ROOT}/artifacts/wheels"

echo "--- building etl_utils wheel ---"
echo "package dir: ${PKG_DIR}"
echo "output dir: ${WHEELS_DIR}"

# check if build tool is installed
if ! python -m build --version &>/dev/null; then
    echo "Installing 'build' package..."
    pip install build --quiet --break-system-packages
fi

# clean previous build artifacts
echo "Cleaning previous builds..."
rm -rf "${PKG_DIR}/dist" "${PKG_DIR}/build" "${PKG_DIR}/src/"*.egg-info

# build wheel
echo "Building wheel..."
python -m build --wheel --outdir "${PKG_DIR}/dist" "${PKG_DIR}"


# copy wheel to artifacts/wheels
mkdir -p "${WHEELS_DIR}"

WHEEL_FILE=$(find "${PKG_DIR}/dist" -name "*.whl" -type f | head -1)

if [[ -z "${WHEEL_FILE}" ]]; then
    echo "ERROR: No .whl file found in ${PKG_DIR}/dist/"
    exit 1
fi

cp "${WHEEL_FILE}" "${WHEELS_DIR}/"
WHEEL_NAME=$(basename "${WHEEL_FILE}")
echo "copied ${WHEEL_NAME} -> ${WHEELS_DIR}"

# clean up build directories
rm -rf "${PKG_DIR}/build" "${PKG_DIR}/src/"*.egg-info

echo "--- build complete ---"
echo "Wheel: ${WHEELS_DIR}/${WHEEL_NAME}"
echo ""
echo "Next steps:"
echo "  - DAG's upload_jobs_to_GCP_bucket will upload this to GCS"
echo "  - Dataproc batch config should reference it in 'python_file_uris'"
echo "  - Or install locally for testing: pip install ${WHEELS_DIR}/${WHEEL_NAME}"