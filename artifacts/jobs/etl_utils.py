"""
file superseded by etl_utils wheel package.
source code now lives at:
    artifacts/etl_utils_pkg/src/etl_utils/

build wheel:
    ./scripts/build_wheel.sh

installing locally:
pip install -e ./artifacts/etl_utils_pkg[dev]

"""

try:
    from etl_utils._core import *

except ImportError:
    raise ImportError(
        "etl_utils package not found. Either:\n"
        "  1. Install the wheel: pip install ./artifacts/etl_utils_pkg\n"
        "  2. Build and install: ./scripts/build_wheel.sh && "
        "pip install artifacts/wheels/etl_utils-*.whl\n"
    )