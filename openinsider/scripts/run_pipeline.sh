#!/bin/bash
cd "$(dirname "$0")/../.."
source .venv/bin/activate 2>/dev/null || true
python -m openinsider.pipeline >> openinsider/data/pipeline.log 2>&1
