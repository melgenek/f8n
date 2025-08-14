#!/bin/bash
set -e

test-load() {
    for i in {1..4}; do
        python3 ./scripts/loadmap.py &
    done
    wait
    python3 ./scripts/histogram.py | awk '{print "[PERF]\t" $0}'
}

echo "Running configmap load generation script"
test-load
