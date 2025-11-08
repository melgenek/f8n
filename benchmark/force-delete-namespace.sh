#!/bin/bash

# Usage: ./force-delete-namespace.sh <namespace-name>

set -e
set -o pipefail

NAMESPACE=$1

if [ -z "$NAMESPACE" ]; then
  echo "Usage: $0 <namespace-name>"
  exit 1
fi

echo "Fetching namespace $NAMESPACE..."
kubectl get namespace "$NAMESPACE" -o json > "${NAMESPACE}.json"

echo "Removing finalizer from ${NAMESPACE}.json..."
# Use a temporary file for sed to work on both Linux and macOS
sed -i.bak '/"finalizers": \[/,/\]/d' "${NAMESPACE}.json"
rm "${NAMESPACE}.json.bak"

echo "Starting kubectl proxy in the background..."
kubectl proxy &
PROXY_PID=$!

# Give the proxy a moment to start
sleep 2

echo "Applying the modified namespace definition..."
curl -k -H "Content-Type: application/json" -X PUT --data-binary @"${NAMESPACE}.json" "http://127.0.0.1:8001/api/v1/namespaces/${NAMESPACE}/finalize"

echo "Stopping kubectl proxy..."
kill $PROXY_PID

# Wait for the proxy to shut down
wait $PROXY_PID 2>/dev/null

rm "${NAMESPACE}.json"

echo "Namespace '$NAMESPACE' should now be deleted."
