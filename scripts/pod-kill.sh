#!/usr/bin/env bash
# pod-kill.sh — fault primitive against real pods: delete pods matching
# a selector so the ReplicaSet recreates them. This is the production
# version of POST /experiments/pod-kill (which does the same thing to
# the in-process replica pool).
#
# Usage: ./scripts/pod-kill.sh [--namespace ns] [--selector app=flowgate] [--count N]
set -euo pipefail

NAMESPACE="default"
SELECTOR="app=flowgate"
COUNT=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --selector)  SELECTOR="$2";  shift 2 ;;
    --count)     COUNT="$2";     shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--count must be a positive integer, got: $COUNT" >&2
  exit 1
fi

pods=$(kubectl get pods -n "$NAMESPACE" -l "$SELECTOR" -o jsonpath='{.items[*].metadata.name}')
read -ra PODLIST <<< "$pods"
total=${#PODLIST[@]}
if [[ "$total" -eq 0 ]]; then
  echo "no pods match selector $SELECTOR in namespace $NAMESPACE" >&2
  exit 1
fi

# Blast-radius guard: never kill more than half the matching pods.
max_kill=$(( total / 2 ))
[[ "$max_kill" -lt 1 ]] && max_kill=1
if [[ "$COUNT" -gt "$max_kill" ]]; then
  echo "blast-radius guard: refusing to kill $COUNT of $total pods (cap is $max_kill)" >&2
  exit 1
fi

echo "killing $COUNT of $total pods (selector=$SELECTOR ns=$NAMESPACE)"
for (( i=0; i<COUNT; i++ )); do
  kubectl delete pod -n "$NAMESPACE" "${PODLIST[$i]}" --wait=false
done
echo "done — watch recovery with: kubectl get pods -n $NAMESPACE -l $SELECTOR -w"
