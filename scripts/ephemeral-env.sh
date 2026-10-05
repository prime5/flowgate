#!/usr/bin/env bash
# ephemeral-env.sh — the whole playground as one command: spin up a
# kind cluster, build and load the flowgate image, deploy it, run the
# k6 sync-stall Job, stream the results, tear it all down. Ephemeral
# environments for fault experiments, the way a load-and-fault
# platform team would run them.
#
# Usage: ./scripts/ephemeral-env.sh [--keep]   (--keep skips teardown)
set -euo pipefail

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

CLUSTER="flowgate-lab"
IMAGE="flowgate:resilience-experiments"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }
need kind
need kubectl
need docker

cd "$(dirname "$0")/.."

# Tear down on any exit, including a failed step, unless --keep.
cleanup() {
  if [[ "$KEEP" -eq 0 ]]; then
    echo "==> tearing down"
    kind delete cluster --name "$CLUSTER"
  else
    echo "==> kept cluster $CLUSTER (kubectl context: kind-$CLUSTER)"
  fi
}

echo "==> building $IMAGE"
docker build -t "$IMAGE" .

echo "==> creating kind cluster $CLUSTER"
kind create cluster --name "$CLUSTER" --wait 60s
trap cleanup EXIT
kubectl config use-context "kind-$CLUSTER"

echo "==> loading image into kind"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> installing metrics-server (the HPA needs CPU metrics)"
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
# kind's kubelets use self-signed certs.
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

echo "==> deploying flowgate"
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/hpa.yaml
kubectl rollout status deployment/flowgate --timeout=120s

echo "==> installing k6 script"
kubectl create configmap flowgate-k6-scripts --from-file=experiments/sync-stall.js
kubectl apply -f deploy/k8s/job.yaml

echo "==> running sync-stall experiment"
kubectl wait --for=condition=complete --timeout=180s job/flowgate-sync-stall || true
kubectl logs job/flowgate-sync-stall

echo "==> HPA state after the run"
kubectl get hpa flowgate || true
