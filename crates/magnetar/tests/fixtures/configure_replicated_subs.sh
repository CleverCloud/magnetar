#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Post-up setup for the two-cluster PIP-33 fixture
# (`docker-compose.replicated-subs.yml`). Creates both clusters in each
# other's metadata, opens the public tenant to both clusters, and turns on
# replicated-subscription-status on the default namespace.
#
# Usage:
#   ./configure_replicated_subs.sh
#
# Assumes both brokers are healthy on:
#   - cluster-a admin: http://localhost:18080 (host port — see
#     docker-compose.replicated-subs.yml for why we're off the
#     default 8080)
#   - cluster-b admin: http://localhost:18081
#
# Idempotent: re-running is safe (each command tolerates AlreadyExists).

set -euo pipefail

# Host-port mappings used by the e2e test on the host (advertised back to
# the caller). Inside `docker exec` we must use each broker's INTERNAL
# admin port (8080 for both — see docker-compose.replicated-subs.yml's
# `webServicePort=8080`); the 18081 host port is only the *mapping* for
# broker-b and is not bound inside the container.
ADMIN_A_HOST="${ADMIN_A_HOST:-http://localhost:18080}"
ADMIN_B_HOST="${ADMIN_B_HOST:-http://localhost:18081}"
ADMIN_INTERNAL="http://localhost:8080"

# Wait until both brokers can answer cluster admin queries — the broker
# health probe goes green before the metadata cache warms, so `clusters
# list` is the better readiness signal for what follows.
wait_for_admin_ready() {
  local url="$1" attempts=60
  until curl -sf "${url}/admin/v2/clusters" >/dev/null 2>&1; do
    attempts=$((attempts - 1))
    if [ "$attempts" -le 0 ]; then
      echo "[pip-33] admin REST never came up on ${url}" >&2
      return 1
    fi
    sleep 2
  done
}
echo "[pip-33] waiting for admin REST to be ready on both brokers"
wait_for_admin_ready "${ADMIN_A_HOST}"
wait_for_admin_ready "${ADMIN_B_HOST}"

echo "[pip-33] registering peer clusters in both admin stores"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" clusters create cluster-a \
  --url "http://broker-a:8080" --broker-url pulsar://broker-a:6650 || true
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" clusters create cluster-b \
  --url "http://broker-b:8080" --broker-url pulsar://broker-b:6650 || true
docker exec magnetar-pip33-broker-b bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" clusters create cluster-a \
  --url "http://broker-a:8080" --broker-url pulsar://broker-a:6650 || true
docker exec magnetar-pip33-broker-b bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" clusters create cluster-b \
  --url "http://broker-b:8080" --broker-url pulsar://broker-b:6650 || true

# Pulsar in full-cluster mode (vs. `standalone`) does NOT auto-bootstrap
# the `public` tenant or the `public/default` namespace — that's a
# standalone-mode convenience. Create both explicitly so the rest of the
# script (and the e2e test) finds them.
echo "[pip-33] creating public tenant + public/default namespace"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" tenants create public \
  --allowed-clusters cluster-a,cluster-b --admin-roles '' || true
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" namespaces create \
  public/default --clusters cluster-a,cluster-b || true

echo "[pip-33] opening public tenant to both clusters"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" tenants update public \
  --allowed-clusters cluster-a,cluster-b || true

echo "[pip-33] adding cluster-b to public/default replication clusters"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_INTERNAL}" namespaces set-clusters \
  public/default --clusters cluster-a,cluster-b

# Replicated subscription status is set at the *topic* or *subscription*
# level in Pulsar 4.x (no `namespaces set-replicated-subscription-status`
# subcommand exists). The e2e test calls `pulsar-admin topics
# set-replicated-subscription-status <topic> --enable` itself once it
# has created its topic, OR sets `replicateSubscriptionState(true)` on
# the consumer subscribe call. No namespace-level action needed here.

echo "[pip-33] fixture ready — cluster-a @ ${ADMIN_A_HOST}, cluster-b @ ${ADMIN_B_HOST}"
