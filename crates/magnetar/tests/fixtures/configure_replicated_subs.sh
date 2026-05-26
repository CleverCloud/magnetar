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
#   - cluster-a admin: http://localhost:8080
#   - cluster-b admin: http://localhost:8081
#
# Idempotent: re-running is safe (each command tolerates AlreadyExists).

set -euo pipefail

ADMIN_A="${ADMIN_A:-http://localhost:8080}"
ADMIN_B="${ADMIN_B:-http://localhost:8081}"

echo "[pip-33] registering peer clusters in both admin stores"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_A}" clusters create cluster-a \
  --url "${ADMIN_A}" --broker-url pulsar://broker-a:6650 || true
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_A}" clusters create cluster-b \
  --url "http://broker-b:8080" --broker-url pulsar://broker-b:6650 || true
docker exec magnetar-pip33-broker-b bin/pulsar-admin --admin-url "${ADMIN_B}" clusters create cluster-a \
  --url "http://broker-a:8080" --broker-url pulsar://broker-a:6650 || true
docker exec magnetar-pip33-broker-b bin/pulsar-admin --admin-url "${ADMIN_B}" clusters create cluster-b \
  --url "${ADMIN_B}" --broker-url pulsar://broker-b:6650 || true

echo "[pip-33] opening public tenant to both clusters"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_A}" tenants update public \
  --allowed-clusters cluster-a,cluster-b || true

echo "[pip-33] adding cluster-b to public/default replication clusters"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_A}" namespaces set-clusters \
  public/default --clusters cluster-a,cluster-b

echo "[pip-33] enabling replicated subscription status on public/default"
docker exec magnetar-pip33-broker-a bin/pulsar-admin --admin-url "${ADMIN_A}" namespaces \
  set-replicated-subscription-status public/default --enable

echo "[pip-33] fixture ready — cluster-a @ ${ADMIN_A}, cluster-b @ ${ADMIN_B}"
