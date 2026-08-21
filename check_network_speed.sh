#!/bin/bash
# Measures the current inbound network speed on the host by reading /proc/net/dev
# twice, 10 seconds apart, and computing the MB/s from the RX bytes difference.

INTERVAL=10

# Find the active interface (first of eth0, ens*, enp*, or the first non-lo interface)
IFACE=$(grep -E '^\s+(eth0|ens[0-9]|enp[0-9])' /proc/net/dev | head -1 | awk -F: '{print $1}' | tr -d ' ')
if [ -z "$IFACE" ]; then
    IFACE=$(grep -v -E '^\s*(lo|Inter|face)' /proc/net/dev | head -1 | awk -F: '{print $1}' | tr -d ' ')
fi

if [ -z "$IFACE" ]; then
    echo "ERROR: no network interface found in /proc/net/dev" >&2
    exit 1
fi

echo "Interface: $IFACE — sampling over ${INTERVAL}s..."

read_rx_bytes() {
    grep -E "^\s*${IFACE}:" /proc/net/dev | awk '{print $2}'
}

RX1=$(read_rx_bytes)
sleep "$INTERVAL"
RX2=$(read_rx_bytes)

DIFF=$(( RX2 - RX1 ))

echo ""
echo "RX bytes at t=0 :  $RX1"
echo "RX bytes at t=${INTERVAL}s:  $RX2"
echo "Delta           :  $DIFF bytes"
echo ""

awk -v diff="$DIFF" -v interval="$INTERVAL" 'BEGIN {
    bps  = diff / interval
    kbps = bps / 1024
    mbps = bps / 1048576
    printf "Download speed  :  %.1f KB/s  (%.3f MB/s)\n", kbps, mbps
}'
