#!/bin/bash
# setup_thunderbolt.sh — Configure Thunderbolt Bridge for exo peer-to-peer inference
#
# Run on BOTH Macs. Mac A uses .1, Mac B uses .2.
# These IPs are only set until reboot; re-run the script after reboot.
#
# Usage:
#   Mac A: ./scripts/setup_thunderbolt.sh a
#   Mac B: ./scripts/setup_thunderbolt.sh b

set -euo pipefail

SUBNET="10.100.0"
MAC_A_IP="${SUBNET}.1"
MAC_B_IP="${SUBNET}.2"
NETMASK="255.255.255.0"
EXO_PORT=50001

usage() {
  echo "Usage: $0 <a|b>"
  echo "  a = this Mac is 'Mac A' (gets IP ${MAC_A_IP})"
  echo "  b = this Mac is 'Mac B' (gets IP ${MAC_B_IP})"
  exit 1
}

[[ $# -ne 1 ]] && usage
ROLE="${1,,}"
[[ "$ROLE" != "a" && "$ROLE" != "b" ]] && usage

if [[ "$ROLE" == "a" ]]; then
  MY_IP="$MAC_A_IP"
  PEER_IP="$MAC_B_IP"
else
  MY_IP="$MAC_B_IP"
  PEER_IP="$MAC_A_IP"
fi

# --- 1. Assign static IP to bridge0 ---
echo "→ Assigning ${MY_IP} to bridge0 ..."
sudo ifconfig bridge0 "${MY_IP}" netmask "${NETMASK}" up

# --- 2. Verify the interface is up ---
echo "→ bridge0 status:"
ifconfig bridge0 | grep -E "inet |status"

# --- 3. Test reachability (optional, non-fatal) ---
echo "→ Pinging peer ${PEER_IP} (3 packets) ..."
if ping -c 3 -t 3 "${PEER_IP}" &>/dev/null; then
  echo "  ✓ Peer is reachable at ${PEER_IP}"
else
  echo "  ✗ Peer not reachable yet — make sure the other Mac has also run this script."
fi

# --- 4. Print the exo command to run ---
echo ""
echo "============================================================"
echo "  Setup complete. Run exo with these flags:"
echo ""
if [[ "$ROLE" == "a" ]]; then
  echo "  Mac A (this machine):"
  echo "    uv run exo --listen-address ${MY_IP} --libp2p-port ${EXO_PORT}"
  echo ""
  echo "  Mac B (other machine — after running setup_thunderbolt.sh b):"
  echo "    uv run exo --listen-address ${PEER_IP} --libp2p-port ${EXO_PORT} \\"
  echo "               --bootstrap-peers /ip4/${MY_IP}/tcp/${EXO_PORT}"
else
  echo "  Mac A bootstrap command to give to Mac A:"
  echo "    uv run exo --listen-address ${PEER_IP} --libp2p-port ${EXO_PORT}"
  echo ""
  echo "  Mac B (this machine):"
  echo "    uv run exo --listen-address ${MY_IP} --libp2p-port ${EXO_PORT} \\"
  echo "               --bootstrap-peers /ip4/${PEER_IP}/tcp/${EXO_PORT}"
fi
echo "============================================================"
