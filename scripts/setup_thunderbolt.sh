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
ROLE="$(echo "$1" | tr '[:upper:]' '[:lower:]')"
[[ "$ROLE" != "a" && "$ROLE" != "b" ]] && usage

if [[ "$ROLE" == "a" ]]; then
  MY_IP="$MAC_A_IP"
  PEER_IP="$MAC_B_IP"
else
  MY_IP="$MAC_B_IP"
  PEER_IP="$MAC_A_IP"
fi

# --- 1. Find the active Thunderbolt interface ---
# macOS creates bridge0 to bridge en1/en2/en3, but IP routing through bridge0
# has a kernel bug (sendto → EHOSTUNREACH despite valid ARP). Assigning the IP
# directly to the active Thunderbolt interface (en1/en2/en3) sidesteps this.
echo "→ Detecting active Thunderbolt interface ..."
TB_IFACE=""

# First: check bridge0 members (explicit Thunderbolt Bridge setup)
for member in $(ifconfig bridge0 2>/dev/null | awk '/member:/{print $2}'); do
  if ifconfig "$member" 2>/dev/null | grep -q "status: active"; then
    TB_IFACE="$member"
    break
  fi
done

# Second: use networksetup to find hardware-identified Thunderbolt interfaces.
# This correctly excludes WiFi which shares the same en* naming scheme.
if [[ -z "$TB_IFACE" ]]; then
  current_port=""
  while IFS= read -r line; do
    line="${line#"${line%%[![:space:]]*}"}"  # trim leading whitespace
    if [[ "$line" == Hardware\ Port:* ]]; then
      current_port="${line#Hardware Port: }"
    elif [[ "$line" == Device:* && "$current_port" == *Thunderbolt* ]]; then
      dev="${line#Device: }"
      if [[ -n "$dev" ]] && ifconfig "$dev" 2>/dev/null | grep -q "status: active"; then
        TB_IFACE="$dev"
        break
      fi
    fi
  done < <(networksetup -listallhardwareports 2>/dev/null)
fi

# Fallback: check ONLY the interfaces listed as "Thunderbolt N" by networksetup.
# Do NOT scan all en1-en5 — that would pick up USB Ethernet adapters (en4/en5)
# that happen to be active.
if [[ -z "$TB_IFACE" ]]; then
  current_port=""
  while IFS= read -r line; do
    line="${line#"${line%%[![:space:]]*}"}"  # trim leading whitespace
    if [[ "$line" == Hardware\ Port:* ]]; then
      current_port="${line#Hardware Port: }"
    elif [[ "$line" == Device:* && "$current_port" =~ ^Thunderbolt[[:space:]][0-9]+$ ]]; then
      dev="${line#Device: }"
      if [[ -n "$dev" ]] && ifconfig "$dev" 2>/dev/null | grep -q "status: active"; then
        TB_IFACE="$dev"
        break
      fi
    fi
  done < <(networksetup -listallhardwareports 2>/dev/null)
fi
if [[ -z "$TB_IFACE" ]]; then
  echo "  ✗ No active Thunderbolt interface found. Is the cable connected?"
  echo "    Falling back to bridge0."
  TB_IFACE="bridge0"
fi
echo "  → Using interface: ${TB_IFACE}"

# --- 2. Clean up bridge0 to prevent routing conflicts ---
if [[ "$TB_IFACE" != "bridge0" ]]; then
  # Remove the interface from bridge0 so it becomes a simple point-to-point link.
  if ifconfig bridge0 2>/dev/null | grep -q "member: ${TB_IFACE}"; then
    echo "→ Removing ${TB_IFACE} from bridge0 (avoids bridge routing conflicts) ..."
    sudo ifconfig bridge0 deletem "${TB_IFACE}" 2>/dev/null || true
  fi
  # Remove any stale 10.100.x.x alias from bridge0 that would conflict.
  for alias_ip in "${MAC_A_IP}" "${MAC_B_IP}"; do
    if ifconfig bridge0 2>/dev/null | grep -q "inet ${alias_ip}"; then
      sudo ifconfig bridge0 -alias "${alias_ip}" 2>/dev/null || true
    fi
  done
fi

# --- 3. Clean up stale 10.100.x.x IPs on all Thunderbolt interfaces ---
# Previous runs may have left stale aliases on inactive TB ports, causing routing
# confusion (multiple interfaces claiming the same subnet).
current_port=""
while IFS= read -r line; do
  line="${line#"${line%%[![:space:]]*}"}"
  if [[ "$line" == Hardware\ Port:* ]]; then
    current_port="${line#Hardware Port: }"
  elif [[ "$line" == Device:* && "$current_port" =~ Thunderbolt ]]; then
    dev="${line#Device: }"
    [[ -z "$dev" || "$dev" == "$TB_IFACE" ]] && continue
    for stale_ip in "${MAC_A_IP}" "${MAC_B_IP}"; do
      if ifconfig "$dev" 2>/dev/null | grep -q "inet ${stale_ip}"; then
        echo "  → Removing stale ${stale_ip} from ${dev} ..."
        sudo ifconfig "$dev" -alias "${stale_ip}" 2>/dev/null || true
      fi
    done
  fi
done < <(networksetup -listallhardwareports 2>/dev/null)

# --- 4. Assign static IP to the Thunderbolt interface ---
echo "→ Assigning ${MY_IP} to ${TB_IFACE} ..."
sudo ifconfig "${TB_IFACE}" "${MY_IP}" netmask "${NETMASK}" up

# --- 5. Flush stale ARP/route entries for the peer ---
sudo route delete -host "${PEER_IP}" 2>/dev/null || true
sudo arp -d "${PEER_IP}" 2>/dev/null || true

# --- 6. Verify the interface is up ---
echo "→ ${TB_IFACE} status:"
ifconfig "${TB_IFACE}" | grep -E "inet |status"

# --- 7. Test reachability (optional, non-fatal) ---
echo "→ Pinging peer ${PEER_IP} (3 packets) ..."
if ping -c 3 -t 3 "${PEER_IP}" &>/dev/null; then
  echo "  ✓ Peer is reachable at ${PEER_IP}"
else
  echo "  ✗ Peer not reachable yet — make sure the other Mac has also run this script."
fi

# --- 4. Print the exo command to run ---
echo ""
echo "============================================================"
echo "  Setup complete."
echo ""
echo "  Run on this machine:"
echo "    uv run exo"
echo ""
if [[ "$ROLE" == "a" ]]; then
  echo "  Run on Mac B (after running setup_thunderbolt.sh b there):"
  echo "    uv run exo"
else
  echo "  Run on Mac A (after running setup_thunderbolt.sh a there):"
  echo "    uv run exo"
fi
echo ""
echo "  exo auto-detects the bridge IP and the peer — no extra flags needed."
echo "============================================================"
