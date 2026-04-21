"""Wake mach2 (second workstation, RTX 5070 Ti) via WoL magic packet.

Mach2 WiFi adapter (Intel Wi-Fi 7 BE200) has WakeOnMagicPacket enabled and
is armed in powercfg. This sends a standard WoL magic packet (6x 0xFF +
16x MAC) as a UDP broadcast on port 9.

Usage:
    uv run python scripts/wake_mach2.py

After sending, wait ~10-15s then SSH to super@192.168.68.70. Returns
non-zero if the SSH probe fails after wake.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time

MACH2_WIFI_MAC = "D4-AB-61-45-E7-55"  # Intel Wi-Fi 7 BE200
MACH2_IP = "192.168.68.70"
BROADCAST_ADDR = "255.255.255.255"
WOL_PORT = 9  # discard port — standard WoL destination


def build_magic_packet(mac: str) -> bytes:
    """Standard WoL magic packet: 6 bytes of 0xFF, then MAC repeated 16 times."""
    mac_clean = mac.replace(":", "").replace("-", "")
    if len(mac_clean) != 12:
        msg = f"Invalid MAC length: {mac!r}"
        raise ValueError(msg)
    mac_bytes = bytes.fromhex(mac_clean)
    return b"\xff" * 6 + mac_bytes * 16


def send_wol(mac: str) -> None:
    packet = build_magic_packet(mac)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(packet, (BROADCAST_ADDR, WOL_PORT))
    print(f"Magic packet sent to {mac} via {BROADCAST_ADDR}:{WOL_PORT}")


def verify_ssh(host: str, timeout_s: int = 30) -> bool:
    """Try SSH with increasing delay; return True if mach2 answers within timeout_s."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        # S603, S607: ssh probe to known LAN host.
        result = subprocess.run(  # noqa: S603
            ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes", f"super@{host}", "echo awake"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and "awake" in result.stdout:
            print(f"mach2 is awake (SSH answered after {time.monotonic() - start:.1f}s)")
            return True
        time.sleep(3)
    print(f"mach2 did not answer SSH within {timeout_s}s")
    return False


def main() -> int:
    send_wol(MACH2_WIFI_MAC)
    if verify_ssh(MACH2_IP, timeout_s=30):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
