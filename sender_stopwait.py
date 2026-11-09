"""
sender_stopwait.py - Stop-and-Wait sender (CP372 Assignment 2).

Sends one packet, starts a timer, and waits for its ACK. On timeout the
same packet is retransmitted. Moves on only when the ACK matches.

Usage:
  python sender_stopwait.py --file notes.txt
  python sender_stopwait.py --generate 1MB --timeout 0.05 -v
"""

import argparse
import socket
import time

from rdt_common import (ACK, BUFFER_SIZE, MAX_PAYLOAD, PacketError,
                        build_packets, describe, load_content, parse_packet,
                        print_summary)

# Receiver configuration
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 12321

TIMEOUT = 0.1       # seconds before retransmitting
MAX_RETRIES = 50    # consecutive timeouts on one packet before giving up


def wait_for_ack(sock, dest, seq, deadline, verbose):
    """Wait until `deadline` for ACK `seq`. Returns True if it arrived."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            pkt = parse_packet(data)
        except socket.timeout:
            return False
        except ConnectionResetError:
            # Windows reports an ICMP "port unreachable" here; treat as no reply
            continue
        except PacketError:
            continue

        if addr != dest or pkt.type != ACK:
            continue
        if pkt.ack == seq:
            if verbose:
                print(f"  recv {describe(pkt)}")
            return True
        # Stale/duplicate ACK: keep waiting, timer keeps running
        if verbose:
            print(f"  recv {describe(pkt)} (duplicate, ignored)")


def send_file(sock, dest, packets, timeout, max_retries, verbose):
    """Send every packet with Stop-and-Wait. Returns (retransmissions, timeouts)."""
    retransmissions = 0
    timeouts = 0

    for seq, raw in enumerate(packets):
        retries = 0
        sock.sendto(raw, dest)
        if verbose:
            print(f"send {describe(parse_packet(raw))}")

        # Timer starts after transmission
        while not wait_for_ack(sock, dest, seq, time.monotonic() + timeout, verbose):
            timeouts += 1
            retries += 1
            if retries > max_retries:
                raise TimeoutError(f"no ACK for seq {seq} after {max_retries} retries")
            sock.sendto(raw, dest)
            retransmissions += 1
            if verbose:
                print(f"TIMEOUT -> resend seq={seq}")

    return retransmissions, timeouts


def main():
    parser = argparse.ArgumentParser(description="Stop-and-Wait sender over UDP")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", help="file to send")
    source.add_argument("--generate", metavar="SIZE",
                        help="send random content of SIZE, e.g. 10KB, 5MB")
    parser.add_argument("--host", default=SERVER_HOST)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    parser.add_argument("--payload", type=int, default=MAX_PAYLOAD,
                        help="data bytes per packet")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log every packet (slow for large files)")
    args = parser.parse_args()

    name, content = load_content(args.file, args.generate)
    packets = build_packets(name, content, args.payload)
    dest = (socket.gethostbyname(args.host), args.port)

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"Stop-and-Wait: sending {name} ({len(content)} bytes, "
          f"{len(packets)} packets) to {dest[0]}:{dest[1]}")

    try:
        start = time.perf_counter()
        retransmissions, timeouts = send_file(sock, dest, packets, args.timeout,
                                              args.max_retries, args.verbose)
        elapsed = time.perf_counter() - start
        print_summary(name, len(content), len(packets), retransmissions,
                      timeouts, elapsed)
    except TimeoutError as e:
        print(f"Transfer failed: {e}. Is the receiver running?")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
