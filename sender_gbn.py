"""
sender_gbn.py - Go-Back-N sender (CP372 Assignment 2).

Keeps up to window_size unACKed packets in flight. One timer runs for the
oldest unACKed packet (base). ACKs are cumulative: ACK n confirms 0..n.
On timeout, every packet in [base, nextseqnum) is retransmitted.

Usage:
  python sender_gbn.py --file notes.txt --window 4
  python sender_gbn.py --generate 1MB --window 16 -v
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

WINDOW_SIZE = 8
TIMEOUT = 0.1       # seconds before retransmitting the window
MAX_RETRIES = 50    # consecutive timeouts without progress before giving up


def send_file(sock, dest, packets, window_size, timeout, max_retries, verbose):
    """Send every packet with Go-Back-N. Returns (retransmissions, timeouts)."""
    total = len(packets)
    base = 0            # oldest unACKed seq
    nextseqnum = 0      # next seq to send
    timer = None        # deadline for the oldest unACKed packet, None = stopped
    retransmissions = 0
    timeouts = 0
    retries = 0         # consecutive timeouts with no new ACK

    while base < total:
        # Fill the window
        while nextseqnum < base + window_size and nextseqnum < total:
            sock.sendto(packets[nextseqnum], dest)
            if verbose:
                print(f"send {describe(parse_packet(packets[nextseqnum]))}")
            if base == nextseqnum:
                timer = time.monotonic() + timeout
            nextseqnum += 1

        # Timeout: go back N
        remaining = timer - time.monotonic()
        if remaining <= 0:
            timeouts += 1
            retries += 1
            if retries > max_retries:
                raise TimeoutError(f"no progress at base {base} after {max_retries} retries")
            if verbose:
                print(f"TIMEOUT -> resend {base}..{nextseqnum - 1}")
            for seq in range(base, nextseqnum):
                sock.sendto(packets[seq], dest)
                retransmissions += 1
            timer = time.monotonic() + timeout
            continue

        # Wait for an ACK until the timer expires
        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            pkt = parse_packet(data)
        except socket.timeout:
            continue
        except ConnectionResetError:
            # Windows reports an ICMP "port unreachable" here; treat as no reply
            continue
        except PacketError:
            continue
        if addr != dest or pkt.type != ACK:
            continue

        # Cumulative ACK: slide the window past everything it confirms
        if base <= pkt.ack < nextseqnum:
            if verbose:
                print(f"  recv {describe(pkt)} -> base {pkt.ack + 1}")
            base = pkt.ack + 1
            retries = 0
            timer = None if base == nextseqnum else time.monotonic() + timeout
        elif verbose:
            print(f"  recv {describe(pkt)} (duplicate, ignored)")

    return retransmissions, timeouts


def main():
    parser = argparse.ArgumentParser(description="Go-Back-N sender over UDP")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", help="file to send")
    source.add_argument("--generate", metavar="SIZE",
                        help="send random content of SIZE, e.g. 10KB, 5MB")
    parser.add_argument("--host", default=SERVER_HOST)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--window", type=int, default=WINDOW_SIZE)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    parser.add_argument("--payload", type=int, default=MAX_PAYLOAD,
                        help="data bytes per packet")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log every packet (slow for large files)")
    args = parser.parse_args()
    if args.window < 1:
        parser.error("--window must be at least 1")

    name, content = load_content(args.file, args.generate)
    packets = build_packets(name, content, args.payload)
    dest = (socket.gethostbyname(args.host), args.port)

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"Go-Back-N (window {args.window}): sending {name} ({len(content)} bytes, "
          f"{len(packets)} packets) to {dest[0]}:{dest[1]}")

    try:
        start = time.perf_counter()
        retransmissions, timeouts = send_file(sock, dest, packets, args.window,
                                              args.timeout, args.max_retries,
                                              args.verbose)
        elapsed = time.perf_counter() - start
        print_summary(name, len(content), len(packets), retransmissions,
                      timeouts, elapsed)
    except TimeoutError as e:
        print(f"Transfer failed: {e}. Is the receiver running?")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
