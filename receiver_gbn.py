"""
receiver_gbn.py - Go-Back-N receiver (CP372 Assignment 2).

Accepts only the expected packet. Anything else (duplicate or out of
order) is discarded and the last in-order packet is re-ACKed, so every
ACK is cumulative. Runs until Ctrl+C.

Example: expected = 3, receive 4 -> discard 4, send ACK 2.

Usage:
  python receiver_gbn.py
  python receiver_gbn.py --out received -v
"""

import argparse
import socket

from rdt_common import (ACK, BUFFER_SIZE, START, FileWriter, PacketError,
                        describe, make_ack, parse_packet)

# Receiver configuration
HOST = "127.0.0.1"
PORT = 12321
OUT_DIR = "received_gbn"


def main():
    parser = argparse.ArgumentParser(description="Go-Back-N receiver over UDP")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--out", default=OUT_DIR, help="folder for received files")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log every packet (slow for large files)")
    args = parser.parse_args()

    # Create UDP socket and bind to address and port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    print(f"Go-Back-N receiver running on {args.host}:{args.port}")

    writer = FileWriter(args.out)
    sender = None      # address of the current sender
    expected = 0       # expectedseqnum
    done = True        # no transfer in progress

    try:
        while True:
            data, addr = sock.recvfrom(BUFFER_SIZE)

            # Part B: simulated packet loss goes here (drop before processing)

            try:
                pkt = parse_packet(data)
            except PacketError as e:
                if args.verbose:
                    print(f"drop malformed packet: {e}")
                continue
            if pkt.type == ACK:
                continue

            # START seq 0 from a new sender (or after a finished transfer) begins a new session
            if pkt.type == START and pkt.seq == 0 and (done or addr != sender):
                writer.abort()
                sender, expected, done = addr, 0, False
            elif addr != sender:
                continue

            if pkt.seq == expected:
                try:
                    finished = writer.handle(pkt)
                except (PacketError, ValueError, OSError) as e:
                    print(f"Bad START packet: {e}")
                    continue  # no ACK, sender gives up after retries
                if args.verbose:
                    print(f"recv {describe(pkt)} -> ACK {pkt.seq}")
                sock.sendto(make_ack(pkt.seq), addr)
                expected += 1
                if finished:
                    done = True
            elif expected > 0:
                # Duplicate or out of order: discard, re-send cumulative ACK
                if args.verbose:
                    print(f"recv {describe(pkt)} discarded (expected {expected}) "
                          f"-> ACK {expected - 1}")
                sock.sendto(make_ack(expected - 1), addr)
    except KeyboardInterrupt:
        print("\nReceiver stopped")
    finally:
        writer.abort()
        sock.close()


if __name__ == "__main__":
    main()
