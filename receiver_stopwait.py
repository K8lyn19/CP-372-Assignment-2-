"""
receiver_stopwait.py - Stop-and-Wait receiver (CP372 Assignment 2).

Accepts the packet with the expected sequence number and ACKs it.
Duplicates (our ACK was lost) are re-ACKed but not written again.
Out-of-order packets are ignored. Runs until Ctrl+C.

Usage:
  python receiver_stopwait.py
  python receiver_stopwait.py --out received -v
"""

import argparse
import socket

from rdt_common import (ACK, BUFFER_SIZE, START, FileWriter, PacketError,
                        describe, make_ack, parse_packet)

# Receiver configuration
HOST = "127.0.0.1"
PORT = 12321
OUT_DIR = "received_stopwait"


def main():
    parser = argparse.ArgumentParser(description="Stop-and-Wait receiver over UDP")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--out", default=OUT_DIR, help="folder for received files")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log every packet (slow for large files)")
    args = parser.parse_args()

    # Create UDP socket and bind to address and port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    print(f"Stop-and-Wait receiver running on {args.host}:{args.port}")

    writer = FileWriter(args.out)
    sender = None      # address of the current sender
    expected = 0       # next in-order sequence number
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
                if args.verbose:
                    print(f"recv {describe(pkt)} -> ACK {pkt.seq}")
                try:
                    finished = writer.handle(pkt)
                except (PacketError, ValueError, OSError) as e:
                    print(f"Bad START packet: {e}")
                    continue  # no ACK, sender gives up after retries
                sock.sendto(make_ack(pkt.seq), addr)
                expected += 1
                if finished:
                    done = True
            elif pkt.seq < expected:
                # Duplicate: the ACK was lost, so ACK again without writing
                if args.verbose:
                    print(f"recv {describe(pkt)} duplicate -> ACK {pkt.seq}")
                sock.sendto(make_ack(pkt.seq), addr)
            elif args.verbose:
                print(f"recv {describe(pkt)} out of order (expected {expected}), ignored")
    except KeyboardInterrupt:
        print("\nReceiver stopped")
    finally:
        writer.abort()
        sock.close()


if __name__ == "__main__":
    main()
