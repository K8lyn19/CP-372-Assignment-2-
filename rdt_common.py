"""
rdt_common.py - Packet format and file helpers shared by the Stop-and-Wait
and Go-Back-N programs (CP372 Assignment 2).

Packet = 11-byte header + payload. Header fields (network byte order):
  type    1 B  START, DATA, END or ACK
  seq     4 B  sequence number of this packet
  ack     4 B  ACK number (used by ACK packets, 0 otherwise)
  length  2 B  payload length in bytes

Payload by type:
  START  "<filename>|<filesize>" in UTF-8 (upload a file)
  DATA   raw file bytes, at most MAX_PAYLOAD
  END    empty (completion of file transfer)
  ACK    empty

A transfer is START (seq 0), DATA (seq 1..n), END (seq n+1), so every
packet, including the control ones, is delivered reliably and in order.
"""

import os
import struct
from collections import namedtuple

HEADER = struct.Struct("!BIIH")
HEADER_SIZE = HEADER.size
MAX_PAYLOAD = 1024
BUFFER_SIZE = 65535  # recvfrom size, larger than any packet

# Packet types
START = 1
DATA = 2
END = 3
ACK = 4
TYPE_NAMES = {START: "START", DATA: "DATA", END: "END", ACK: "ACK"}

Packet = namedtuple("Packet", "type seq ack payload")


class PacketError(Exception):
    """Malformed packet. Receivers drop these."""


# Encoding / decoding

def make_packet(ptype, seq, ack=0, payload=b""):
    if len(payload) > 0xFFFF:
        raise ValueError("payload too large for 16-bit length field")
    return HEADER.pack(ptype, seq, ack, len(payload)) + payload


def make_ack(ack_num):
    return make_packet(ACK, 0, ack_num)


def parse_packet(data):
    if len(data) < HEADER_SIZE:
        raise PacketError(f"packet too short ({len(data)} bytes)")
    ptype, seq, ack, length = HEADER.unpack_from(data)
    payload = data[HEADER_SIZE:]
    if ptype not in TYPE_NAMES:
        raise PacketError(f"unknown type {ptype}")
    if len(payload) != length:
        raise PacketError(f"length field {length} != payload {len(payload)}")
    return Packet(ptype, seq, ack, bytes(payload))


def describe(pkt):
    """Short log string, e.g. 'DATA seq=5 (1024 B)' or 'ACK 5'."""
    if pkt.type == ACK:
        return f"ACK {pkt.ack}"
    return f"{TYPE_NAMES[pkt.type]} seq={pkt.seq} ({len(pkt.payload)} B)"


# Sender helpers

def parse_size(text):
    """'10KB' -> 10240, '5MB' -> 5242880, '500' -> 500."""
    text = text.strip().upper().rstrip("B")
    units = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def load_content(path=None, generate=None):
    """Return (filename, bytes) from a file, or random bytes of a given size."""
    if path:
        with open(path, "rb") as f:
            return os.path.basename(path), f.read()
    size = parse_size(generate)
    return f"generated_{generate}.bin", os.urandom(size)


def build_packets(filename, content, payload_size=MAX_PAYLOAD):
    """Split a file into [START, DATA..., END] raw packets, seq 0..n+1."""
    header = f"{filename}|{len(content)}".encode()
    packets = [make_packet(START, 0, payload=header)]
    view = memoryview(content)
    for offset in range(0, len(content), payload_size):
        chunk = view[offset:offset + payload_size]
        packets.append(make_packet(DATA, len(packets), payload=chunk))
    packets.append(make_packet(END, len(packets)))
    return packets


def print_summary(name, size, packets, retransmissions, timeouts, elapsed):
    throughput = size / elapsed if elapsed > 0 else 0
    print(f"Transfer complete: {name}")
    print(f"  File size:       {size} bytes")
    print(f"  Packets:         {packets} (incl. START/END)")
    print(f"  Timeouts:        {timeouts}")
    print(f"  Retransmissions: {retransmissions}")
    print(f"  Transfer time:   {elapsed:.4f} s")
    print(f"  Throughput:      {throughput:.2f} bytes/s")


# Receiver helpers

def safe_filename(name):
    """Strip directories so a sender can't write outside the output folder."""
    name = os.path.basename(name.replace("\\", "/"))
    if name in ("", ".", ".."):
        raise PacketError(f"invalid filename {name!r}")
    return name


class FileWriter:
    """Acts on in-order packets: START creates the file, DATA writes, END closes."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.file = None
        self.path = None
        self.expected_size = 0
        self.received = 0

    def handle(self, pkt):
        """Process one in-order packet. Returns True when the transfer is done."""
        if pkt.type == START:
            self.abort()
            name, size = pkt.payload.decode().rsplit("|", 1)
            os.makedirs(self.out_dir, exist_ok=True)
            self.path = os.path.join(self.out_dir, safe_filename(name))
            self.expected_size = int(size)
            self.received = 0
            self.file = open(self.path, "wb")
            print(f"Receiving {self.path} ({self.expected_size} bytes)")
        elif pkt.type == DATA and self.file:
            self.file.write(pkt.payload)
            self.received += len(pkt.payload)
        elif pkt.type == END and self.file:
            self.file.close()
            self.file = None
            status = "OK" if self.received == self.expected_size else "SIZE MISMATCH"
            print(f"Saved {self.path}: {self.received}/{self.expected_size} bytes [{status}]")
            return True
        return False

    def abort(self):
        """Close and delete a partial file (sender restarted mid-transfer)."""
        if self.file:
            self.file.close()
            self.file = None
            os.remove(self.path)
            print(f"Discarded incomplete file {self.path}")
