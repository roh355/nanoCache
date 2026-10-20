#!/usr/bin/env python3
"""Integration checks for NanoCache's threaded TCP server."""

import concurrent.futures
import os
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORT = 9000
SOURCES = [
    "server.cpp",
    "cache.cpp",
    "commands.cpp",
    "resp_parser.cpp",
    "snapshot.cpp",
    "worker_pool.cpp",
]


def encode_command(*parts):
    encoded = [str(part).encode("utf-8") for part in parts]
    return b"*" + str(len(encoded)).encode() + b"\r\n" + b"".join(
        b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n"
        for part in encoded
    )


class RespClient:
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(8)
        self.buffer = bytearray()

    def close(self):
        self.sock.close()

    def send(self, *parts):
        self.sock.sendall(encode_command(*parts))

    def command(self, *parts):
        self.send(*parts)
        return self.read_response()

    def read_line(self):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                line = bytes(self.buffer[:end])
                del self.buffer[: end + 2]
                return line
            data = self.sock.recv(4096)
            if not data:
                raise EOFError("server closed before completing a RESP line")
            self.buffer.extend(data)

    def read_exactly(self, size):
        while len(self.buffer) < size:
            data = self.sock.recv(4096)
            if not data:
                raise EOFError("server closed before completing a RESP value")
            self.buffer.extend(data)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def read_response(self):
        line = self.read_line()
        if not line:
            raise AssertionError("empty RESP response")
        prefix, payload = line[:1], line[1:]
        if prefix == b"+":
            return payload.decode("utf-8")
        if prefix == b"-":
            raise AssertionError("NanoCache returned RESP error: " + payload.decode())
        if prefix == b":":
            return int(payload)
        if prefix == b"$":
            length = int(payload)
            if length == -1:
                return None
            value = self.read_exactly(length + 2)
            if value[-2:] != b"\r\n":
                raise AssertionError("bulk string has no RESP terminator")
            return value[:-2].decode("utf-8")
        raise AssertionError("unknown RESP response prefix: " + repr(prefix))

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


class ThreadedServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build_dir = tempfile.TemporaryDirectory(prefix="nanocache-build-")
        cls.binary = Path(cls.build_dir.name) / "nanocache-test-server"
        compiler = shlex.split(os.environ.get("CXX", "clang++"))
        command = compiler + [
            "-std=c++17",
            "-O1",
            "-Wall",
            "-Wextra",
            "-pedantic",
            "-pthread",
            *[str(ROOT / source) for source in SOURCES],
            "-o",
            str(cls.binary),
        ]
        subprocess.run(command, check=True, cwd=ROOT, capture_output=True, text=True)

    @classmethod
    def tearDownClass(cls):
        cls.build_dir.cleanup()

    def setUp(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", PORT))
        except OSError as exc:
            probe.close()
            self.skipTest(f"TCP port {PORT} is already unavailable: {exc}")
        probe.close()

        self.runtime_dir = tempfile.TemporaryDirectory(prefix="nanocache-run-")
        self.server = None
        self.start_server()

    def tearDown(self):
        self.stop_server()
        self.runtime_dir.cleanup()

    def start_server(self):
        self.server = subprocess.Popen(
            [str(self.binary)],
            cwd=self.runtime_dir.name,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                error = self.server.stderr.read()
                raise AssertionError(f"NanoCache exited during startup: {error}")
            try:
                probe = socket.create_connection(("127.0.0.1", PORT), timeout=0.2)
                probe.close()
                return
            except OSError:
                time.sleep(0.02)
        self.stop_server()
        raise AssertionError("NanoCache did not begin listening")

    def stop_server(self):
        if self.server is None:
            return
        if self.server.poll() is None:
            self.server.send_signal(signal.SIGTERM)
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=2)
                raise AssertionError("NanoCache did not shut down promptly")
        self.server.stderr.close()
        self.server = None

    def connect_client(self):
        return RespClient(socket.create_connection(("127.0.0.1", PORT), timeout=5))

    def test_concurrent_get_set_del_expire_and_multiple_clients(self):
        clients = 12
        barrier = threading.Barrier(clients)
        with self.connect_client() as seed:
            self.assertEqual(seed.command("SET", "shared:set", "initial"), "OK")
            self.assertEqual(seed.command("SET", "shared:delete", "present"), "OK")

        def exercise(index):
            with self.connect_client() as client:
                barrier.wait(timeout=5)
                set_result = client.command("SET", "shared:set", f"value-{index}")
                get_result = client.command("GET", "shared:set")
                expire_result = client.command("EXPIRE", "shared:set", "60")
                delete_result = client.command("DEL", "shared:delete")
                ttl_result = client.command("TTL", "shared:set")
                return set_result, get_result, expire_result, delete_result, ttl_result

        with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as executor:
            results = list(executor.map(exercise, range(clients)))

        for set_result, get_result, expire_result, _delete_result, ttl_result in results:
            self.assertEqual(set_result, "OK")
            self.assertIn(get_result, {f"value-{i}" for i in range(clients)})
            self.assertEqual(expire_result, 1)
            self.assertTrue(ttl_result == -1 or 0 <= ttl_result <= 60)
        self.assertEqual(sum(result[3] for result in results), 1)

    def test_concurrent_incr_ordering_and_high_request_volume(self):
        clients = 16
        increments_per_client = 200
        total = clients * increments_per_client
        barrier = threading.Barrier(clients)
        with self.connect_client() as client:
            self.assertEqual(client.command("SET", "counter", "0"), "OK")

        def increment_many(_index):
            with self.connect_client() as client:
                barrier.wait(timeout=5)
                payload = encode_command("INCR", "counter") * increments_per_client
                client.sock.sendall(payload)
                return [client.read_response() for _ in range(increments_per_client)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as executor:
            responses = list(executor.map(increment_many, range(clients)))

        flattened = [value for client_values in responses for value in client_values]
        self.assertEqual(len(flattened), total)
        self.assertEqual(sorted(flattened), list(range(1, total + 1)))
        with self.connect_client() as client:
            self.assertEqual(client.command("GET", "counter"), str(total))

    def test_fragmented_request_and_pipelined_request_order(self):
        with self.connect_client() as client:
            request = encode_command("SET", "ordered", "10")
            client.sock.sendall(request[:4])
            client.sock.settimeout(0.1)
            with self.assertRaises(socket.timeout):
                client.sock.recv(1)
            client.sock.settimeout(5)
            client.sock.sendall(request[4:])
            self.assertEqual(client.read_response(), "OK")

            pipeline = (
                encode_command("SET", "ordered", "20")
                + encode_command("GET", "ordered")
                + encode_command("PING")
            )
            client.sock.sendall(pipeline)
            self.assertEqual(client.read_response(), "OK")
            self.assertEqual(client.read_response(), "20")
            self.assertEqual(client.read_response(), "PONG")

    def test_disconnect_during_inflight_command(self):
        with self.connect_client() as client:
            self.assertEqual(client.command("SET", "disconnect-counter", "0"), "OK")

        disconnected = socket.create_connection(("127.0.0.1", PORT), timeout=2)
        disconnected.sendall(encode_command("INCR", "disconnect-counter"))
        disconnected.close()

        deadline = time.monotonic() + 3
        observed = None
        while time.monotonic() < deadline:
            with self.connect_client() as client:
                observed = client.command("GET", "disconnect-counter")
            if observed == "1":
                break
            time.sleep(0.02)
        self.assertEqual(observed, "1")

    def test_snapshot_is_consistent_during_shutdown_traffic(self):
        with self.connect_client() as client:
            self.assertEqual(client.command("SET", "snapshot-anchor", "saved"), "OK")
            self.assertEqual(client.command("SET", "snapshot-counter", "0"), "OK")

        client_count = 8
        commands_per_client = 1000
        barrier = threading.Barrier(client_count + 1)
        release_clients = threading.Event()
        sent_count = 0
        sent_lock = threading.Lock()

        def flood():
            nonlocal sent_count
            sock = socket.create_connection(("127.0.0.1", PORT), timeout=5)
            try:
                barrier.wait(timeout=5)
                sock.sendall(encode_command("INCR", "snapshot-counter") * commands_per_client)
                with sent_lock:
                    sent_count += 1
                release_clients.wait(timeout=5)
            finally:
                sock.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=client_count) as executor:
            futures = [executor.submit(flood) for _ in range(client_count)]
            barrier.wait(timeout=5)
            deadline = time.monotonic() + 5
            while sent_count < client_count and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertEqual(sent_count, client_count)
            time.sleep(0.01)

            try:
                self.server.send_signal(signal.SIGTERM)
                return_code = self.server.wait(timeout=5)
                self.server.stderr.close()
                self.server = None
            finally:
                release_clients.set()
            for future in futures:
                future.result(timeout=5)

        self.assertEqual(return_code, 0)
        snapshot_path = Path(self.runtime_dir.name) / "nanocache.snapshot"
        self.assertTrue(snapshot_path.exists())
        self.assertGreater(snapshot_path.stat().st_size, 16)

        self.start_server()
        with self.connect_client() as client:
            self.assertEqual(client.command("GET", "snapshot-anchor"), "saved")
            value = client.command("GET", "snapshot-counter")
            self.assertIsNotNone(value)
            self.assertGreaterEqual(int(value), 0)
            self.assertLessEqual(int(value), client_count * commands_per_client)

    def test_shutdown_wakes_idle_workers(self):
        started = time.monotonic()
        self.server.send_signal(signal.SIGTERM)
        return_code = self.server.wait(timeout=3)
        elapsed = time.monotonic() - started
        self.server.stderr.close()
        self.server = None

        self.assertEqual(return_code, 0)
        self.assertLess(elapsed, 3)
        self.assertTrue((Path(self.runtime_dir.name) / "nanocache.snapshot").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
