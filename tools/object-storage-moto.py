"""Start only the S3 HTTP emulator required by the Go contract suite."""
import logging
import signal
import sys
import threading

from moto.server import ThreadedMotoServer

logging.getLogger("werkzeug").setLevel(logging.ERROR)
shutdown = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
signal.signal(signal.SIGINT, lambda *_: shutdown.set())
server = ThreadedMotoServer(ip_address="127.0.0.1", port=int(sys.argv[1]), verbose=False)
server.start()
try:
    shutdown.wait()
finally:
    server.stop()
