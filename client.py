from google.protobuf.internal.decoder import _DecodeVarint32
from socketserver import ThreadingTCPServer, BaseRequestHandler
from ClusterCommand_pb2 import ClusterCommand
from pathlib import Path
from sys import argv


class Handler(BaseRequestHandler):
    def handle(self):
        while True:
            buf = self.request.recv(1024)
            self.log_file.write(buf)
            self.log_file.flush()
            n = 0
            while n < len(buf):
                msg_len, new_pos = _DecodeVarint32(buf, n)
                n = new_pos
                if (len(buf)) < n + msg_len:
                    buf += self.request.recv(n + msg_len - len(buf))
                msg_buf = buf[n:n + msg_len]
                n += msg_len
                cmd = ClusterCommand()
                cmd.ParseFromString(cmd)
                self.log_file.write(cmd.data)
                self.log_file.flush()


if __name__ == "__main__":
    print("Starting client...")
    hostname = argv[-1]
    here = Path(__file__).parent.resolve()
    nodes = here / "active_nodes"
    nodes.mkdir(parents=True, exist_ok=True)
    server = ThreadingTCPServer(('', 0), Handler, False)
    server.server_bind()
    port = server.socket.getsockname()[1]
    with (nodes / f"{hostname}.node").open('w') as f:
        f.write(str(port))
    Handler.log_file = (nodes / f"{hostname}.node.log").open('w')
    server.server_activate()
    server.serve_forever()
