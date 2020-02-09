from google.protobuf.internal.decoder import _DecodeVarint32
from socketserver import ThreadingTCPServer, BaseRequestHandler
from ClusterCommand_pb2 import ClusterCommand
from pathlib import Path
from sys import argv
from os import chdir
from subprocess import run


class Handler(BaseRequestHandler):
    def handle(self):
        while True:
            self.log_file.write("handling")
            self.log_file.flush()
#            self.log_file.write("am I stcuK")
#            self.log_file.flush()
            buf = self.request.recv(1024)
#            self.log_file.write(f"made it here {len(buf)}\n")
            self.log_file.flush()
#            self.log_file.write(f"{repr(buf)}\n")
            self.log_file.flush()
            n = 0
            while n < len(buf):
                msg_len, new_pos = _DecodeVarint32(buf, n)
                self.log_file.write(f"{msg_len} {new_pos}\n")
                self.log_file.flush()
                n = new_pos
                if (len(buf)) < n + msg_len:
                    buf += self.request.recv(n + msg_len - len(buf))
                    self.log_file.write(f"{repr(buf)} {n} {msg_len} {len(buf)}\n")
                msg_buf = buf[n:n + msg_len]
                n += msg_len
                cmd = ClusterCommand()
                cmd.ParseFromString(msg_buf)
                getattr(self, f"handle_{cmd.type}")(cmd.data)
#                self.log_file.write("test\n")
                self.log_file.flush()

    def handle_run(self, data):
        self.log_file.write(f"running: {data}\n")
        self.log_file.flush()
        run(data.split())

    def handle_cd(self, loc):
        with (self.data_dir / "active_nodes" / "active_directory").open() as f:
            chdir(f.read())
            self.log_write(loc)
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
    Handler.data_dir = here
    Handler.log_file = (nodes / f"{hostname}.node.log").open('w')
    server.server_activate()
    server.serve_forever()
