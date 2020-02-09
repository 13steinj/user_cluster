import re

from sys import argv
from json import dumps
from pathlib import Path
from os import getcwd, chdir
from ClusterCommand_pb2 import ClusterCommand

import subprocess
import socket

from sys import stderr
from time import sleep
from threading import Thread
from queue import Queue

from google.protobuf.internal.encoder import _VarintBytes
from google.protobuf.internal.decoder import _DecodeVarint32

import getpass

ip_re = "((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))"



class Node(object):
    def __init__(self, hostname, ip, port):
        self.hostname, self.ip, self.port = hostname, ip, port
        self.command_queue = Queue()
        self.thread = Thread(group=None, target=self.activate)
        self.thread.daemon = True
        self.thread.start()

    def activate(self):
        retry = None
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
                conn.connect((self.hostname, self.port))
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 1)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
                while True:
                    if retry is None:
                        cmd = self.command_queue.get()
                        to_send = ClusterCommand()
                        to_send.type, to_send.data = cmd
                    else:
                        to_send = retry
                    size = to_send.ByteSize()
                    try:
                        conn.send(_VarintBytes(size) + to_send.SerializeToString())
                        retry = None
                    except BrokenPipeError:
                        retry = to_send
                        break

class Cluster(dict):
    def __init__(self, *args, hostname=None, **kwargs):
        super(Cluster, self).__init__(*args, **kwargs)
        self.active_nodes = []
        self.data_dir = Path(__file__).parent.resolve()
        self.hostname = hostname
        self.scheduled = 0

    @classmethod
    def find_cluster(cls, hostname_regex):
        regexp = re.compile(f"Nmap scan report for ({hostname_regex}) \({ip_re}\)")
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        IPAddr = IPAddr.rsplit(".",1)[0] + ".0/24"
        print("Finding available nodes...")
        ran = subprocess.run(
            f"nmap -p 22 {IPAddr}".split(),
            stdout=subprocess.PIPE)
        return cls({host: ip for host, ip in regexp.findall(ran.stdout.decode())}, hostname=hostname)

    def initiate(self):
        client = self.data_dir / "client.py"

        for host in self:
            subprocess.run(f"ssh -f {host} python3 {client} {host}".split())
        sleep(1)
        for host, ip in self.items():
            with (self.data_dir / "active_nodes" / f"{host}.node").open() as f:
                port = int(f.read())
                self.active_nodes.append(Node(host, ip, port))
        self.cd(getcwd())

    def shell(self):
        user = getpass.getuser()
        while True:
            cmd = input(f"{user}@user_cluster({self.hostname}) [nodes={len(self.active_nodes)}]:{getcwd()}$ ")
            if cmd[:2] == "cd":
                self.cd(cmd[2:].strip())
                continue
            if cmd[:4] == "crun":
                self.crun(cmd[4:].strip())
                continue
            subprocess.run(cmd.split())

    def crun(self, cmd):
        on = self.scheduled
        self.scheduled += 1
        self.scheduled %= len(self.active_nodes)
        self.active_nodes[on].command_queue.put(("run", cmd))

    def cd(self, loc):
        print(getcwd())
        print(loc)
        try:
            chdir(loc)
        except FileNotFoundError as e:
            print(e.strerror, file=stderr)
        else:
            print(getcwd())
            print(__file__)
            directory = Path(__file__).parent.resolve()
            print(directory)
            with (self.data_dir / "active_nodes" / "active_directory").open('w') as f:
                f.write(getcwd())
            for node in self.active_nodes:
                node.command_queue.put(("cd", loc))



if __name__ == '__main__':
    found = Cluster.find_cluster(r"remote05\.cs\.binghamton\.edu")
        # input("Please enter a hostname mask (regex): "))
    print("Found nodes, Cluster is:", found)
    found.initiate()
    found.shell()
