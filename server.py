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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
            conn.connect((self.hostname, self.port))
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 1)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            while True:
                cmd = self.command_queue.get()
                print(cmd)
                to_send = ClusterCommand()
                to_send.type, to_send.data = cmd
                size = to_send.ByteSize()
                conn.send(_VarintBytes(size))
                print
                conn.send(to_send.SerializeToString())

class Cluster(dict):
    def __init__(self, *args, hostname=None, **kwargs):
        super(Cluster, self).__init__(*args, **kwargs)
        self.active_nodes = []
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
        directory = Path(__file__).parent.resolve()
        client = directory / "client.py"
        print(client)

        for host in self:
            subprocess.run(f"ssh -f {host} python3 {client} {host}".split())
        sleep(1)
        for host, ip in self.items():
            with (directory / "active_nodes" / f"{host}.node").open() as f:
                port = int(f.read())
                self.active_nodes.append(Node(host, ip, port))

    def shell(self):
        user = getpass.getuser()
        while True:
            cmd = input(f"{user}@user_cluster({self.hostname}) [nodes={len(self.active_nodes)}]:{getcwd()}$ ")
            if cmd[:2] == "cd":
                try:
                    chdir(cmd[2:])
                except FileNotFoundError as e:
                    print(e.strerror, file=stderr)
                continue
            if cmd[:4] == "crun":
                print(cmd)
                self.crun(cmd[4:])
                continue
            subprocess.run(cmd.split())

    def crun(self, cmd):
        on = self.scheduled
        self.scheduled += 1
        self.scheduled %= len(self.active_nodes)
        print(cmd)
        self.active_nodes[on].command_queue.put(("run", cmd))
        print("put on queue")



if __name__ == '__main__':
    found = Cluster.find_cluster()
        # input("Please enter a hostname mask (regex): "))
    print("Found nodes, Cluster is:", found)
    found.initiate()
    found.shell()
