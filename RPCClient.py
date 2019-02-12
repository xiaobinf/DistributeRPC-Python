# encoding:utf-8
import json
import time
import struct
import socket
import random
from kazoo.client import KazooClient

import Request_pb2

zk_root = "/demo"
G = {"servers":None} #全局server

class RemoteServer(object):

    def __init__(self,addr):
        self.addr = addr
        self._socket = None

    @property
    def socket(self):
        if not self._socket:
            print "self.connect() ..."
            self.connect()
        return self._socket

    def ping(self,twitter):
        print "server.ping...",twitter
        return self.rpc("ping", twitter)

    def pi(self,n):
        print "server.pi() ... "
        return self.rpc("pi", n)

    def rpc(self,in_,params):
        sock = self.socket
        print "sock..."
        request = Request_pb2.Request()
        print "in_:",in_,"params:",params
        request._in = in_
        request.params = str(params)
        print "源数据",request._in,request.params
        data = request.SerializeToString()
        print "parse to data:",data,"saiopop"
#        request = json.dumps({"in": in_, "params": params})
        length_prefix = struct.pack("I", len(data))
        sock.send(length_prefix)
        sock.sendall(data)
        length_prefix = sock.recv(4)
        length, = struct.unpack("I", length_prefix)
        body = sock.recv(length)
        print "body:",body,"kkkkkk"
        resp = Request_pb2.Request()
        resp.ParseFromString(body)

  #      response = json.loads(body)
#        return response["out"], response["result"]
        print "parse attributes:", resp._in,resp.params
        return resp._in,resp.params

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print "self.addr in connect():",self.addr
        host, port = self.addr.split(":")
        print "connect() ",host,port
        sock.connect((host, int(port)))
        print sock
        self._socket = sock

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        if self._socket.close():
            self._socket.close()
            self._socket = None

# 获取server列表
def get_servers():
    zk = KazooClient(hosts="95.163.193.71:3181")
    zk.start()
    current_addrs = set()  # 当前活跃地址列表

    def watch_servers(*args):
        new_addrs = set()
        for child in zk.get_children(zk_root,watch = watch_servers):
            node = zk.get(zk_root+"/"+child)
            addr = json.load(node[0])
            new_addrs.add("%s:%d"%(addr["host"],addr["port"]))


        add_addrs = new_addrs - current_addrs
        del_addrs = current_addrs - new_addrs
        
        del_servers = []
        #依次删除服务器中的地址
        for addr in del_addrs:
            for s in G["servers"]:
                if s.addr == addr:
                    del_servers.append(s)
                    break
        for s in del_servers:
            G["servers"].remove(s)
            current_addrs.remove(s.addr)
        for addr in add_addrs:
            G["servers"].append(RemoteServer(addr))
            current_addrs.add(addr)
        
    # 首次获取节点列表并持续监听服务列表变更
    for child in zk.get_children(zk_root, watch=watch_servers):
        node = zk.get(zk_root + "/" + child)
        addr = json.loads(node[0])
        current_addrs.add("%s:%d" % (addr["host"], addr["port"]))
    G["servers"] = [RemoteServer(s) for s in current_addrs]
    return G["servers"]


# 随机获取server
def random_server(): 
    if G["servers"] is None:
        get_servers()  # 首次初始化服务列表
    if not G["servers"]:
        return
    return random.choice(G["servers"])

if __name__ == "__main__":
    for i in range(100):
        server = random_server()
        if not server:
            break  # 如果没有节点存活，就退出
        time.sleep(0.5)
        try:
            print "reader 1..."
            out, result = server.ping("ireader %d" % i)
            print "reader 2..."
            print "打印服务端的地址，以及服务的返回值"
            print server.addr, out, result
        except Exception, ex:
            print "reader Exception..."
            server.close()  # 遇到错误，关闭连接
            print ex
        server = random_server()
        if not server:
            break  # 如果没有节点存活，就退出
        time.sleep(0.5)
        try:
            print "PI 1..."
            out, result = server.pi(100000)
            print "reader 2..."
            print "打印服务端的地址，以及服务的返回值"
            print server.addr, out, result
        except Exception, ex:
            print "reader Exception ..."
            server.close()  # 遇到错误，关闭连接
            print ex
