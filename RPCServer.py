# encoding:utf-8
import os
import sys
import math
import json
import errno
import struct
import signal
import socket
import asyncore
from cStringIO import StringIO
from kazoo.client import KazooClient

import Request_pb2

class RPCServer(asyncore.dispatcher):
    zk_root = "/demo"
    zk_rpc = zk_root + "/rpc"
    zk_rpc1 = zk_root + "/rpc1"

    def __init__(self,host,port):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host,port))
        self.listen(1)
        self.child_pids = []
        if self.prefork(3):
            self.register_zk() # 注册服务
            self.register_parent_signal()  # 父进程善后处理
        else:
            self.register_child_signal() # 子进程善后处理
        for pid in self.child_pids:
            print pid
    
    # 开启n个子进程
    def prefork(self,n):
        for i in range(n):
            pid = os.fork()
            if pid < 0:
                raise
            if pid > 0: # 父进程
                self.child_pids.append(pid) # 记录子进程的pid
                continue
            if pid == 0: # 子进程
                return False
        return True

    def register_zk(self):
        print "start register zk..."
        self.zk = KazooClient(hosts = "95.163.193.71:3181")
        self.zk.start()
        self.zk.ensure_path(self.zk_root)
        value = json.dumps({"host":self.host,"port":self.port})
        # 创建服务子节点
        self.zk.create(self.zk_rpc,value,ephemeral=True, sequence=True)

        value_1 = json.dumps({"host":"95.163.193.71","port":8082})
        # 创建第二个服务子节点
       # self.zk.create(self.zk_rpc1,value_1,ephemeral=True, sequence=True)
        print "zk localhost 3181"

    def exit_parent(self,sig,frame):
        print "exit_parent"
    
    def reap_child(self,sig,frame):
        print "before reap_child"
        while True:
           try:
              info = os.waitpid(-1,os.WNOHANG)
              break
           except OSError,ex:
              if ex.args[0] == errno.ECHILD:
                 return # 无子进程收割
              if ex.args[0] != errno.EINTR:
                 raise ex
        pid = info[0]
        try:
           self.child_pids.remove(pid)
        except ValueError:
           pass
        print "reap_child pid:",pid
    
    def exit_child(self,sig,frame):
        self.close() # 关闭serversocket
        asyncore.close_all() # 关闭所有clientsocket
        print "exit_child all_closed"

    def register_parent_signal(self):
        signal.signal(signal.SIGINT,self.exit_parent)
        signal.signal(signal.SIGTERM,self.exit_parent)
        signal.signal(signal.SIGCHLD,self.reap_child)  # -9 -2

    def register_child_signal(self):
        signal.signal(signal.SIGINT, self.exit_child)
        signal.signal(signal.SIGTERM, self.exit_child)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock,addr = pair
            print sock,pair
            print "handle_accept"
            RPCHandler(sock,addr)


class RPCHandler(asyncore.dispatcher_with_send):
    def __init__(self,sock,addr):
        asyncore.dispatcher_with_send.__init__(self,sock=sock)
        self.addr = addr
        self.handlers = {"ping":self.ping,"pi":self.pi}
        self.rbuf = StringIO()
        print "RPCHandler __init__"

    def ping(self,params):
        print "服务端 RPCHandler() ping"
        self.send_result("RPCHandler()pong",params)

    def pi(self,n):
        print "客户端 RPCHandler() pi"
        s = 0.0
        for i in range(int(n)+1):
            s += math.pow(-1,i)/(2*i+1)
        p = 4*s
        self.send_result("pi_server_return:",p)
   
    def send_result(self,out,result):
#        response = {"out":out,"result":result}
        response = Request_pb2.Request()
        response._in = out
        response.params = str(result)
#        body = json.dumps(response)
        body = response.SerializeToString()
        length_prefix = struct.pack("I",len(body))
        self.send(length_prefix)
        self.send(body)


    def handle_connect(self):
        print self.addr,'handle_connect() client comes...'
    
    def handle_close(self):
        print self.addr,'handle_close() bye...'

    def handle_read(self):
        while True:
            content = self.recv(1024)
            print "handle_read() content...",content
            if content:
                self.rbuf.write(content)
            if len(content)<1024:
                print "读取完毕"
                break
        self.handle_rpc()

    def handle_rpc(self):
        while True:
            self.rbuf.seek(0)
            length_prefix = self.rbuf.read(4)
            if len(length_prefix) < 4:
                break
            length,=struct.unpack("I",length_prefix)
            body = self.rbuf.read(length)
            if len(body) < length:
                break
#            request = json.loads(body)
            request = Request_pb2.Request()
            request.ParseFromString(body)
#            in_ = request['in']
#            params = request['params']
            in_ = request._in
            params = request.params
            print os.getpid(), in_, params
            handler = self.handlers[in_]
            handler(str(params))
            left = self.rbuf.getvalue()[length + 4:]
            print left
            self.rbuf = StringIO()
            self.rbuf.write(left)
        self.rbuf.seek(0, 2)            




if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    print host,port
    RPCServer(host,port)
    asyncore.loop()
