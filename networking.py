import asyncore
import Queue
import json
import re
import time
import socket

class DARNMessage:
	def __init__(self, type, expire):
		self.type = type
		# XXX: expire naleven
		self.expire = expire

	def toString(self):
		return json.dumps([self.type, self.getPayload()])

	def getPayload(self):
		return None

class DARNMessagePing(DARNMessage):
	def __init__(self, configversion, expiry):
		DARNMessage.__init__(self, "ping", expiry)
		self.configversion = configversion

	def getPayload(self):
		return [self.configversion]

class DARNMessagePong(DARNMessage):
	def __init__(self, expiry):
		DARNMessage.__init__(self, "pong", expiry)

class DARNHost:
	def __init__(self, callback):
		self.socket = None
		self.msgqueue = Queue.Queue(0)
		self.callback = callback

	def setSocket(self, sock):
		self.socket = sock

	def connect(self, host, port):
		sock = DARNSocket(self)
		sock.connect(host, port)
		self.setSocket(sock)

	def has_socket(self):
		return (self.socket is not None)

	def receive_msg(self, msg):
		data = json.loads(msg)
		self.callback(self, data)

	def send(self, message):
		self.msgqueue.put_nowait(message)

class DARNSocket(asyncore.dispatcher):
	def __init__(self, manager, *args):
		asyncore.dispatcher.__init__(self, *args)
		self.manager = manager
		self.outbuf = ''
		self.inbuf = ''

	def connect(self, host, port):
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect((host, port))

	def handle_connect(self):
		pass

	def handle_close(self):
		self.close()

	def handle_read(self):
		self.inbuf += self.recv(8192)
		m = re.match(r"^(\d+):", self.inbuf)
		if m:
			datalen = len(m.group(0)) + int(m.group(1)) + 1
			if len(self.inbuf) >= datalen:
				self.manager.receive_msg(self.inbuf[len(m.group(0)):datalen-1])
				self.inbuf = self.inbuf[datalen:]

	def writable(self):
		if len(self.outbuf) > 0:
			return True
		return (not self.manager.msgqueue.empty())

	def handle_write(self):
		if len(self.outbuf) == 0:
			msg = self.manager.msgqueue.get_nowait()
			str = msg.toString()
			self.outbuf = "%d:%s\n" % (len(str), str)
		sent = self.send(self.outbuf)
		self.outbuf = self.outbuf[sent:]

class DARNServerSocket(asyncore.dispatcher):
	def __init__(self, host, port, callback):
		asyncore.dispatcher.__init__(self)
		self.callback = callback
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((host, port))
		self.listen(5)

	def handle_accept(self):
		pair = self.accept()
		if pair is not None:
			sock, addr = pair
			print 'Incoming connection from %s' % repr(addr)
			host = DARNHost(self.callback)
			host.setSocket(DARNSocket(host, sock))

class DARNetworking:
	def __init__(self):
		self.timers = []

	def create_server_socket(self, host, port, callback):
		self.server = DARNServerSocket(host, port, callback)

	def add_timer(self, stamp, what):
		self.timers.append((time.time() + stamp, what))

	def get_first_timer(self):
		if len(self.timers) == 0:
			return None
		first = (0, self.timers[0][0], self.timers[0][1])
		for (idx, (stamp, what)) in enumerate(self.timers):
			if stamp > first[1]:
				first = (idx, stamp, what)
		return first

	def run(self):
		while True:
			now = time.time()
			first = self.get_first_timer()
			if first:
				idx, stamp, what = first
				if stamp >= now:
					what()
					del self.timers[idx]
					continue
				timeout = stamp - now
			else:
				timeout = None
			if timeout < 0: timeout = 0
			asyncore.loop(timeout)
