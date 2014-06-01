import asyncore
import Queue
import json
import re
import time

class DARNMessage:
	def __init__(self, type, expire):
		self.type = type
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
	def __init__(self, host, port):
		self.socket = DARNSocket(self, host, port)
		self.msgqueue = Queue.Queue(0)

	def has_socket(self):
		return (self.socket is not None)

	def receive_msg(self, msg):
		data = json.loads(msg)
		if data[0] == "ping":
			response = DARNMessagePing(data[1], None)
		elif data[0] == "pong":
			response = DARNMessagePong(data[1], None)
		else:
			fuck()
		doe_iets_met(response)

	def send(self, message):
		self.msgqueue.put_nowait(message)

class DARNSocket(asyncore.dispatcher):
	def __init__(self, dn, host, port):
		asyncore.dispatcher.__init__(self)
		self.dn = dn
		self.outbuf = ''
		self.inbuf = ''
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect((host, port))

	def handle_connect(self):
		pass

	def handle_close(self):
		self.close()

	def handle_read(self):
		self.inbuf += self.recv(8192)
		m = r"^(\d+):".match(self.inbuf)
		if m:
			datalen = len(m.group(0)) + int(m.group(1)) + 1
			if len(self.inbuf) >= datalen:
				self.dh.receive_msg(self.inbuf[len(m.group(0)):int(m.group(1))])
				self.inbuf = self.inbuf[datalen:]

	def writable(self):
		if len(self.outbuf) > 0:
			return True
		return (not self.dh.msgqueue.empty())

	def handle_write(self):
		if len(self.outbuf) == 0:
			msg = self.dh.msgqueue.get_nowait()
			str = msg.toString()
			self.outbuf = "%d:%s\n" % (len(str), str)
		sent = self.send(self.outbuf)
		self.outbuf = self.outbuf[sent:]

class DARNetworking:
	def __init__(self):
		self.timers = []

	def addTimer(self, stamp, what):
		self.timers.append((stamp, what))

	def getFirstTimer(self):
		if len(self.timers) == 0:
			return None
		first = (0, self.timers[0][0], self.timers[0][1])
		for (idx, (stamp, what)) in enumerate(self.timers):
			if stamp > first[1]:
				first = (idx, stamp, what)
		return first

	def run():
		while True:
			now = time.time()
			first = self.getFirstTimer()
			if first:
				if first[1] >= now:
					first[2]()
					del self.timers[first[0]]
					continue
				timeout = first - now
			else:
				timeout = None
			asyncore.loop(timeout)
