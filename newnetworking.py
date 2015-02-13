import asyncore
import time
import sys
import socket

packetheaderre = r"^(\d+):"
outstandingRequests = {}

class DARNPeerBase:
	def __init__(self):
		self.reqid = 0

	def sendRPC(self, args, successCallback, failCallback, timeout):
		self.reqid += 1
		args.append(self.reqid)
		self.connection.sendMessage(args)
		# XXX zet timer op timeout voor failCallback
		outstandingRequests[self.reqid] = (successCallback, failCallback)

class DARNConnectionManager:
	connections = {}

	@staticmethod
	def set(host, port, connection):
		if host not in connections:
			connections[host] = {}
		connections[host][port] = connection

	@staticmethod
	def get(host, port):
		if host not in connections:
			connections[host] = {}
		if port not in connections:
			connections[host][port] = DARNPeerConnection(host, port)
		return connections[host][port]

class DARNPeer(DARNPeerBase):
	def __init__(self, host, port):
		super(DARNPeerPeer, self).__init__(self)
		self.connection = DARNConnectionManager.get(host, port)
		self.connection.subscribe(self)
		self.connection.connect()

	def onCommand(sel, cmd, *args):
		return False

class DARNPeerPeer(DARNPeerBase):
	def __init__(self, host, port):
		super(DARNPeerPeer, self).__init__(self)
		self.connection = DARNConnectionManager.get(host, port)
		self.connection.subscribe(self)
		self.connection.connect()

	def onCommand(sel, cmd, *args):
		return False

class DARNPeerConnection:
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.socket = None
		self.msgqueue = []
		self.subscribers = set()

	def subscribe(self, sub):
		self.subscribers.add(sub)

	def unsubscribe(self, sub):
		self.subscribers.remove(sub)

	def connect():
		self.socket = DARNSocket(self)
		self.socket.connect(self.host, self.port)

	def setSocket(self, sock):
		self.socket = self

	def _onConnect(self):
		self.onConnect()

	def _onDisconnect(self):
		self.socket = None
		self.onDisconnect()

	def _onConnectError(self, exctype, value):
		self.socket = None
		self.onConnectError()

	def onConnect(self):
		for sub in self.subscribers:
			sub.onConnect()

	def onDisconnect(self):
		for sub in self.subscribers:
			sub.onDisconnect()

	def onConnectError(self, exctype, value):
		for sub in self.subscribers:
			sub.onConnectError(exctype, value)

	def onMessage(self, raw):
		data = json.loads(raw)
		self.onCommand(self, data[0], data[1:])

	def onCommand(self, cmd, *args):
		if cmd == 'reply':
			id = args.pop()
			if id not in outstandingRequests:
				# XXX warning over dat we deze negeren
				return
			outstandingRequests[id][0](args)
			del outstandingRequests[id]
			# XXX remove timer for id
			return
		for sub in self.subscribers:
			if sub.onCommand(cmd, *args):
				return
		self.sendPacket('error', 'invalid-command')

	def sendMessage(self, *args):
		self.sendPacket(json.dumps(args))

	def sendPacket(self, data):
		self.msgqueue.append(data)

	def getNextPacket(self, peek=False):
		if len(self.msgqueue) == 0:
			return None
		if peek:
			return self.msgqueue[0]
		else:
			return self.msgqueue.pop(0)

	def hasNextPacket(self):
		return len(self.msgqueue) > 0

class DARNAnonymousConnection:
	def __init__(self, sock):
		self.socket = DARNSocket(self, sock)

	def _onDisconnect(self):
		self.socket = None

	def onMessage(self, raw):
		data = json.loads(raw)
		self.onCommand(self, data[0], data[1:])

	def onCommand(self, cmd, args):
		if cmd == 'identify':
			connection = DARNConnectionManager.get(args[0], args[1])
			self.socket.setConnection(connection)
			return
		self.sendPacket('error', 'invalid-command')

	def hasNextPacket(self):
		return False

class DARNSocket(asyncore.dispatcher):
	def __init__(self, connection, *args):
		asyncore.dispatcher.__init__(self, *args)
		self.connection = connection
		self.outbuf = ''
		self.inbuf = ''

	def setConnection(self, connection):
		self.connection = connection

	def connect(self, host, port):
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.handle_error = self.handle_connect_error
		asyncore.dispatcher.connect(self, (host, port))

	def handle_connect(self):
		self.connection._onConnect()
		self.handle_error = asyncore.dispatcher.handle_error

	def handle_connect_error(self):
		exctype, value = sys.exc_info()[:2]
		self.connection._onConnectError(exctype, value)

	def handle_close(self):
		self.connection._onDisconnect()
		self.close()

	def handle_read(self):
		self.inbuf += self.recv(8192)
		m = re.match(packetheaderre, self.inbuf)
		while m:
			datalen = len(m.group(0)) + int(m.group(1)) + 1
			if len(self.inbuf) >= datalen:
				self.connection.onMessage(self.inbuf[len(m.group(0)):datalen-1])
				self.inbuf = self.inbuf[datalen:]
			else:
				break
			m = re.match(packetheaderre, self.inbuf)
		if re.match(r"^\D", self.inbuf):
			self.close()

	def writable(self):
		if len(self.outbuf) > 0:
			return True
		return self.connection.hasNextPacket()

	def handle_write(self):
		if len(self.outbuf) == 0:
			msg = self.connection.getNextPacket()
			if msg is None:
				return
			str = json.dumps(msg)
			self.outbuf = "%d:%s\n" % (len(str), str)
		sent = self.send(self.outbuf)
		self.outbuf = self.outbuf[sent:]

class DARNServerSocket(asyncore.dispatcher):
	def __init__(self, host, port):
		asyncore.dispatcher.__init__(self)
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((host, port))
		self.listen(5)

	def handle_accept(self):
		pair = self.accept()
		if pair is not None:
			sock, addr = pair
			print 'Incoming connection from %s' % repr(addr)
			DARNAnonymousConnection(connection, sock)

class DARNetworking:
	def __init__(self):
		self.timers = []
		self.timersSorted = True
		self.timerId = 0

	def create_server_socket(self, host, port):
		self.server = DARNServerSocket(host, port)

	def add_timer(self, stamp, what):
		self.timerId += 1
		self.timers.append((time.time() + stamp, what, self.timerId))
		self.timersSorted = False
		return self.timerId

	def get_first_timer(self):
		if len(self.timers) == 0:
			return None
		if not self.timersSorted:
			self.timers.sort(key=lambda x: x[0])
			self.timersSorted = True
		return self.timers[0]

	def run(self):
		while True:
			now = time.time()
			first = self.get_first_timer()
			if first:
				stamp, what, id = first
				if stamp <= now:
					what()
					self.timers.pop(0)
					continue
				timeout = stamp - now
			else:
				timeout = None
			if timeout < 0:
				timeout = 0
			asyncore.loop(timeout=timeout, count=1)
