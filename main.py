import uuid
from datetime import datetime
import time
import random
from networking import *
import re

class DARN:
	VERSION = "0.1"

	def log(self, severity, message):
		print "%s: DARN[%s]: %s" % (datetime.now(), severity, message)

	def info(self, message):
		self.log("info", message)

	def debug(self, message):
		self.log("debug", message)

	"""Create a DARN object. Read config from given file. """
	def __init__(self, configfile):
		self.info("Initialising DARN version " + DARN.VERSION)
		self.configfile = configfile
		self.net = DARNetworking()
		self.running = False
		self.node_configs = {}
		self.expected_pongs = set()
		self.error_seq = 1
		self.error_events = []
		self.config_version = None
		self.hosts = {}
		self.reload()

	def split_hostname(self, node):
		m = re.match(r'^(.+?)(?::(\d+))?$', node)
		return (m.group(1), int(m.group(2)))

	def host(self, node):
		if not self.hosts[node]:
			(hostname, port) = self.split_hostname(node)
			self.hosts[node] = host = DARNHost()
			host.connect(hostname, port)
			host.send({
				'hostname': self.config['hostname'],
			})
		return self.hosts[node]

	def data_from_unidentified_host(self, host, data):
		self.debug("DARN Host connected to me: %s and sent: %s" % (host, data))
		host.merge(self.hosts[data['hostname']])

	def stop(self):
		self.info("Stopping")
		self.running = False

	"""Start the DARN daemon. This call blocks until stop() is called. """
	def run(self):
		if self.running:
			return
		self.info("Starting")
		self.running = True
		self.net.add_timer(0, lambda: self.check_nodes())
		# This method blocks until there are no more timers to run
		self.net.run()

	"""
	Start checking all nodes. This generates a list of 'ping' calls to the
	networking layer. If no succesful pong comes back for a given node,
	an error event is generated. This is checked asynchronously, so this
	call does not block.
	"""
	def check_nodes(self):
		if not self.running:
			return
		for node in self.config.nodes:
			node_config = self.node_configs[node.hostname]
			if node_config is None:
				self.info("Cannot check friend node %s: no configuration known yet" % node.hostname)
				continue
			ping_packet = {
				'type': 'ping',
				'ttl': 15,
				'config_version': node_config['config_version'],
			}
			self.expected_pongs.append(node.hostname)
			self.debug("Sending ping to friend node %s" % node.hostname)
			self.host(node.hostname).send(ping_packet)
		self.net.add_timer(20, lambda: self.check_timeouts())
		self.net.add_timer(30, lambda: self.check_nodes())

	def receive_pong(self, node):
		self.debug("Received pong from friend node %s" % node)
		del self.expected_pongs[node]

	def receive_ping(self, node, config_version):
		self.debug("Received ping from friend node %s" % node)
		pong_packet = {
			'type': 'pong',
			'ttl': 15,
		}
		self.host(node).send(pong_packet)
		if config_version != self.config_version:
			self.info("Friend node %s has older config of mine (version %s), pushing new config version %s"
				% (node, config_version, self.config_version))
			self.push_config_to_node(node)

	"""
	Received an error event. Process it by sending an e-mail, and send a
	sign-off reply.
	"""
	def receive_error_event(self, node, event):
		success=random.random() > 0.5
		signoff_packet = {
			'type': 'signoff',
			'id': event.id,
			'message': "Handled by ignoring",
			'success': success,
		}
		node_config = self.node_configs[node]
		self.debug("Should have sent an e-mail to %s" % node_config['config']['email']);
		self.info("Received erorr event, sending signoff success: " + success)
		self.host(node).send(signoff_packet)

	"""
	Check if any of the hosts we checked earlier didn't respond yet.
	Generate error events for every host that seems to be down.
	"""
	def check_timeouts(self):
		if not self.running:
			return
		if len(self.expected_pongs) == 0:
			return
		for victim in self.expected_pongs:
			self.info("Expected pong from friend %s, but did not receive any, generating error event", victim)
			self.error_seq = self.error_seq + 1
			error_event = {
				'type': 'error',
				'id': uuid.uuid1(self.config['hostname'], self.error_seq),
				'victim': victim,
				'ttl': 20,
				'message': "%s failed to received response from %s within 30 seconds",
			}
			error_event_status = {
				testament: self.node_configs[victim]['testament'],
				current_index: None,
				timeout: datetime.fromtimestamp(0),
				node_failed: False,
			}
			self.error_events.append((error_event, error_event_status))
		self.pump_error_events()

	"""
	For every error event that's still active, check if we need to send it
	to the next node in the victim's testament list.
	"""
	def pump_error_events(self):
		self.debug("Pumping %d error events" % len(self.error_events))
		for (event, event_status) in self.error_events:
			if event_status.timeout <= datetime.now() or event_status.node_failed:
				if event_status.current_index is None:
					# this event was never sent anywhere
					event_status.current_index = 0
				else:
					event_status.current_index = event_status.current_index + 1
				if len(event_status.testament) >= event_status.current_index:
					raise SystemExit, "All testament nodes for a victim failed!"
				current_node = event_status.testament[event_status.current_index]
				self.info("Sending error event about victim %s to node %s", event.victim, current_node)
				event_status.timeout = datetime.now() + datetime.timedelta(seconds=20)
				event_status.node_failed = False
				self.host(current_node).send(event)

	"""
	Process an error-event sign-off packet from a node. If the sign-off is
	succesful, forget about the error event. If it's unsuccesfull, immediately
	mark the error event so that it is sent to the next testament node.
	"""
	def process_error_event_signoff(self, node, id, success):
		self.debug("Received error event signoff packet from node %s, success %s", node, success)
		new_error_events = []
		for (event, event_status) in self.error_events:
			if event.id == id:
				self.debug("Packet is about victim %s", victim)
				if success:
					self.info("Node %s succesfully signed-off error event about victim %s", node, victim)
					continue
				else:
					self.info("Node %s failed to handle error event about victim %s", node, victim)
					event_status.node_failed = True
			new_error_events.append((event, event_status))
		self.error_events = new_error_events
		self.pump_error_events()

	"""
	"""
	def reload(self):
		self.config = self.load_config(self.configfile)
		self.node_key = self.generate_node_key()
		self.testament = self.generate_testament()
		self.config_version = int(time.time())
		self.info("Loaded configuration version %s" % self.config_version)
		(host, port) = self.split_hostname(self.config['hostname'])
		self.net.create_server_socket(host, port, self.data_from_unidentified_host)
		self.push_config()

	"""
	Load configuration from the given file.
	"""
	def load_config(self, configfile):
		return {
			'nodes': [],
			'hostname': 'localhost:1337',
		}

	"""
	Generate testament from configuration. See module
	documentation for more information about the testament.
	"""
	def generate_testament(self):
		return []

	"""
	Generate a node key. See module documentation for more information
	about the testament.
	"""
	def generate_node_key(self):
		return "four"

	"""Push configuration, testament and node key to all nodes."""
	def push_config(self):
		for node in self.config['nodes']:
			self.push_config_to_node(node)

	"""Push configuration, testament and node key to given node."""
	def push_config_to_node(self, node):
		config_push = {
			'type': 'config',
			'ttl': '20',
			'config': self.config,
			'testament': self.testament,
			'node_key': self.node_key,
			'config_version': self.config_version,
		}
		self.debug("Pushing my configuration to node %s" % node)
		self.host(node).send(config_push)

if __name__ == "__main__":
	darn = DARN("/etc/darn.conf")
	darn.run()
