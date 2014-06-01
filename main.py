import uuid
import datetime
import time
import random
from networking import *

class DARN:
	"""Create a DARN object. Read config from given file. """
	def __init__(self, configfile):
		self.configfile = configfile
		self.net = DARNetworking()
		self.running = False
		self.node_configs = {}
		self.expected_pongs = set()
		self.error_seq = 1
		self.error_events = []
		self.config_version = None
		self.reload()

	def stop(self):
		self.running = False

	"""Start the DARN daemon. This call blocks until stop() is called. """
	def run(self):
		if self.running:
			return
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
				continue
			ping_packet = {
				ttl: 15,
				config_version: node_config['config_version'],
			}
			self.expected_pongs.append(node.hostname)
			self.net.ping(node.hostname, ping_packet)
		self.net.add_timer(20, lambda: self.check_timeouts())
		self.net.add_timer(30, lambda: self.check_nodes())

	def receive_pong(self, node):
		del self.expected_pong[node]

	def receive_ping(self, node, config_version):
		self.net.send_pong(node)
		if config_version != self.config_version:
			self.push_config_to_node(node)

	"""
	Received an error event. Process it by sending an e-mail, and send a
	sign-off reply.
	"""
	def receive_error_event(self, node, event):
		success=random.random() > 0.5
		signoff = {
			id: event.id,
			message: "Handled by ignoring",
			success: success,
		}
		self.net.send_signoff(node, signoff)

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
			# this victim still has a ping_packet, generate an
			# error event for this
			self.error_seq = self.error_seq + 1
			error_event = {
				id: uuid.uuid1(self.config['hostname'], self.error_seq),
				victim: victim,
				ttl: 20,
				message: "%s failed to received response from %s within 30 seconds",
			}
			error_event_status = {
				testament: self.node_configs[victim]['testament'],
				current_index: None,
				timeout: datetime.fromtimestamp(0),
				peer_failed: False,
			}
			self.error_events.append((error_event, error_event_status))
		self.pump_error_events()

	"""
	For every error event that's still active, check if we need to send it
	to the next node in the victim's testament list.
	"""
	def pump_error_events(self):
		for (event, event_status) in self.error_events:
			if event_status.timeout <= datetime.now or event_status.peer_failed:
				if event_status.current_index is None:
					# this event was never sent anywhere
					event_status.current_index = 0
				else:
					event_status.current_index = event_status.current_index + 1
				if len(event_status.testament) >= event_status.current_index:
					raise SystemExit, "All testament peers for a victim failed!"
				current_peer = event_status.testament[event_status.current_index]
				event_status.timeout = datetime.now() + datetime.timedelta(seconds=20)
				event_status.peer_failed = False
				self.net.send_error_event(current_peer, event)

	"""
	Process an error-event sign-off packet from a peer. If the sign-off is
	succesful, forget about the error event. If it's unsuccesfull, immediately
	mark the error event so that it is sent to the next testament peer.
	"""
	def process_error_event_signoff(self, id, success):
		new_error_events = []
		for (event, event_status) in self.error_events:
			if event.id == id:
				if success:
					continue
				else:
					event_status.peer_failed = True
			new_error_events.append((event, event_status))
		self.error_events = new_error_events
		self.pump_error_events()

	"""
	"""
	def reload(self):
		self.config = self.load_config(self.configfile)
		self.node_key = self.generate_node_key()
		self.testament = self.generate_testament()
		self.config_version = time.time()
		self.push_config()

	"""
	Load configuration from the given file.
	"""
	def load_config(self, configfile):
		return {
			'nodes': [],
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
			config: self.config,
			testament: self.testament,
			node_key: self.node_key,
			config_version: self.config_version,
		}
		self.net.push_config(node, self.node_key, config_push)

if __name__ == "__main__":
	darn = DARN("/etc/darn.conf")
	darn.run()
