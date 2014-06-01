import uuid
from datetime import datetime, timedelta
import time
import random
from networking import *
import re
import sys

class DARN:
	VERSION = "0.1"

	def log(self, severity, message):
		hostname = "unknown"
		if hasattr(self, 'config') and 'hostname' in self.config:
			hostname = self.config['hostname']
		print "%s: DARN[%s][%s]: %s" % (datetime.now(), hostname, severity, message)

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
		if node not in self.hosts:
			(hostname, port) = self.split_hostname(node)
			self.hosts[node] = host = DARNHost(lambda x: host.send({'hostname': self.config['hostname']}), self.data_from_identified_host)
			host.setHost(hostname, port)
			host.connect()
			return host
		return self.hosts[node]

	def data_from_unidentified_host(self, host, data):
		self.debug("DARN Host connected to me: %s and sent: %s" % (host, data))
		if 'hostname' not in data:
			host.destroy()
			return
		host.merge(self.hosts[data['hostname']])
		(hostname, port) = self.split_hostname(data['hostname'])
		host.setHost(hostname, port)
		host.changeCallback(self.data_from_identified_host)

	def data_from_identified_host(self, host, data):
		self.debug("DARN Host Data from identified host: %s" % data)

		if 'type' not in data:
			host.destroy()
			return

		peer = None
		for _peer in self.hosts:
			if self.hosts[_peer] == host:
				peer = _peer
				break

		if peer is None:
			self.info("Failed to match data from host %s to a peer" % host);
			print self.hosts
			return

		if data['type'] == "config":
			self.info("Noted configuration for identified host: %s" % peer)
			self.node_configs[peer] = data;
			print self.node_configs
		elif data['type'] == "ping":
			self.debug("Received ping from friend node %s" % peer)
			config_version = data['config_version']
			pong_packet = {
				'type': 'pong',
				'ttl': 15,
			}
			self.host(peer).send(pong_packet)
			if config_version != self.config_version:
				self.info("Friend node %s has older config of mine (version %s), pushing new config version %s"
					% (peer, config_version, self.config_version))
				self.push_config_to_node(peer)
		elif data['type'] == "pong":
			self.debug("Received pong from friend node %s" % peer)
			self.expected_pongs.remove(peer)
		elif data['type'] == "error":
			self.info("Received error from friend node %s" % peer)
			self.receive_error_event(peer, data)
		else:
			abort()

	def stop(self):
		self.info("Stopping")
		self.running = False

	"""Start the DARN daemon. This call blocks until stop() is called. """
	def run(self):
		if self.running:
			return
		self.info("Starting")
		self.running = True
		self.net.add_timer(0, self.check_nodes)
		# This method blocks until there are no more timers to run
		self.net.run()

	"""
	Start checking all nodes. This generates a list of 'ping' calls to the
	networking layer. If no succesful pong comes back for a given node,
	an error event is generated. This is checked asynchronously, so this
	call does not block.
	"""
	def check_nodes(self):
		self.debug("About to check friend nodes")
		if not self.running:
			return
		for node in self.config['nodes']:
			node_config_version = 0;
			if node['hostname'] in self.node_configs:
				node_config_version = self.node_configs[node['hostname']]['config_version']
			ping_packet = {
				'type': 'ping',
				'ttl': 15,
				'config_version': node_config_version,
			}
			self.expected_pongs.add(node['hostname'])
			self.debug("Sending ping to friend node %s" % node['hostname'])
			self.host(node['hostname']).send(ping_packet)
		self.net.add_timer(10, self.check_timeouts)
		self.net.add_timer(20, self.check_nodes)

	"""
	Received an error event. Process it by sending an e-mail, and send a
	sign-off reply. 'node' is the sender of this error event; the victim
	is in event['victim'].
	"""
	def receive_error_event(self, node, event):
		self.debug("Received error event for node=%s" % node)
		print self.node_configs
		if event['victim'] not in self.node_configs:
			self.info("Received error event about victim %s, but I don't have a node config, so can't inform it" % node)
			signoff_packet = {
				'type': 'signoff',
				'id': event['id'],
				'message': "Can't signoff, don't have a node config for this node",
				'success': False,
			}
			print "Sending sign-off packet: " + str(signoff_packet)
			self.host(node).send(signoff_packet)
			return

		victim_config = self.node_configs[event['victim']]
		success=random.random() > 0.5
		signoff_packet = {
			'type': 'signoff',
			'id': event['id'],
			'message': "Handled by ignoring",
			'success': success,
		}
		email = "unknown address"
		if 'email' in victim_config['config']:
			email = victim_config['config']['email']
		self.debug("Should have sent an e-mail to %s" % email)
		self.info("Received erorr event, sending signoff success: %s" % success)

		print "Sending sign-off packet: " + str(signoff_packet)
		self.host(node).send(signoff_packet)

	"""
	Check if any of the hosts we checked earlier didn't respond yet.
	Generate error events for every host that seems to be down.
	"""
	def check_timeouts(self):
		if not self.running:
			return
		for victim in self.expected_pongs:
			if victim not in self.node_configs:
				self.info("Expected pong from friend %s, but did not receive any; however, don't have node configuration, so silent ignore" % victim)
				continue
			if 'testament' not in self.node_configs[victim]:
				self.info("Expected pong from friend %s, but did not receive any; however, node config for %s does not contain testament, so silent ignore" % victim)
				continue
			else:
				self.info("Expected pong from friend %s, but did not receive any, generating error event" % victim)
			self.error_seq = self.error_seq + 1
			error_event = {
				'type': 'error',
				'id': str(uuid.uuid1(None, self.error_seq)),
				'victim': victim,
				'ttl': 20,
				'message': "%s failed to received response from %s within 30 seconds" % (self.config['hostname'], victim),
			}
			error_event_status = {
				'testament': self.node_configs[victim]['testament'],
				'current_index': None,
				'timeout': datetime.fromtimestamp(0),
				'node_failed': False,
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
			self.debug("Error event has status: %s" % event_status)
			
			if event_status['timeout'] <= datetime.now() or event_status['node_failed']:
				if event_status['current_index'] is None:
					# this event was never sent anywhere
					event_status['current_index'] = 0
				else:
					event_status['current_index'] += 1
				if len(event_status['testament']) <= event_status['current_index']:
					raise SystemExit, "All testament nodes for a victim failed!"
				current_node = event_status['testament'][event_status['current_index']]
				self.info("Sending error event about victim %s to node %s" % (event['victim'], current_node))
				event_status['timeout'] = datetime.now() + timedelta(seconds=20)
				event_status['node_failed'] = False
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
			if event['id'] == id:
				self.debug("Packet is about victim %s", victim)
				if success:
					self.info("Node %s succesfully signed-off error event about victim %s", node, victim)
					continue
				else:
					self.info("Node %s failed to handle error event about victim %s", node, victim)
					event_status['node_failed'] = True
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
		fh = open(configfile)
		cfg = json.load(fh)
		fh.close()
		return cfg

	"""
	Generate testament from configuration. See module
	documentation for more information about the testament.
	"""
	def generate_testament(self):
		nodes = []
		for node in self.config['nodes']:
			nodes.append(node['hostname'])
		return nodes

	"""
	Generate a node key. See module documentation for more information
	about the testament.
	"""
	def generate_node_key(self):
		return "four"

	"""Push configuration, testament and node key to all nodes."""
	def push_config(self):
		for node in self.config['nodes']:
			self.push_config_to_node(node['hostname'])

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
	darn = DARN(sys.argv[1])
	darn.run()
