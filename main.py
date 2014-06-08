import uuid
from datetime import datetime, timedelta
import time
import random
from networking import *
import re
import sys
import smtplib
from email.mime.text import MIMEText
import signal, os

def split_hostname(node):
	m = re.match(r'^(.+?)(?::(\d+))?$', node)
	return (m.group(1), int(m.group(2)))

class DARNode:
	def __init__(self, name):
		self.name = name
		self.connection = None
		self.expecting_pong = False
		self.failed = False
		self.config = None
		self.config_version = 0
		self.testament = None
		self.node_key = None
		self.maintenance_mode = False

	def connect(self):
		assert self.connection is None
		(hostname, port) = split_hostname(self.name)
		self.connection = DARNHost(self._initialize_outbound_connection, self._receive_data, self._report_error)
		self.connection.setHost(hostname, port)
		self.connection.connect()

	def adopt_connection(self, host):
		(hostname, port) = split_hostname(self.name)
		host.setHost(hostname, port)
		if self.connection is None:
			host.change_callbacks(self._initialize_outbound_connection, self._receive_data, self._report_error)
			self.connection = host
		else:
			host.merge(self.connection)

	def set_config(self, config, version, testament, node_key):
		self.config = config
		self.config_version = version
		self.testament = testament
		self.node_key = node_key

	def send_ping(self):
		ping_packet = {
			'type': 'ping',
			'ttl': 15,
			'config_version': self.config_version,
		}
		self.expecting_pong = True
		darn.debug("Sending ping to friend node %s, config version %d" % (self.name, self.config_version))
		self.connection.send(ping_packet)

	"""Push my configuration, testament and node key to this node."""
	def push_config(self, other):
		config_push = {
			'type': 'config',
			'ttl': '20',
			'config': other.config,
			'testament': other.testament,
			'node_key': other.node_key,
			'config_version': other.config_version,
		}
		darn.debug("Pushing my %s configuration to node %s" % (other.name, self.name))
		self.connection.send(config_push)

	def _initialize_outbound_connection(self, host):
		assert host == self.connection
		self.connection.send_priority({'hostname': darn.mynode.name})

	def _receive_data(self, host, data):
		assert host == self.connection
		darn.debug("DARN Host Data from identified host %s: %s" % (self.name, data))

		if 'type' not in data:
			host.destroy()
			return

		if data['type'] == "config":
			darn.info("Noted configuration for identified host: %s" % self.name)
			self.set_config(data['config'], data['config_version'], data['testament'], data['node_key'])
		elif data['type'] == "ping":
			darn.debug("Received ping from friend node %s" % self.name)
			config_version = data['config_version']
			if darn.maintenance_shutdown:
				if config_version != 0:
					maintenance_packet = {
						'hostname': darn.mynode.name,
						'type': 'maintenance',
					}
					self.connection.send(maintenance_packet)
				return
			pong_packet = {
				'type': 'pong',
				'ttl': 15,
			}
			if config_version != darn.mynode.config_version:
				darn.info("Friend node %s has older config of mine (version %s), pushing new config version %s"
					% (self.name, config_version, darn.mynode.config_version))
				self.push_config(darn.mynode)
			self.connection.send(pong_packet)
		elif data['type'] == "pong":
			darn.debug("Received pong from friend node %s" % self.name)
			self.expecting_pong = False
			self.failed = False
		elif data['type'] == "error":
			darn.info("Received error from friend node %s" % self.name)
			darn.receive_error_event(self, data)
		elif data['type'] == "signoff":
			darn.info("Received signoff event from node %s, success=%s: %s" % (self.name, data['success'], data['message']))
			darn.process_error_event_signoff(self.name, data['id'], data['success'])
		elif data['type'] == "maintenance":
			self.config = None
			self.config_version = 0
		else:
			darn.info("Received unknown packet type %s from node %s" % (data['type'], self.name))
	
	def _report_error(self, host, exctype, error):
		assert host == self.connection
		darn.info("Error while connecting to node %s: %s" % (self.name, error))

class DARN:
	VERSION = "0.1"
	SEND_PINGS=1
	LOG_DEBUG=0

	def log(self, severity, message):
		hostname = "unknown"
		if hasattr(self, 'mynode'):
			hostname = self.mynode.name
		print "%s: DARN[%s][%s]: %s" % (datetime.now(), hostname, severity, message)

	def info(self, message):
		self.log("info", message)

	def debug(self, message):
		if DARN.LOG_DEBUG:
			self.log("debug", message)

	"""Create a DARN object. Read config from given file. """
	def __init__(self, configfile):
		self.info("Initialising DARN version " + DARN.VERSION)
		self.configfile = configfile
		self.net = DARNetworking()
		self.running = False
		self.nodes = {}
		self.error_seq = 1
		self.error_events = []
		self.maintenance_shutdown = False
		self.reload()
		(host, port) = split_hostname(self.mynode.name)
		host = ''
		if 'bind_host' in self.mynode.config:
			host = self.mynode.config['bind_host']
		self.debug("Going to listen on host %s port %s" % (host if host != '' else '*', port))
		self.net.create_server_socket(host, port, lambda *_: None, self.data_from_unidentified_host)

		for node in self.mynode.config['nodes']:
			name = node['hostname']
			self.nodes[name] = DARNode(name)
			self.nodes[name].connect()

	def data_from_unidentified_host(self, host, data):
		self.debug("DARN Host connected to me: %s and sent: %s" % (host, data))
		if 'hostname' not in data:
			host.destroy()
			return
		if data['hostname'] in self.nodes:
			node = self.nodes[data['hostname']]
		else:
			node = DARNode(data['hostname'])
			self.nodes[data['hostname']] = node
		node.adopt_connection(host)

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
		if not DARN.SEND_PINGS:
			return
		for name in self.mynode.config['nodes']:
			node = self.nodes[name['hostname']]
			node.send_ping()
		self.net.add_timer(10, self.check_timeouts)
		self.net.add_timer(15, self.check_nodes)

	def handle_error_event(self, event, callback):
		victim_config = self.nodes[event['victim']].config
		if not 'email' in victim_config:
			callback(False, "Cannot send e-mail regarding failure of victim %s: no e-mail address known" % event['victim'])
		email = victim_config['email']

		if not 'smtp' in self.mynode.config or not 'sender' in self.mynode.config['smtp'] or not 'host' in self.mynode.config['smtp']:
			callback(False, "Cannot send e-mail regarding failure of victim %s: no valid smtp configuration" % event['victim'])

		body  = "Error event report fired!\n"
		body += "Event report ID: %s\n" % event['id']
		body += "Time: %s\n" % datetime.now()
		body += "Victim: %s\n" % event['victim']
		body += "Message: %s\n" % event['message']

		msg = MIMEText(body)
		msg['Subject'] = "DARN! Error event report"
		msg['From'] = self.mynode.config['smtp']['sender']
		msg['To'] = email
		email_succeeded = None
		email_error = None
		try:
			s = smtplib.SMTP(self.mynode.config['smtp']['host'])
			recipients_failed = s.sendmail(self.mynode.config['smtp']['sender'], [email], msg.as_string())
			s.quit()
			if len(recipients_failed) > 0:
				email_succeeded = False
				email_error = "Failed to send to some recipients: " + str(recipients_failed)
			else:
				email_succeeded = True
		except Exception as e:
			email_succeeded = False
			email_error = str(e)
		callback(email_succeeded, email_error)

	"""
	Received an error event. Process it by sending an e-mail, and send a
	sign-off reply. 'node' is the sender of this error event; the victim
	is in event['victim'].
	"""
	def receive_error_event(self, node, event):
		self.debug("Received error event for node %s" % node.name)
		if event['victim'] not in self.nodes or self.nodes[event['victim']].config is None:
			self.info("Received error event about victim %s, but I don't have its node config, so can't inform it" % event['victim'])
			signoff_packet = {
				'type': 'signoff',
				'id': event['id'],
				'message': "Can't signoff, don't have a node config for this node",
				'success': False,
			}
			node.connection.send(signoff_packet)
			return

		def handle_message(success, message):
			signoff_packet = {
				'type': 'signoff',
				'id': event['id'],
				'message': message,
				'success': success,
			}
			node.connection.send(signoff_packet)

		self.handle_error_event(event, handle_message)

	"""
	Check if any of the hosts we checked earlier didn't respond yet.
	Generate error events for every host that seems to be down.
	"""
	def check_timeouts(self):
		if not self.running:
			return
		for victim in self.mynode.config['nodes']:
			victim = victim['hostname']
			node = self.nodes[victim]
			if not node.expecting_pong:
				continue
			if not node.config:
				self.info("Expected pong from friend %s, but did not receive any; however, don't have node configuration, so silent ignore" % victim)
				continue
			if not node.testament:
				self.info("Expected pong from friend %s, but did not receive any; however, we don't know its testament, so silent ignore" % victim)
				continue
			if node.failed:
				self.info("Expected pong from friend %s, but did not receive any, host is probably still down" % victim)
				continue
			self.info("Expected pong from friend %s, but did not receive any, generating error event" % victim)
			node.failed = True
			self.error_seq = self.error_seq + 1
			error_event = {
				'type': 'error',
				'id': str(uuid.uuid1(None, self.error_seq)),
				'victim': victim,
				'ttl': 20,
				'message': "%s failed to receive response from %s within 30 seconds" % (self.mynode.name, victim),
			}
			error_event_status = {
				'testament': node.testament,
				'current_index': None,
				'timeout': datetime.fromtimestamp(0),
				'node_failed': False, # XXX reporter_failed?
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
					# this event was never sent anywhere (or all nodes failed and we're trying them all again)
					event_status['current_index'] = 0
				else:
					event_status['current_index'] += 1

				if len(event_status['testament']) <= event_status['current_index']:
					self.info("All testament nodes for a victim failed. Starting over.")
					event_status['current_index'] = None
					event_status['timeout'] = datetime.now() + timedelta(minutes=5)
					event_status['node_failed'] = False
					continue

				current_node = event_status['testament'][event_status['current_index']]
				event_status['timeout'] = datetime.now() + timedelta(seconds=20)
				event_status['node_failed'] = False
				if current_node == self.mynode.name:
					self.info("Trying to handle error event about victim %s myself" % event['victim'])
					def handle_response(success, message):
						successstr = "Failed"
						if success:
							successstr = "Succeeded"
						self.info("%s to handle error event about victim %s myself: %s" % (successstr, event['victim'], message))
						self.process_error_event_signoff(current_node, event['id'], success)
					self.handle_error_event(event, handle_response)
				else:
					self.info("Sending error event about victim %s to node %s" % (event['victim'], current_node))
					if current_node not in self.nodes:
						self.nodes[current_node] = DARNode(current_node)
						self.nodes[current_node].connect()
					self.nodes[current_node].connection.send(event)

	"""
	Process an error-event sign-off packet from a node. If the sign-off is
	succesful, forget about the error event. If it's unsuccesfull, immediately
	mark the error event so that it is sent to the next testament node.
	"""
	def process_error_event_signoff(self, node, id, success):
		self.debug("Received error event signoff packet from node %s, success %s" % (node, success))
		new_error_events = []
		for (event, event_status) in self.error_events:
			if event['id'] == id:
				victim = event['victim']
				self.debug("Packet is about victim %s" % victim)
				if success:
					self.info("Node %s succesfully signed-off error event about victim %s" % (node, victim))
					continue
				else:
					self.info("Node %s failed to handle error event about victim %s" % (node, victim))
					event_status['node_failed'] = True
			new_error_events.append((event, event_status))
		# TODO: Niet steeds opnieuw schrijven maar gewoon een dict gebruiken
		self.error_events = new_error_events
		self.pump_error_events()

	"""
	"""
	def reload(self):
		config = self.load_config(self.configfile)
		self.mynode = DARNode(config['hostname'])
		self.mynode.set_config(config, int(time.time()), self.generate_testament(config), self.generate_node_key())
		self.info("Loaded configuration version %s" % self.mynode.config_version)
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
	def generate_testament(self, config):
		nodes = []
		for node in config['nodes']:
			nodes.append(node['hostname'])
		return nodes

	"""
	Generate a node key. See module documentation for more information
	about the testament.
	"""
	def generate_node_key(self):
		return "four"

	def enable_maintenance(self):
		self.maintenance_shutdown = True
		maintenance_packet = {
			'hostname': self.mynode.name,
			'type': 'maintenance',
		}
		for name in self.mynode.config['nodes']:
			node = self.nodes[name['hostname']]
			node.connection.send(maintenance_packet)
		self.net.add_timer(10, lambda: sys.exit(0))

	"""Push configuration, testament and node key to all nodes."""
	def push_config(self):
		for node in self.nodes:
			node.push_config(self.mynode)

if __name__ == "__main__":
	darn = DARN(sys.argv[1])
	def _reload(a, b):
		darn.reload()
	signal.signal(signal.SIGHUP, _reload)
	def _maintenance(a, b):
		darn.enable_maintenance()
	signal.signal(signal.SIGUSR1, _maintenance)
	darn.run()
