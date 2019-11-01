# device.py copied from Google IoT examples.


import argparse
import datetime
import json
import os
import ssl
import time

import strand
import jwt
import paho.mqtt.client as mqtt
import multiprocessing
import urllib.request
import logging

logging.basicConfig(level="DEBUG")
DEBUG = True

MAXIMUM_BACKOFF_TIME = 32

MAX_TIME_BETWEEN_ACKS = 60

STRAND_URL = "http://localhost:5000"

jwt_exp_mins = 240
should_backoff = False
client_connected = False

led_pattern_process = None


def create_jwt(project_id, private_key_file, algorithm):
    """Create a JWT (https://jwt.io) to establish an MQTT connection."""
    token = {
        'iat': datetime.datetime.utcnow(),
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=jwt_exp_mins),
        'aud': project_id
    }
    with open(private_key_file, 'r') as f:
        private_key = f.read()
    print('Creating JWT using {} from private key file {}'.format(
        algorithm, private_key_file))
    return jwt.encode(token, private_key, algorithm=algorithm)


def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))

def merge_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

class Process(object):
    global led_pattern_process
    led_pattern_process = multiprocessing.Process()


class Device(object):
    """Represents the state of a single device."""

    def __init__(self):
        self.led_on = False
        self.connected = False
        self.pattern = None
        self.error = False
        self.errormsg = None

        self.device_id = None
        self.mqtt_telemetry_topic = "state"
        self.registry_id = None
        self.lastack = datetime.datetime.utcnow()


    def get_status(self):
        logging.debug('Refreshing device status...')
        return_status = {}
        latest_status = {
            "deviceId": self.device_id,
            "registryId": self.registry_id,
            "lastResultStatus": None,
            "pattern": None,
            "led_on": None
        }
        try:
            data = urllib.request.urlopen("{}/status".format(STRAND_URL)).read()
            return_status = json.loads(data.decode('utf-8'))
        except Exception as e:
            logging.debug("Exception: {}".format(e))
            logging.info("Unable to contact strand server at {}".format(STRAND_URL))
            latest_status['lastResultStatus'] = False
        logging.debug('Status endpoint returned: {}'.format(return_status))

        latest_status = merge_dicts(latest_status,return_status)
        logging.debug("Current status: {}".format(latest_status))
        self.pattern = latest_status['pattern']
        return latest_status

    def send_status(self, client):
        current_status = self.get_status()
        mqtt_topic = self.mqtt_telemetry_topic
        payload = current_status
        payload = json.dumps(payload)
        print('Publishing message \'{}\''.format(payload))
        client.publish(mqtt_topic, payload, qos=1)

    def send_config(self, config):
        req = urllib.request.Request('{}/setpattern'.format(STRAND_URL))
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        jsondata = json.dumps(config).encode('utf-8')
        req.add_header('Content-Length', len(jsondata))
        try:
            req = urllib.request.urlopen(req, jsondata)
            logging.debug("Server response: {}".format(req.getcode()))

        except Exception as e:
            logging.debug("Exception: {}".format(e))
            logging.info("Unable to contact strand server at {}".format(STRAND_URL))

        if req.getcode() != 200:
            logging.debug("Server returned an error.")
            return False
        else:
            return True

    def send_pattern(self, pattern):
        pattern = pattern.lower()
        logging.debug("Checking current pattern: {}".format(self.pattern))
        logging.debug("Desired pattern {}".format(pattern))
        if pattern == self.pattern:
            logging.info("Already running {}, ignoring".format(pattern))
            return True
        desired_config = {
            "pattern": pattern
        }
        result = self.send_config(desired_config)
        logging.debug("Set pattern returned: {}".format(result))


def wait_for_connection(timeout):
    """Wait for the device to become connected."""
    total_time = 0
    connected = False
    while not connected and total_time < timeout:
        time.sleep(1)
        total_time += 1

    if not connected:
        raise RuntimeError('Could not connect to MQTT bridge.')

def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print('Connection Result:', error_str(rc))

def on_disconnect(client, userdata, rc):
    """Callback for when a device disconnects."""
    print('Disconnected: {} {}', error_str(rc), userdata)
    global client_connected
    client_connected = False
    client.loop_stop()
    print("We have become disconnected, trying to reconnect.")


def on_publish(unused_client, unused_userdata, unused_mid):
    """Callback when the device receives a PUBACK from the MQTT bridge."""
    device.lastack = datetime.datetime.utcnow()
    if DEBUG:
        print('Published message acked.')

def on_subscribe(unused_client, unused_userdata, unused_mid,
                 granted_qos):
    """Callback when the device receives a SUBACK from the MQTT bridge."""
    print('Subscribed: ', granted_qos)
    if granted_qos[0] == 128:
        print('Subscription failed.')

def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = message.payload.decode('utf-8')
    logging.debug('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
        payload, message.topic, str(message.qos)))

    if not payload:
        return False

    data = json.loads(payload)

    device.send_pattern(data['pattern'])

def on_log(client, userdata, level, buf):
    print("{} Client log: {}", datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"), buf)


def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Example Google Cloud IoT MQTT device connection code.')
    parser.add_argument(
        '--project_id',
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        required=True,
        help='GCP cloud project name.')
    parser.add_argument(
        '--registry_id', required=True, help='Cloud IoT registry id')
    parser.add_argument(
        '--device_id',
        required=True,
        help='Cloud IoT device id')
    parser.add_argument(
        '--private_key_file', required=False, help='Path to private key file.', default='.keys/device.key')
    parser.add_argument(
        '--algorithm',
        choices=('RS256', 'ES256'),
        required=False,
        help='Which encryption algorithm to use to generate the JWT.',
        default='RS256')
    parser.add_argument(
        '--cloud_region', default='us-central1', help='GCP cloud region')
    parser.add_argument(
        '--ca_certs',
        default='.keys/roots.pem',
        help='CA root certificate. Get from https://pki.google.com/roots.pem')
    parser.add_argument(
        '--num_messages',
        type=int,
        default=100,
        help='Number of messages to publish.')
    parser.add_argument(
        '--mqtt_bridge_hostname',
        default='mqtt.googleapis.com',
        help='MQTT bridge hostname.')
    parser.add_argument(
        '--mqtt_bridge_port', type=int, default=8883, help='MQTT bridge port.')
    parser.add_argument(
        '--message_type', choices=('event', 'state'),
        default='event',
        help=('Indicates whether the message to be published is a '
              'telemetry event or a device state message.'))

    return parser.parse_args()


def get_client(
        project_id, cloud_region, registry_id, device_id, private_key_file,
        algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client = mqtt.Client(
            client_id=('projects/{}/locations/{}/registries/{}/devices/{}'
                       .format(
                               project_id,
                               cloud_region,
                               registry_id,
                               device_id)))

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
            username='unused',
            password=create_jwt(
                    project_id, private_key_file, algorithm))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port, keepalive=60)

    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(device_id)

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)

    return client

def client_connection(client,args,jwtIssued):
    # Wait if backoff is required.
    if should_backoff:
        # If backoff time is too large, give up.
        if minimum_backoff_time > MAXIMUM_BACKOFF_TIME:
            print('Exceeded maximum backoff time. Giving up.')
            return False

        # Otherwise, wait and connect again.
        delay = minimum_backoff_time + time.random.randint(0, 1000) / 1000.0
        print('Waiting for {} before reconnecting.'.format(delay))
        time.sleep(delay)
        minimum_backoff_time *= 2
        client.connect(args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    # Watching for last ack because disconnection detection doesnt work.
    seconds_since_ack = (abs(datetime.datetime.utcnow() - device.lastack)).total_seconds()

    seconds_since_issue = (datetime.datetime.utcnow() - jwtIssued).seconds
    if DEBUG:
        print("JWT is {} seconds old.".format(seconds_since_issue))
        print("Last ACK was: {}, {} seconds ago.".format(device.lastack, seconds_since_ack))

    if seconds_since_ack > MAX_TIME_BETWEEN_ACKS:
        print('Last ACK was {} seconds ago, MAX_TIME_BETWEEN_ACKS: {}'.format(seconds_since_ack, MAX_TIME_BETWEEN_ACKS))
        main()
    elif seconds_since_issue > 60 * jwt_exp_mins:
        print('Refreshing token after {}s').format(seconds_since_issue)
        client.loop_stop()
        jwtIssued = datetime.datetime.utcnow()
        client = get_client(
            args.project_id, args.cloud_region,
            args.registry_id, args.device_id, args.private_key_file,
            args.algorithm, args.ca_certs, args.mqtt_bridge_hostname,
            args.mqtt_bridge_port)
        client.loop_start()

def main():
    args = parse_command_line_args()

    global device
    device = Device()
    device.registry_id = args.registry_id
    device.device_id = args.device_id
    device.mqtt_telemetry_topic = '/devices/{}/state'.format(args.device_id)

    global minimum_backoff_time



    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(args.device_id)

    jwtIssued = datetime.datetime.utcnow()
    client = get_client(
        args.project_id, args.cloud_region, args.registry_id, args.device_id,
        args.private_key_file, args.algorithm, args.ca_certs,
        args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    device.get_status()


    client.loop_start()



    while True:

        client_connection(client, args, jwtIssued)
        device.send_status(client)
        time.sleep(30)

    print('Finished.')

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)



    client.disconnect()
    client.loop_stop()
    print('Finished loop successfully. Goodbye!')


if __name__ == '__main__':
    main()
