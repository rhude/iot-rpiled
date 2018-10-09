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

DEBUG = False


def create_jwt(project_id, private_key_file, algorithm):
    """Create a JWT (https://jwt.io) to establish an MQTT connection."""
    token = {
        'iat': datetime.datetime.utcnow(),
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
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

class Process(object):
    led_pattern_process = multiprocessing.Process()


class Device(object):
    """Represents the state of a single device."""

    def __init__(self):
        self.led_on = False
        self.connected = False
        self.pattern = None
        self.error = False
        self.errormsg = None

        self.jwtIssued = None

        self.ledpattern = strand.Pattern()
        global led_pattern_process


    def get_status(self):
        if DEBUG:
            print('Refreshing device status...')

    def led_start(self, pattern):
        print('Checking if lights are in a current pattern.')
        if Process.led_pattern_process.is_alive():
            print('Lights are in a pattern, terminating...')
            Process.led_pattern_process.terminate()
            Process.led_pattern_process.join()

        pattern = pattern.lower()
        pattern = pattern.strip()
        pattern = pattern.replace(" ", "")

        print('Setting pattern to: {}'.format(pattern))

        Process.led_pattern_process = multiprocessing.Process(target=self.ledpattern.run, args=(pattern,))
        Process.led_pattern_process.start()

    def led_stop(self):
        print('Turning all LEDs off...')
        print('Checking if lights are in a current pattern.')
        if Process.led_pattern_process.is_alive():
            print('Lights are in a pattern, terminating...')
            Process.led_pattern_process.terminate()
            Process.led_pattern_process.join()
        print('Setting all LEDs off')
        Process.led_pattern_process = multiprocessing.Process(target=self.ledpattern.off)
        Process.led_pattern_process.start()

    def wait_for_connection(self, timeout):
        """Wait for the device to become connected."""
        total_time = 0
        while not self.connected and total_time < timeout:
            time.sleep(1)
            total_time += 1

        if not self.connected:
            raise RuntimeError('Could not connect to MQTT bridge.')

    def on_connect(self, unused_client, unused_userdata, unused_flags, rc):
        """Callback for when a device connects."""
        print('Connection Result:', error_str(rc))
        self.connected = True

    def on_disconnect(self, unused_client, unused_userdata, rc):
        """Callback for when a device disconnects."""
        print('Disconnected:', error_str(rc))
        self.connected = False

    def on_publish(self, unused_client, unused_userdata, unused_mid):
        """Callback when the device receives a PUBACK from the MQTT bridge."""
        if DEBUG:
            print('Published message acked.')

    def on_subscribe(self, unused_client, unused_userdata, unused_mid,
                     granted_qos):
        """Callback when the device receives a SUBACK from the MQTT bridge."""
        print('Subscribed: ', granted_qos)
        if granted_qos[0] == 128:
            print('Subscription failed.')

    def on_message(self, unused_client, unused_userdata, message):
        """Callback when the device receives a message on a subscription."""
        payload = message.payload
        print('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
            payload, message.topic, str(message.qos)))

        # The device will receive its latest config when it subscribes to the
        # config topic. If there is no configuration for the device, the device
        # will receive a config with an empty payload.
        if not payload:
            return

        # The config is passed in the payload of the message. In this example,
        # the server sends a serialized JSON string.
        data = json.loads(payload)

        if data['led_on']:
            Device.led_start(self, data['pattern'])
        elif not data['led_on']:
            Device.led_stop(self)




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


def main():
    args = parse_command_line_args()

    # Create the MQTT client and connect to Cloud IoT.
    client = mqtt.Client(
        client_id='projects/{}/locations/{}/registries/{}/devices/{}'.format(
            args.project_id,
            args.cloud_region,
            args.registry_id,
            args.device_id))
    client.username_pw_set(
        username='unused',
        password=create_jwt(
            args.project_id,
            args.private_key_file,
            args.algorithm))
    client.tls_set(ca_certs=args.ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    device = Device()

    device.jwtIssued = datetime.datetime.utcnow()

    client.on_connect = device.on_connect
    client.on_publish = device.on_publish
    client.on_disconnect = device.on_disconnect
    client.on_subscribe = device.on_subscribe
    client.on_message = device.on_message

    client.connect(args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    client.loop_start()

    # This is the topic that the device will publish telemetry events
    # (temperature data) to.
    mqtt_telemetry_topic = '/devices/{}/events'.format(args.device_id)

    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(args.device_id)

    # Wait up to 5 seconds for the device to connect.
    device.wait_for_connection(5)

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)

    while True:

        device.get_status()

        #Check on when the JWT was issued.
        if DEBUG:
            print("JWT Issued: {}".format(device.jwtIssued))
        seconds_since_issue = (datetime.datetime.utcnow() - device.jwtIssued).seconds
        if seconds_since_issue > 60 * 60:
            print('Refreshing token after {}s').format(seconds_since_issue)
            client.username_pw_set(
                username='unused',
                password=create_jwt(
                    args.project_id,
                    args.private_key_file,
                    args.algorithm))
            device.jwtIssued = datetime.datetime.utcnow()

        payload = json.dumps({
            'led_on': device.led_on,
            'pattern': device.pattern,
            'error': device.error,
            'errormsg': device.errormsg
        })
        if DEBUG:
            print('Publishing payload', payload)
        client.publish(mqtt_telemetry_topic, payload, qos=1)
        # Send events every second.
        time.sleep(5)

    client.disconnect()
    client.loop_stop()
    print('Finished loop successfully. Goodbye!')


if __name__ == '__main__':
    main()
