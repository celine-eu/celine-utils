import threading
import time
import string
import random
import hashlib

import paho.mqtt.client as mqtt

from celine.utils.common.config.mqtt import MqttConfig
from celine.utils.common.logger import get_logger


logger = get_logger(__name__)


class Client:

    # Class-level attribute, allows client sharing among different tasks of the same worker.
    # Warning: paho-mqtt might NOT be thread safe. If issue appears please rework this.
    _clients: dict[str, mqtt.Client] = {}

    def __init__(self, config: MqttConfig):
        self._config: MqttConfig = config
        self._config_hash: str = hashlib.sha256(
            config.model_dump_json().encode()
        ).hexdigest()
        self._lock: threading.Lock = threading.Lock()

    def _get_client(self) -> mqtt.Client:
        """Return a connected client for the given configuration

        If no client exists in the internal cache, a new one will be created.
        This method is thread safe.
        """
        with self._lock:
            client = Client._clients.get(self._config_hash)
            if client and client.is_connected():
                logger.debug(f"Already connected (client id: {client._client_id})")
                return client

            random_id = "celine_mqtt_client_" + (
                "".join(random.choice(string.ascii_letters) for _ in range(8))
            )
            logger.info(f"Creating new client (client id: {random_id})")
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=random_id,
            )
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message
            client.on_subscribe = self._on_subscribe
            client.on_unsubscribe = self._on_unsubscribe

            client.username_pw_set(self._config.user, self._config.password)

            client.reconnect_delay_set(min_delay=1, max_delay=64)
            client.connect(
                host=self._config.host,
                port=int(self._config.port),
                keepalive=21,
            )
            client.loop_start()
            time.sleep(0.3)
            Client._clients[self._config_hash] = client
            return Client._clients[self._config_hash]

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info("connected")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.info(
                f"disconnected with error {flags=} {reason_code=} {properties=}"
            )
        else:
            logger.info("disconnected")
        client.loop_stop()

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        for reason_code in reason_code_list:
            if reason_code.is_failure:
                logger.error(f"Broker rejected you subscription: {reason_code}")
            else:
                logger.debug(f"Broker granted the following QoS: {reason_code.value}")

    def _on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        # Be careful, the reason_code_list is only present in MQTTv5.
        # In MQTTv3 it will always be empty
        for reason_code in reason_code_list:
            if not reason_code.is_failure:
                logger.debug(
                    f"unsubscribe succeeded (if SUBACK is received in MQTTv3 it success): {reason_code}"
                )
            else:
                logger.error(f"unsubscribe failed: {reason_code}")

    def _on_message(self, client, userdata, message):
        return self.on_message(message)

    def publish(self, topic, payload: mqtt.PayloadType, qos=0, timeout=3):
        logger.debug(f"Publish message to {topic}")
        msg_info = self._get_client().publish(topic, payload, qos)
        msg_info.wait_for_publish(timeout=timeout)
        return msg_info

    def disconnect(self):
        """Disconnect and delete the client from the internal cache

        This method is thread safe.
        """
        with self._lock:
            client = Client._clients.get(self._config_hash)
            if client and client.is_connected():
                client.disconnect()
                Client._clients.pop(self._config_hash)

    def subscribe(self, topic: str, qos: int):
        self._get_client().subscribe(topic, qos)

    def unsubscribe(self, topic: str):
        self._get_client().unsubscribe(topic)

    def on_message(self, message: mqtt.MQTTMessage):
        raise NotImplementedError(
            "You must override Client.on_message in a subclass before subscribing to any topic"
        )
