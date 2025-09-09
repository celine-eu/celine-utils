from pydantic import Field
from typing import Optional

from .settings import AppBaseSettings


class MqttConfig(AppBaseSettings):
    host: Optional[str] = Field(default="mqtt", alias="MQTT_HOST")
    port: Optional[str] = Field(default="1883", alias="MQTT_PORT")
    user: Optional[str] = Field(default="admin", alias="MQTT_USER")
    password: Optional[str] = Field(default="admin", alias="MQTT_PASSWORD")
