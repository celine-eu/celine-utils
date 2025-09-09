import datetime
import uuid
from pydantic import BaseModel, Field

from . import utils


class BasePayload(BaseModel):
    """Base class for all Celine payloads"""
    pass


class BaseRecord(BaseModel):
    """Base class for all Celine records"""
    catalog: str
    schema: str
    table: str
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    created: datetime.datetime = Field(default_factory=utils.utc_now)
    updated: datetime.datetime = Field(default_factory=utils.utc_now)
    data: BasePayload

    @classmethod
    def get_common_fields(cls) -> dict[str, type]:
        result = {}
        for name, field in cls.model_fields.items():
            # Discarding "data"
            if name == 'data':
                continue
            result['_' + name] = field.annotation
        return result

    @classmethod
    def get_data_fields(cls) -> dict[str, type]:
        result = {}
        common_fields = cls.get_common_fields()
        for name, field in cls.model_fields['data'].annotation.model_fields.items():
            if name in common_fields:
                raise AttributeError(f'Field name "{name}" is reserved and cannot be used in a payload')
            result[name] = field.annotation
        return result

    @classmethod
    def get_all_fields(cls) -> dict[str, type]:
        return cls.get_common_fields() | cls.get_data_fields()
