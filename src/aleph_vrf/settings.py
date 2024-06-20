from typing import Optional

from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.conf import settings as sdk_settings
from hexbytes import HexBytes
from pydantic import BaseSettings, Field, HttpUrl


class Settings(BaseSettings):
    API_HOST: Optional[HttpUrl] = Field(
        default=None,
        description="URL of the reference aleph.im Core Channel Node. "
        "If None, the value from the SDK settings is used.",
    )
    CORECHANNEL_AGGREGATE_ADDRESS = Field(
        default="0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
        description="Address posting the `corechannel` aggregate.",
    )
    CORECHANNEL_AGGREGATE_KEY = Field(
        default="corechannel", description="Key for the `corechannel` aggregate."
    )
    FUNCTION: str = Field(
        default="f6a734dbc98659f030e1cd9c12d8ffb769deac55d42d5db5285fba099755c779",
        description="VRF function to use.",
    )
    NB_EXECUTORS: int = Field(default=32, description="Number of executors to use.")
    NB_BYTES: int = Field(
        default=32, description="Number of bytes of the generated random number."
    )
    ETHEREUM_PRIVATE_KEY: Optional[str] = Field(
        default=None, description="Application private key to post to aleph.im."
    )

    @property
    def api_host(self) -> HttpUrl:
        return self.API_HOST or sdk_settings.api_host

    def private_key(self) -> HexBytes:
        if self.ETHEREUM_PRIVATE_KEY:
            return HexBytes(self.ETHEREUM_PRIVATE_KEY)

        return HexBytes(get_fallback_private_key())

    def aleph_account(self) -> ETHAccount:
        return ETHAccount(self.private_key())

    class Config:
        env_prefix = "ALEPH_VRF_"
        case_sensitive = False
        env_file = ".env"


settings = Settings()
