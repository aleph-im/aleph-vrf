from typing import Optional

from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from hexbytes import HexBytes
from pydantic import BaseSettings, Field, HttpUrl


class Settings(BaseSettings):
    API_HOST: HttpUrl = Field(
        default="https://api2.aleph.im",
        description="URL of the reference aleph.im Core Channel Node.",
    )
    CORECHANNEL_AGGREGATE_ADDRESS = Field(
        default="0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
        description="Address posting the `corechannel` aggregate.",
    )
    CORECHANNEL_AGGREGATE_KEY = Field(
        default="corechannel", description="Key for the `corechannel` aggregate."
    )
    FUNCTION: str = Field(
        default="c52d6586837b0992d3043d7d0dbe8f49b180911e8c3a87c732f8dbcef2c254ab",
        description="VRF function to use.",
    )
    NB_EXECUTORS: int = Field(default=32, description="Number of executors to use.")
    NB_BYTES: int = Field(
        default=32, description="Number of bytes of the generated random number."
    )
    ETHEREUM_PRIVATE_KEY: Optional[str] = Field(
        default=None, description="Application private key to post to aleph.im."
    )

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
