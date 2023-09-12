from typing import Literal, List, Union, Optional

from pydantic import BaseSettings, Field, HttpUrl, conlist


class Settings(BaseSettings):
    API_HOST: str = Field(
        "https://api2.aleph.im",
        description="URL of the reference aleph.im Core Channel Node.",
    )
    CORECHANNEL_AGGREGATE_ADDRESS = Field(
        "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
        description="Address posting the `corechannel` aggregate.",
    )
    CORECHANNEL_AGGREGATE_KEY = Field(
        "corechannel", description="Key for the `corechannel` aggregate."
    )
    EXECUTORS: Optional[conlist(HttpUrl, min_length=1)] = Field(
        None,
        description="List of executor VMs to use. If set to None, the coordinator will randomly "
        "select NB_EXECUTORS compute resource nodes to execute the EXECUTOR_VM_HASH VM.",
    )
    EXECUTOR_VM_HASH: str = Field(
        "67705389842a0a1b95eaa408b009741027964edc805997475e95c505d642edd8",
        description="Hash of the executor VM.",
    )
    NB_EXECUTORS: int = Field(32, description="Number of executors to use.")
    NB_BYTES: int = Field(
        32, description="Number of bytes of the generated random number."
    )

    class Config:
        env_prefix = "ALEPH_VRF_"
        case_sensitive = False
        env_file = ".env"


settings = Settings()
