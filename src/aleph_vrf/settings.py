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
        default="4992b4127d296b240bbb73058daea9bca09f717fa94767d6f4dc3ef53b4ef5ce",
        description="VRF function to use.",
    )
    NB_EXECUTORS: int = Field(
        default=32, description="Number of executors to use."
    )
    NB_BYTES: int = Field(
        default=32, description="Number of bytes of the generated random number."
    )

    class Config:
        env_prefix = "ALEPH_VRF_"
        case_sensitive = False
        env_file = ".env"


settings = Settings()
