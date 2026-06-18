from pydantic import Field
import dagster as dg


class Nb0Cfg(dg.Config):
    a: int = Field(description="this is the paramentr description", default=10)


class Nb1Cfg(dg.Config):
    a: int = Field(description="this is the paramentr description", default=10)


class Nb2Cfg(dg.Config):
    b: int = Field(description="this is the paramentr description", default=10)
