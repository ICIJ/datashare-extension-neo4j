from pydantic import BaseSettings


class Config(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 8000
