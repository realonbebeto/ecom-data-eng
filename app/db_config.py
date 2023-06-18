from pydantic import BaseSettings
from sqlalchemy import create_engine

class Settings(BaseSettings):
    db_username: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str

    class Config:
        env_file = "~/Kazispace/local/py/ecom_de/.env"

settings = Settings()



DB_URL = f"postgresql+psycopg2://{settings.db_username}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"
ENGINE=create_engine(DB_URL)