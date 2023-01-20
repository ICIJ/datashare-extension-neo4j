import uvicorn

from neo4j_app.run.config import Config
from neo4j_app.run.utils import create_app

config = Config()

app = create_app()

uvicorn.run(app, **config.dict())
