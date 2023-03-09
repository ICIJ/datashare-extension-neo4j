import importlib.metadata
from pathlib import Path

ROOT_DIR = Path(__file__).parent

__version__ = importlib.metadata.version(__package__)
