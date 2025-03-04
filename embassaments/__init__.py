from embassaments.server import main
from embassaments.himport import import_historical_data

import importlib.metadata as importlib_metadata

__version__ = importlib_metadata.version(__name__)

__all__ = ["main", "import_historical_data"]
