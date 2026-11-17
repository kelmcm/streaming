from contextlib import contextmanager
import psycopg
from config import DB_CONFIG


@contextmanager
def get_connection():
    with psycopg.connect(**DB_CONFIG) as conn:
        yield conn
