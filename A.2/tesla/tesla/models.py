# ref: https://www.jitsejan.com/scraping-with-scrapy-and-postgres

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

from tesla import settings

DeclarativeBase = declarative_base()


def db_connect() -> Engine:
    """
    Creates database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))


def create_items_table(engine: Engine):
    """
    Create the Items table
    """
    DeclarativeBase.metadata.create_all(engine)


class Items(DeclarativeBase):
    """
    Defines the items model
    """

    # __tablename__ = "items"

    # name = Column("name", String, primary_key=True)
    # price = Column("price", Integer)

    __tablename__ = "wikip"
    
    url = Column("url", String, primary_key=True)
    name = Column("name", String)
    tags = Column("tags", String)
    entities = Column("entities", String)
    parent = Column("parent", String)
    title = Column("title", String)
    body = Column("body", String)
    keywords = Column("keywords", String)
    text = Column("text", String)