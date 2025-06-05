from config import MYSQL_URL
from models import Base
from sqlalchemy import create_engine

engine = create_engine(MYSQL_URL)
Base.metadata.create_all(engine)
