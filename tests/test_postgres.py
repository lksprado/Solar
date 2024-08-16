import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import pytest

# TESTANDO CONEXÃO COM POSTGRES DOCKER
# CHECK IF SOMETHING IS LISTENING ON PORT 5432
# $ nc -zv localhost 5432 
# MUST RETURN: Connection to localhost 5432 port [tcp/postgresql] succeeded!


def test_connection():
    try:
        # Creating an engine
        engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5432/postgres') # 172.18.0.3
        

        # Connecting to the database
        with engine.connect() as connection:
            # Execute a simple query to test the connection
            result = connection.execute(text("SELECT 1"))
            print("Connection successful!")
            for row in result:
                print(row)

    except SQLAlchemyError as e:
        print("Connection failed!")
        print(e)

# Run the test
test_connection()


# postgresql://postgres:pg12345@localhost:5432/employees