import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import pytest

def test_connection():
    try:
        # Creating an engine
        engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@172.17.0.1:5432/employees') 
        

        # Connecting to the database
        with engine.connect() as connection:
            # Execute a simple query to test the connection
            result = connection.execute("SELECT 1")
            print("Connection successful!")
            for row in result:
                print(row)

    except SQLAlchemyError as e:
        print("Connection failed!")
        print(e)

# Run the test
test_connection()


# postgresql://postgres:pg12345@localhost:5432/employees