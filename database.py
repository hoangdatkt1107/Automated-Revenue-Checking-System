import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

def connect_to_postgres():
    try:
        # Establish the connection
        connection = psycopg2.connect(
            host= os.getenv("HOST"),
            user= os.getenv("USER"),
            password= os.getenv("PASS_WORD"),
            database= os.getenv("DATABASE"),
            port= os.getenv("PORT")
        )

        print("Connected to PostgreSQL database")
        return connection

    except psycopg2.Error as err:
        print(f"Error: {err}")
        return None


