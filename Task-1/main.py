import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# Use the loaded environment variables
conn_params = {
    'account': os.environ.get("SNOWFLAKE_ACCOUNT"),
    'user': os.environ.get("SNOWFLAKE_USER"),
    'password': os.environ.get("SNOWFLAKE_PASSWORD"),
    'warehouse': os.environ.get("SNOWFLAKE_WAREHOUSE"),
    'database': os.environ.get("SNOWFLAKE_DATABASE"),
    'schema': os.environ.get("SNOWFLAKE_SCHEMA"),
}

