# Import necessary libraries
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# Use the loaded environment variables for Snowflake connection
conn_params = {
    'account': os.environ.get("SNOWFLAKE_ACCOUNT"),
    'user': os.environ.get("SNOWFLAKE_USER"),
    'password': os.environ.get("SNOWFLAKE_PASSWORD"),
    'warehouse': os.environ.get("SNOWFLAKE_WAREHOUSE"),
    'database': os.environ.get("SNOWFLAKE_DATABASE"),
    'schema': os.environ.get("SNOWFLAKE_SCHEMA"),
}

def main():
    # Main function to run the Streamlit app
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()

    # Define available operations in a list
    operations = ["Please Select from the following options", "Setup Environment", "Generate Data", "View Data", "Forecast Data", "Anomaly Detection", "Clean Up Environment"]

    # Create a dropdown to select the operation
    operation = st.selectbox("Select Operation", operations)

    if operation == "Please Select from the following options":
        st.write("")
    elif operation == "Setup Environment":
        setup_environment(cur)
    elif operation == "Generate Data":
        generate_data(cur)
    elif operation == "View Data":
        view_data(cur)
    elif operation == "Forecast Data":
        forecast_data(cur)
    elif operation == "Anomaly Detection":
        anomaly_detection(cur)
    elif operation == "Clean Up Environment":
        clean_up_environment(cur)

    conn.close()

if __name__ == '__main__':
    main()








