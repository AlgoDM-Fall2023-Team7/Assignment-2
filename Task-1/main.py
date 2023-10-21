# Import necessary libraries
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import dotenv
import os

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

# Define functions for various operations
def setup_environment(cur):
    # Set up the Snowflake environment
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute("CREATE WAREHOUSE AD_FORECAST_DEMO_WH WITH WAREHOUSE_SIZE='XSmall' STATEMENT_TIMEOUT_IN_SECONDS=600 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15")
    cur.execute("USE WAREHOUSE AD_FORECAST_DEMO_WH")
    cur.execute("CREATE DATABASE AD_FORECAST_DEMO")
    cur.execute("CREATE SCHEMA AD_FORECAST_DEMO.DEMO")
    st.write("Environment setup complete.")

def generate_data(cur):
    # Generate and populate the daily_impressions table
    cur.execute("CREATE TABLE DEMO.daily_impressions(day TIMESTAMP, impression_count INTEGER)")
    cur.execute("INSERT INTO DEMO.daily_impressions SELECT DATEADD(DAY, SEQ4(), '2022-09-01'::TIMESTAMP) AS day, ABS(ROUND(NORMAL(35000, 7000, RANDOM(4)))) AS impression_count FROM TABLE(GENERATOR(ROWCOUNT=>96))")
    cur.execute("""
        UPDATE DEMO.daily_impressions
        SET impression_count = (
            (CASE
                WHEN DAYNAME(day) IN ('Sat', 'Sun') THEN 0.7
                WHEN DAYNAME(day) = 'Fri' THEN 0.9
                ELSE 1
            END) *
            (impression_count + (DATEDIFF(DAY, '2022-09-01'::TIMESTAMP, day) * 120))
        )
    """)
    st.write("Data generation complete.")

def view_data(cur):
    # View the data in the daily_impressions table and create a line chart
    cur.execute("SELECT * FROM DEMO.daily_impressions")
    data = cur.fetchall()
    data_df = pd.DataFrame(data, columns=['day', 'impression_count'])
    st.write(data_df)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data_df['day'], y=data_df['impression_count'], mode='lines', name='Impression Count'))
    fig.update_layout(title="Ad Volume Prediction")
    fig.update_xaxes(title_text="Day")
    fig.update_yaxes(title_text="Impression Count")
    st.plotly_chart(fig, use_container_width=True)

def clean_up_environment(cur):
    # Clean up the Snowflake environment
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute("DROP DATABASE IF EXISTS AD_FORECAST_DEMO")
    cur.execute("DROP WAREHOUSE IF EXISTS AD_FORECAST_DEMO_WH")
    st.write("Environment cleanup complete.")

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








