
import streamlit as st
import pandas as pd
from snowflake.connector import connect
from snowflake.ml.modeling.preprocessing import OneHotEncoder

# Streamlit interface
st.title("Customer Sales Prediction")

# Section for user input
st.sidebar.header("Input Parameters")
st.sidebar.write("Please provide the following information:")

# Gender selection
gender = st.sidebar.selectbox("Gender", ["Male", "Female"])

# Marital Status selection
marital_status = st.sidebar.selectbox("Marital Status", ["Divorced", "Married", "Single", "Unknown", "Widowed"])

# Credit Rating selection
credit_rating = st.sidebar.selectbox("Credit Rating", ["Good", "High Risk", "Low Risk"])

# Education Status selection
education_status = st.sidebar.selectbox("Education Status", ["2 yr Degree", "4 yr Degree", "Advanced Degree", "College", "Primary", "Secondary", "Unknown"])

# Birth Year input
birth_year = st.sidebar.number_input("Birth Year", min_value=1900, max_value=2023, value=1990)

# Dependency Count input
dependency_count = st.sidebar.number_input("Dependency Count", min_value=0, value=1)

# Total Sales input
total_sales = st.sidebar.number_input("Total Sales", min_value=0, value=20000)


#----

if st.button("Predict"):
    # Create a dictionary for one-hot encoding
    input_data_dict = {
        "CD_GENDER_F": [1.0 if gender == "F" else 0.0],
        "CD_GENDER_M": [1.0 if gender == "M" else 0.0],
        "CD_MARITAL_STATUS_D": [1.0 if marital_status == "D" else 0.0],
        "CD_MARITAL_STATUS_M": [1.0 if marital_status == "M" else 0.0],
        "CD_MARITAL_STATUS_S": [1.0 if marital_status == "S" else 0.0],
        "CD_MARITAL_STATUS_U": [1.0 if marital_status == "U" else 0.0],
        "CD_MARITAL_STATUS_W": [1.0 if marital_status == "W" else 0.0],
        "CD_CREDIT_RATING_GOOD": [1.0 if credit_rating == "Good" else 0.0],
        "CD_CREDIT_RATING_HIGHRISK": [1.0 if credit_rating == "High Risk" else 0.0],
        "CD_CREDIT_RATING_LOWRISK": [1.0 if credit_rating == "Low Risk" else 0.0],
        "CD_EDUCATION_STATUS_2YRDEGREE": [1.0 if education_status == "2 yr Degree" else 0.0],
        "CD_EDUCATION_STATUS_4YRDEGREE": [1.0 if education_status == "4 yr Degree" else 0.0],
        "CD_EDUCATION_STATUS_ADVANCEDDEGREE": [1.0 if education_status == "Advanced Degree" else 0.0],
        "CD_EDUCATION_STATUS_COLLEGE": [1.0 if education_status == "College" else 0.0],
        "CD_EDUCATION_STATUS_PRIMARY": [1.0 if education_status == "Primary" else 0.0],
        "CD_EDUCATION_STATUS_SECONDARY": [1.0 if education_status == "Secondary" else 0.0],
        "CD_EDUCATION_STATUS_UNKNOWN": [1.0 if education_status == "Unknown" else 0.0],
        "C_BIRTH_YEAR": [birth_year],
        "CD_DEP_COUNT": [dependency_count],
        "TOTAL_SALES" : [total_sales]
    }

    # Create a DataFrame from the input data
    input_data_df = pd.DataFrame(input_data_dict)
    print(input_data_df)

    # Connect to Snowflake
    connection = connect(
        user='JArjun0207',
        password='Jarjun123',
        account='grfxvwc-nz59991',
        warehouse='FE_AND_INFERENCE_WH',
        database='tpcds_xgboost',
        schema='demo'
    )

    # Call Snowflake UDF
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT TPCDS_PREDICT_CLV({','.join(map(str, input_data_df.values[0]))})")
        prediction = cursor.fetchone()[0]

    st.write(f"Predicted Total Sales: {prediction}")



# Add a brief description
st.sidebar.markdown("---")
st.sidebar.write("This app predicts customer total sales based on the input parameters. Click 'Predict' to see the result.")


# Add a footer
st.sidebar.markdown("---")
st.sidebar.write("Powered by SNOWFLAKE")

# Optionally, you can add CSS for styling or use Streamlit's built-in themes to further enhance the appearance of your app.
