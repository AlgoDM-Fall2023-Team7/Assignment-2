# Snowflake Machine Learning Project

This project consists of two parts, each involving the use of Snowflake and Streamlit to build and test various machine learning applications.

## Task 1

In this part, you will complete a tutorial and build a Streamlit app with two key use cases: forecasting and anomaly detection.

### Tutorial Completion
- Follow the tutorial: [Predict Ad Impressions with ML-Powered Analysis](https://quickstarts.snowflake.com/guide/predict_ad_impressions_with_ml_powered_analysis/index.html) and make sure to submit your work.

### Streamlit App Development
- Build a Streamlit app that allows you to interact with the data and perform two use cases: forecasting and anomaly detection.

## Task 2

This part involves testing three different machine learning applications that we discussed in class.

### Customer Lifetime Value Computation
- Implementation details can be found in the following resources:
  - [Medium Article Part 1](https://medium.com/snowflake/ml-on-snowflake-at-scale-with-snowpark-python-and-xgboost-c329c30c2feb)
  - [Medium Article Part 2](https://medium.com/snowflake/ml-on-snowflake-at-scale-with-snowpark-python-and-snowpark-ml-part-2-6491d72a9903)
  - [GitHub Repository](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/tpcds-customer-lifetime-value)

### Predict Customer Spend
- Implementation details can be found in the [GitHub Repository](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Predict%20Customer%20Spend)

### ROI Prediction
- Implementation details can be found in the [GitHub Repository](https://github.com/Snowflake-Labs/snowpark-python-demos/tree/main/Advertising-Spend-ROI-Prediction)

### Multipage Streamlit App
- Combine all three functions (Customer Lifetime Value Computation, Predict Customer Spend, and ROI Prediction) into a single Streamlit app with multiple pages.

## How to Run the Streamlit App
- Make sure to install the necessary dependencies for your Streamlit app. You can use `pip` to install them.
- Run the Streamlit app using the following command:
  ```bash
  streamlit run your_app_name.py
  ```

## Project Contributions

**Aishwarya SVS**

**1. Forecasting and Anamoly Detection Tutorial Completion** 
- Successfully completed the tutorial titled "Predict Ad Impressions with ML-Powered Analysis," meeting all the tutorial's requirements and objectives.
- Building a forecasting model to predict future values, enhancing our ability to make informed decisions based on anticipated ad impressions. 
- Developing a model for detecting anomalies in incoming data, providing a mechanism to identify irregularities or unexpected patterns in ad impression data, which can be critical for maintaining data quality and security.

**2. Streamlit App Development** 
- Developed a robust Streamlit application that empowers users to interact with data effectively. 
- Implemented two key use cases within the application: forecasting and anomaly detection, enhancing its utility and versatility.

**3. Technical Steps and Implications** 
- **Environment Setup**: Created a warehouse, a database, and a schema, ensuring the appropriate infrastructure for data analysis and model development. 
Analyst Role Configuration: Configured the analyst role with specific privileges, granting necessary access rights for database, schema, and warehouse usage. Also enabled the creation of tables, views, and machine learning models. 
- **Data Generation**: Simulated data generation, which emulated real-world ad impression patterns, including an upward trend and day-of-week effects, for forecasting purposes. This step involved the creation of data that resembled actual ad impression data. 
- **Data Verification**: Employed SQL queries and visualization tools to verify the generated data's alignment with intended patterns and properties. This allowed for a comprehensive understanding of data trends and anomalies, ensuring data quality and reliability for analysis.

**Arjun Janardhan**

**1. Linear Regression Model Implementation**
- Developed a Linear Regression model using the Scikit-Learn library to predict customer spending based on user activity data. 
- Model Training and Scoring: Conducted model training locally, ensuring that the model could effectively predict customer spending patterns. 
- Implemented model scoring within Snowflake using a User-Defined Function (UDF) created via Snowpark in a Jupyter Notebook. 

**2. Data Analysis and Preparation** 
- Performed data engineering tasks, including data analysis and data preparation, to ensure the dataset was ready for machine learning. 
- Utilized Python libraries such as pandas, numpy, matplotlib, and seaborn to analyze and manipulate data. 

**3. Integration with Snowpark and Streamlit** 
- Utilized Snowpark for Python for feature engineering and model deployment. 
- Developed an interactive web application using Streamlit to visualize the model's predictions and insights on customer spending. 

**4. Web Application for ROI Prediction** 
- Applied similar techniques to create a web application for predicting the Return on Investment (ROI) of advertising spend budgets across multiple channels, including search, video, social media, and email. 
- Utilized Snowpark for Python, Streamlit, and the scikit-learn library to develop this interactive web app for visualizing ROI based on advertising spend allocations

**Sanidhya Mathur**

**1. Data Prep and Feature Engineering for Customer Lifetime Value computation:**
- Aggregated sales data by customer across all channels, facilitating a comprehensive understanding of customer behavior. 
- Joined the aggregated data with customer dimension tables to extract valuable features, enabling deeper insights into customer patterns and preferences.
- Saved the resulting data to a feature store table for future use, ensuring easy accessibility and reusability. 

**2. Model Training Using a Stored Procedure:** 
- Utilized the Snowflake environment to train the provided machine learning model, leveraging Snowpark ML for streamlined model development. 
- Employed popular Python ML frameworks for model training, enhancing the model's capabilities and flexibility. 

**3. Deploying the Model for Batch Inference** 
- Created a User-Defined Function (UDF) for batch inference, utilizing the trained model to make predictions on specified datasets. 
- Executed batch inference on the data, generating predictions at scale. 
- Saved the inference results to a table for analysis and decision-making. 

**4. Integrating the Developments with Streamlit** 
- Built a user-friendly dashboard within Streamlit, for enabling users to interact with the trained machine learning model and access inference results seamlessly for Customer Lifetime Value computation. 
- Utilized Streamlit for visualizing the model's output and prediction results, making complex information more interpretable and understandable for end-users.
- Addressed common question for retailers: "What is the value of a customer across all sales channels?" 



