"""

ETL pipeline for historical weather data

"""

# Import neccessary libraries

import requests
import io
import psycopg2
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from zipfile import ZipFile
from pathlib import Path
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# Define DAG

default_args = {
	'owner': 'turbo_team',
	'start_date': datetime(2025, 11, 21),
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

dag = DAG(
	'Historical_Weather_Teamwork_v2',
	default_args = default_args,
	schedule_interval = '@daily',
	catchup = False,
)


#==========
# EXTRACT
#==========

KAGGLE_API_URL = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"
TEMP_PATH = Path() / "tmp"
TEMP_PATH.mkdir(parents=True, exist_ok=True)

def extract_func(**kwargs):
    #Extract dataset from Kaggle API 
    response = requests.get(KAGGLE_API_URL, stream=True)
    bytes_io = io.BytesIO(response.content)

    #Handle any ZIP-files 
    with ZipFile(bytes_io, "r") as zip_file:
        filename = zip_file.filelist[0].filename
        zip_file.extractall(TEMP_PATH)

    ti: TaskInstance = kwargs["ti"]

    file_path = TEMP_PATH / filename

    ti.xcom_push(key="extract_file_path", value=str(file_path))


#==========
# CLEAN
#==========

def clean_func(**kwargs):
    ti = kwargs["ti"]
    extract_file_path = ti.xcom_pull(task_ids="extract_task", key="extract_file_path")

    df = pd.read_csv(extract_file_path)

    critical_columns = [
        "Precip Type",
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Wind Bearing (degrees)",
        "Visibility (km)",
        "Loud Cover",
        "Pressure (millibars)",
    ]

    #Remove missing data from critical columns
    df = df.dropna(subset=critical_columns)

    #Remove any duplicates
    df = df.drop_duplicates()

    #Check for and remove any erroneous data
    df["is_valid_temp_c"] = df["Temperature (C)"].between(-50.0, 50.0)
    df["is_valid_humidity"] = df["Humidity"].between(0.0, 1.0)
    df["is_valid_wind_speed"] = df["Wind Speed (km/h)"] >= 0

    df = df[df["is_valid_temp_c"]]
    df = df[df["is_valid_humidity"]]
    df = df[df["is_valid_wind_speed"]]

    df = df.drop(columns=["is_valid_temp_c", "is_valid_humidity", "is_valid_wind_speed"])

    clean_data_file_name = "clean_weather_data.csv"
    clean_file_path = TEMP_PATH / clean_data_file_name
    df.to_csv(clean_file_path, index=False)

    ti.xcom_push(key="clean_file_path", value=str(clean_file_path))


#==========
# TRANSFORM
#==========

def categorize_wind_strength(wind_speed):
#Categorize wind speed
    if wind_speed <= 1.5:
        return "Calm"
    elif wind_speed <= 3.3:
        return "Light Air"
    elif wind_speed <= 5.4:
        return "Light Breeze"
    elif wind_speed <= 7.9:
        return "Gentle Breeze"
    elif wind_speed <= 10.7:
        return "Moderate Breeze"
    elif wind_speed <= 13.8:
        return "Fresh Breeze"
    elif wind_speed <= 17.1:
        return "Strong Breeze"
    elif wind_speed <= 20.7:
        return "Near Gale"
    elif wind_speed <= 24.4:
        return "Gale"
    elif wind_speed <= 28.4:
        return "Strong Gale"
    elif wind_speed <= 32.6:
        return "Storm"
    else:
        return "Violent Storm"

def transform_weather(**kwargs):
    ti = kwargs["ti"]
    clean_file_path = ti.xcom_pull(task_ids="clean_task", key="clean_file_path")
    df = pd.read_csv(clean_file_path)

    # Convert Formatted Date to a date format & set as index
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], 
                                            format="%Y-%m-%d %H:%M:%S.%f %z", 
                                            errors="coerce", 
                                            utc=True)
    df = df.set_index("Formatted Date")

    # Convert wind speed from km/h to m/s and categorize the instance 
    # of windspeed to different levels of wind strength
    df['Wind Speed (m/s)'] = df['Wind Speed (km/h)'] / 3.6
    df['Wind Strength'] = df['Wind Speed (m/s)'].apply((categorize_wind_strength))

    #--- DAILY AVERAGES ---
    
    #Aggregate daily mean values
    daily_averages_df = df.resample("D").agg({
        "Temperature (C)": "mean",
        "Apparent Temperature (C)": "mean",
        "Humidity": "mean",
        "Wind Speed (km/h)": "mean",
        "Visibility (km)": "mean",
        "Pressure (millibars)": "mean"
    }).reset_index()

    #Aggregate daily mode values
    daily_mode_df = df.resample("D").agg({
        "Precip Type": lambda x: x.mode(dropna=False).iloc[0] if not x.mode(dropna=False).empty else None,
        "Wind Strength": lambda x: x.mode(dropna=False).iloc[0] if not x.mode(dropna=False).empty else None
    }).reset_index()

    # Merge separate DataFrames
    daily_df = daily_averages_df.merge(daily_mode_df, on="Formatted Date")


    #--- MONTHLY AVERAGES ---
    
    # Aggregate Precip Type mode values 
    monthly_mode_df = df[["Precip Type"]].resample("M").apply(lambda x: pd.Series.mode(x, dropna=False))
    monthly_mode_df.rename(columns={"Precip Type": "Mode Precip Type"}, inplace=True)
    
    # Aggregate monthly mean values
    monthly_mean_df = df[['Temperature (C)', 
                        'Humidity',  
                        'Visibility (km)', 
                        'Pressure (millibars)', 
                        'Apparent Temperature (C)'
                        ]].resample("M").mean()

    # Join the two seperate Dataframes together                    
    month_df = monthly_mean_df.join(monthly_mode_df) 

    # Create month column (last day of month)
    month_df = month_df.copy()
    month_df["month"] = month_df.index.normalize()

    # Daily and monthly averages paths
    daily_averages_path = '/tmp/daily_averages.csv'
    monthly_averages_path = '/tmp/monthly_averages.csv'

    # Daily and monthly DataFrames to csv files
    daily_df.to_csv(daily_averages_path, index=False)
    month_df.to_csv(monthly_averages_path, index=False)

    # Changing the date index back to a column called "Formatted Date"
    df = df.copy()
    df['Formatted Date'] = df.index  #First copy the datetime index into a column.
    df.reset_index(drop=True, inplace=True)  #Then drop the index.
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])  #Lastly ensure that "Formatted Date" column has proper dtype of datetime.

    #Transformed dataframe to csv file for easier pushing via xcom.
    transformed_path = "/tmp/transformed_df.csv"
    df.to_csv(transformed_path, index=False)

    #Xcom pushing the transformed df, monthly averages, and daily averages.
    ti.xcom_push(key='daily_averages_file_path', value=daily_averages_path)
    ti.xcom_push(key='monthly_averages_file_path', value=monthly_averages_path)
    ti.xcom_push(key='transformed_file_path', value=transformed_path)


#==========
# VALIDATE
#==========

def check_missing_values(df, critical_columns):
    #Check for missing values without dropping.
    missing_cases = []

    for col in critical_columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            missing_cases.append(f"{col}: {missing_count} missing values")

    if missing_cases:
        return False, missing_cases
    
    return True, []

def validate_ranges(df):
    #Checks the ranges without dropping invalid values. In case of invalid rows, will print the sum of invalid rows in any column.

    problem_cases = []

    # Temperature
    invalid_temp = ~df['Temperature (C)'].between(-50, 50)
    if invalid_temp.any():
        problem_cases.append(f"Invalid Temperature values: {invalid_temp.sum()} rows")

    # Humidity
    invalid_humidity = ~df['Humidity'].between(0, 1)
    if invalid_humidity.any():
        problem_cases.append(f"Invalid Humidity values: {invalid_humidity.sum()} rows")

    # Wind
    invalid_wind_kmh = df['Wind Speed (km/h)'] < 0
    if invalid_wind_kmh.any():
        problem_cases.append(f"Invalid Wind Speed (km/h): {invalid_wind_kmh.sum()} rows")

    invalid_wind_ms = df['Wind Speed (m/s)'] < 0
    if invalid_wind_ms.any():
        problem_cases.append(f"Invalid Wind Speed (m/s): {invalid_wind_ms.sum()} rows")

    # return results
    if problem_cases:
        return False, problem_cases
    return True, []


def detect_outliers(df, columns, outlier_path, lower_q=0.01, upper_q=0.99):
    #Should skip non-numeric columns in the checking for outliers
    mask = pd.Series(False, index=df.index)

    for col in columns:
        if not is_numeric_dtype(df[col]):
            continue
        
        lower = df[col].quantile(lower_q)
        upper = df[col].quantile(upper_q)

        mask |= (df[col] < lower) | (df[col] > upper)

    outlier_rows = df[mask]

    #Save outliers to csv if any.
    if not outlier_rows.empty:
        outlier_rows.to_csv(outlier_path, index=False)
        print(f"Outliers detected: {len(outlier_rows)} rows. Outliers saved to: {outlier_path}")
    else:
        print("No outliers detected.")

    
    return mask.any(), outlier_rows

def validate_weather(**kwargs):
#Execute validations in the correct order
    ti = kwargs["ti"]

    #Pull transformed df
    path = ti.xcom_pull(task_ids="transform_task", key="transformed_file_path")
    df = pd.read_csv(path)

    #Required columns for checking for missing values. Make sure that all of the columns exist in the dataframe.
    critical_columns = [
        'Summary',
        'Precip Type',
        'Temperature (C)',
        'Apparent Temperature (C)',
        'Humidity',
        'Wind Speed (km/h)',
        'Wind Speed (m/s)',
        'Wind Strength',
        'Wind Bearing (degrees)',
        'Visibility (km)',
        'Loud Cover',
        'Pressure (millibars)',
        'Daily Summary'
    ]

    #Missing value validation
    missing_ok, missing_msgs = check_missing_values(df, critical_columns)

    #Range validation
    ranges_ok, range_msgs = validate_ranges(df)

    #Outlier validation
    numeric_columns= ["Temperature (C)", "Humidity", "Wind Speed (m/s)", "Pressure (millibars)", "Visibility (km)"]
    outlier_path = "/tmp/outliers.csv"
    outliers_found, outliers = detect_outliers(df, numeric_columns, outlier_path)

    #Setting fail conditions for not progressing to load task.
    error_messages = []

    if not missing_ok:
        error_messages.extend([f"Missing values detected: {msg}" for msg in missing_msgs])
        
    if not ranges_ok:
        error_messages.extend(range_msgs)

    if error_messages:
        print("\nVALIDATION FAILED:")
        for msg in error_messages:
            print(" - " + msg)
        raise ValueError("Validation failed, see error messages above.")

    #If no failures, save validated df and continue with pushing paths to xcom.
    validated_path = "/tmp/validated_weather.csv"
    df.to_csv(validated_path, index=False)

    ti.xcom_push(key='df_validated_path', value=validated_path)
    ti.xcom_push(key='outlier_rows_path', value=outlier_path)

    print("Validation successful, proceeding to load task.")
    return "Validation complete."


#==========
# LOAD
#==========

def load_weather(**kwargs):
    ti = kwargs["ti"]

    # Pull csv filepaths from transformation task via XCom
    daily_path = ti.xcom_pull(task_ids="transform_task", key="daily_averages_file_path")
    monthly_path = ti.xcom_pull(task_ids="transform_task", key="monthly_averages_file_path")

    # Create engine for database connection
    engine = create_engine("postgresql+psycopg2://postgres:1063@localhost:5432/historical_weather")


#---LOAD DAILY WEATHER---
    daily_df = pd.read_csv(daily_path)

    # Rename according to SQL schema
    daily_df = daily_df.rename(columns={
        "Formatted Date": "formatted_date",
        "Precip Type": "precip_type",
        "Apparent Temperature (C)": "apparent_temperature_c",
        "Visibility (km)": "visibility_km",
        "Pressure (millibars)": "pressure_millibars",
        "Wind Strength": "wind_strength",
        "Humidity": "avg_humidity",
        "Temperature (C)": "avg_temperature_c",
        "Wind Speed (km/h)": "avg_wind_speed_kmh"
    })

    #Convert DataFrame to SQL table
    daily_df.to_sql(
        'daily_weather',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )

#---LOAD MONTHLY WEATHER---
    monthly_df = pd.read_csv(monthly_path)

    # Rename according to SQL schema
    monthly_df = monthly_df.rename(columns={
        "Temperature (C)": "avg_temperature_c",
        "Apparent Temperature (C)": "avg_apparent_temperature_c",
        "Humidity": "avg_humidity",
        "Visibility (km)": "avg_visibility_km",
        "Pressure (millibars)": "avg_pressure_millibars",
        "Mode Precip Type": "mode_precip_type"
    })

    #Convert DataFrame to SQL table
    monthly_df.to_sql(
        'monthly_weather',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    return "Load complete."


#==========
# TASKS
#==========

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_func,
    dag=dag
)

clean_task = PythonOperator(
    task_id="clean_task",
    python_callable=clean_func,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)
 
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=validate_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

extract_task >> clean_task >> transform_task >> validate_task >> load_task