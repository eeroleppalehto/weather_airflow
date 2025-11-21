from pathlib import Path
from datetime import datetime, timedelta
import requests
import io
from zipfile import ZipFile

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from sqlalchemy import create_engine

from pandas.api.types import is_numeric_dtype

from util.extract import extract
from util.clean import cleanse_data
from util.transform import (
    read_and_index_data,
    generate_daily_averages,
    convert_kmh_to_ms,
    add_wind_strength_column,
    monthly_aggregation_mean_and_mode
)
from util.validate import (
    drop_missing_critical,
    validate_ranges,
    detect_outliers,
)

from util.load import store_daily_aggregates, store_monthly_aggregates



default_args = {
    "owner": "team_trio",
    "start_date": datetime(2025, 11, 9),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "test_weather_dag_2",
    tags=["rm"],
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False
)

TMP_DIR = Path().parent / "tmp"
KAGGLE_API_URL = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"  

def extract_func(**kwargs):
    response = requests.get(KAGGLE_API_URL, stream=True)
    bytes_io = io.BytesIO(response.content)
    path = TMP_DIR

    with ZipFile(bytes_io, "r") as zip_file:
        file_name = zip_file.filelist[0].filename
        zip_file.extractall(path)

    ti: TaskInstance = kwargs["ti"]

    file_path = TMP_DIR / file_name

    ti.xcom_push(key="extract_file_path", value=str(file_path))

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_func,
    dag=dag
)


def clean_func(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    extract_file_path = ti.xcom_pull(task_ids="extract_task", key="extract_file_path")

    df = pd.read_csv(extract_file_path)
    # df = cleanse_data(df)

    critical_columns = [
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Wind Bearing (degrees)",
        "Visibility (km)",
        "Loud Cover",
        "Pressure (millibars)",
    ]

    df = df.dropna(subset=critical_columns)

    df = df.drop_duplicates()

    df["is_valid_temp_c"] = df["Temperature (C)"].between(-50.0, 50.0)
    df["is_valid_humidity"] = df["Humidity"].between(0.0, 1.0)
    df["is_valid_wind_speed"] = df["Wind Speed (km/h)"] >= 0

    df = df[df["is_valid_temp_c"]]
    df = df[df["is_valid_humidity"]]
    df = df[df["is_valid_wind_speed"]]

    df = df.drop(columns=["is_valid_temp_c", "is_valid_humidity", "is_valid_wind_speed"])

    clean_data_file_name = "clean_weather_data.csv"
    clean_data_file_path = TMP_DIR / clean_data_file_name
    df.to_csv(clean_data_file_path, index=False)

    ti.xcom_push(key="clean_file_path", value=str(clean_data_file_path))


clean_task = PythonOperator(
    task_id="clean_task",
    python_callable=clean_func,
    dag=dag
)

def categorize_wind_strength(wind_speed):
#Categorize wind speed

    if wind_speed <= 1.5:
        return "calm"
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

def transform_func(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    extract_file_path = ti.xcom_pull(task_ids="clean_task", key="clean_file_path")
    df = read_and_index_data(extract_file_path)

    #---TRANSFORM---    
    # df = convert_kmh_to_ms(df)
    # df = add_wind_strength_column(df)

    df['Wind Speed (m/s)'] = df['Wind Speed (km/h)'] / 3.6


    df['Wind Strength'] = df['Wind Speed (m/s)'].apply((categorize_wind_strength))

    transformed_file_name = "transformed_weather_data.csv"
    transformed_file_path = TMP_DIR / transformed_file_name
    df.to_csv(transformed_file_path)

    #---DAILY---
    # daily_averages_df = generate_daily_averages(df)

    df_slice = df[["Humidity", "Temperature (C)", "Wind Speed (km/h)"]]
    daily_averages_df = df_slice.resample("D").mean()
    daily_agg_file_name = "daily_agg.csv"
    daily_agg_path = TMP_DIR / daily_agg_file_name
    daily_averages_df.to_csv(daily_agg_path)
    
    #---MONTHLY---
    monthly_mode_df = df[["Precip Type"]].resample("M").apply(lambda x: pd.Series.mode(x, dropna=False))

    # Aggregate mean values
    monthly_mean_df = df[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']].resample("M").mean()

    # Join the two seperate Dataframes together
    month_df = monthly_mean_df.join(monthly_mode_df)

    monthly_agg_file_name = "monthly_agg.csv"
    monthly_agg_path = TMP_DIR / monthly_agg_file_name
    month_df.to_csv(monthly_agg_path)

    ti.xcom_push(key="transformed_file_path", value=str(transformed_file_path))
    ti.xcom_push(key="daily_file_path", value=str(daily_agg_path))
    ti.xcom_push(key="monthly_file_path", value=str(monthly_agg_path))

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform_func,
    dag=dag
)

def validate_weather(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    #Execute validations in the correct order.
    #Pull transformed df
    path = ti.xcom_pull(task_ids="transform_task", key="transformed_file_path")
    print("Path: ",path)
    df = pd.read_csv(path)

    #Drop rows with missing data.
    df = drop_missing_critical(df)
    critical_columns = [
        'Summary',
        'Precip Type',
        'Temperature (C)',
        'Apparent Temperature (C)',
        'Humidity',
        'Wind Speed (km/h)',
        'Wind Bearing (degrees)',
        'Visibility (km)',
        'Pressure (millibars)',
        'Daily Summary'
    ]

    new_columns = [
        'Wind Speed (m/s)',
        'Wind Strength'
    ]

    df = df.dropna(subset=critical_columns + new_columns)
    
    #Range validation and saving invalid rows to csv
    invalid_temp = ~df['Temperature (C)'].between(-50, 50)
    invalid_humidity = ~df['Humidity'].between(0, 1)
    invalid_wind_kmh = df['Wind Speed (km/h)'] < 0
    invalid_wind_ms = df['Wind Speed (m/s)'] < 0

    invalid_rows = df[invalid_temp | invalid_humidity | invalid_wind_kmh | invalid_wind_ms]

    df = df.drop(index=invalid_rows.index)


    # df, invalid_rows = validate_ranges(df)
    invalid_path = "/tmp/invalid_rows.csv"
    invalid_rows.to_csv(invalid_path, index=False)

    #Outlier detection and saving invalid rows to csv
    numeric_columns= ["Temperature (C)", "Humidity", "Wind Speed (m/s)", "Pressure (millibars)", "Visibility (km)"]
   
    outlier_path = "/tmp/outliers.csv"

    lower_q=0.01
    upper_q=0.99

    mask = pd.Series(False, index=df.index)

    for col in numeric_columns:
        if not is_numeric_dtype(df[col]):
            continue
        
        lower = df[col].quantile(lower_q)
        upper = df[col].quantile(upper_q)

        mask |= (df[col] < lower) | (df[col] > upper)

    outlier_rows = df[mask]

    #Save outliers to csv if any.
    if not outlier_rows.empty:
        outlier_rows.to_csv(outlier_path, index=False)
        print(f"Outliers logged to: {outlier_path}")
    else:
        print("No outliers detected.")

    #Keep the non-outlier data in the dataframe
    df = df[~mask]

    #Save the validated dataframe to a csv file
    validated_path = "/tmp/validated_weather.csv"
    df.to_csv(validated_path, index=False)

    #Push validated dataframe, invalid rows, and outlier rows to xcom for pulling in load task.
    ti.xcom_push(key='df_validated_path', value=validated_path)
    ti.xcom_push(key='invalid_rows_path', value=invalid_path)
    ti.xcom_push(key='outlier_rows_path', value=outlier_path)
    return "Validation complete."
    

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=validate_weather,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True
)

def load_weather(**kwargs):
    ti = kwargs["ti"]

    # Pull CSV paths from XCom
    daily_path = ti.xcom_pull(task_ids="transform_task", key="daily_file_path")
    monthly_path = ti.xcom_pull(task_ids="transform_task", key="monthly_file_path")

    """
    invalid_path = ti.xcom_pull(task_ids="validate_task", key="invalid_rows_path")
    outlier_path = ti.xcom_pull(task_ids="validate_task", key="outlier_rows_path")
    """

    engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/weather")

    daily_df = pd.read_csv(daily_path)

    daily_df = daily_df.rename(columns={
        "Formatted Date": "formatted_date",
        "Temperature (C)": "avg_temperature_c",
        "Humidity": "avg_humidity",
        "Wind Speed (km/h)": "avg_wind_speed_kmh"
    })

    daily_df.to_sql(
        'daily_weather',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )


    monthly_df = pd.read_csv(monthly_path)


    monthly_df = monthly_df.rename(columns={
        "Formatted Date": "month",
        "Temperature (C)": "avg_temperature_c",
        "Humidity": "avg_humidity",
        "Wind Speed (km/h)": "avg_wind_speed_kmh",
        "Visibility (km)": "avg_visibility_km",
        "Pressure (millibars)": "avg_pressure_millibars",
        "Precip Type": "mode_precip_type"
    })

    monthly_df = monthly_df.drop(columns="avg_wind_speed_kmh")

    monthly_df.to_sql(
        'monthly_weather',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    return "Load complete."



load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

extract_task >> clean_task >>transform_task >> validate_task >> load_task
