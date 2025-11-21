import requests
import io
import pandas as pd
import psycopg2

from zipfile import ZipFile
from pathlib import Path
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine



#==========
# EXTRACT
#==========

KAGGLE_API_URL = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"
TEMP_PATH = Path() / "tmp"


def extract():
    response = requests.get(KAGGLE_API_URL, stream=True)
    bytes_io = io.BytesIO(response.content)

    with ZipFile(bytes_io, "r") as zip_file:
        filename = zip_file.filelist[0].filename
        zip_file.extractall(TEMP_PATH)

    return filename

if __name__ == "__main__":
    extract()




#==========
# CLEAN
#==========

def read_and_index_data(path_to_file: Path | str):
    df = pd.read_csv(path_to_file)
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], format="%Y-%m-%d %H:%M:%S.%f %z", errors="raise", utc=True)

    df = df.set_index("Formatted Date")
    return df

def clean_data(df: DataFrame) -> DataFrame:
    """Clean Dataframe by dropping duplicates and dropping row with NaN
    values in the critical columns.

    Args:
        df (DataFrame): DataFrame with weather history data


    Returns:
        DataFrame: Cleaned Dataframe
    """
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

    return df

def validate_pre_aggragated_data(df: DataFrame) -> DataFrame:
    """Function that adds columns to the Dataframe that flag if the row value is valid.

    Args:
        df (DataFrame): Cleaned DataFrame with weather history data

    Returns:
        DataFrame: DataFrame with additional is_valid_temp_c, is_valid_humidity, is_valid_wind_speed
    """

    df["is_valid_temp_c"] = df["Temperature (C)"].between(-50.0, 50.0)
    df["is_valid_humidity"] = df["Humidity"].between(0.0, 1.0)
    df["is_valid_wind_speed"] = df["Wind Speed (km/h)"] > 0

    return df

def generate_daily_averages(df: DataFrame) -> DataFrame:
    """Generate a daily average report with Humidity, Temperature (C), Wind Speed (km/h) columns

    Args:
        df (DataFrame): Cleaned and validated DataFrame with weather history data

    Returns:
        DataFrame: Daily average report with Humidity, Temperature (C), Wind Speed (km/h)
    """

    valid_humidity = df[df["is_valid_humidity"]]
    valid_temparuture = df[df["is_valid_temp_c"]]
    valid_wind_speed = df[df["is_valid_wind_speed"]]

    daily_humidity = valid_humidity["Humidity"].resample("D").mean()
    daily_temparuture = valid_temparuture["Temperature (C)"].resample("D").mean()
    daily_wind_speed = valid_wind_speed["Wind Speed (km/h)"].resample("D").mean()

    data = {
        "Humidity": daily_humidity,
        "Temperature (C)": daily_temparuture,
        "Wind Speed (km/h)": daily_wind_speed
    }
    daily_averages_df = pd.DataFrame(data)

    return daily_averages_df

def get_monthly_precipitation_type(df: DataFrame) -> DataFrame:
    """Create DataFrame with mode of the precipitation type by month

    Args:
        df (DataFrame): Cleaned and validated DataFrame with weather history data


    Returns:
        DataFrame: DataFrame with mode of the precipitation type by month
    """
    monthly_df = df[["Precip Type"]].resample("ME").agg(pd.Series.mode)
    return monthly_df


if __name__ == "__main__":
    TEMP_PATH = Path() / "tmp"
    FILE_NAME = "weatherHistory.csv"
    path_to_file = TEMP_PATH / FILE_NAME
    
    df = read_and_index_data(path_to_file)
    df = clean_data(df)
    df = validate_pre_aggragated_data(df)

    daily_averages_df = generate_daily_averages(df)
    monthly_precipitation_type_df = get_monthly_precipitation_type(df)

    print("----------DF----------")
    print(df.head())
    print("----------DAILY DF----------")
    print(daily_averages_df.head())
    print("----------MONTHLY DF----------")
    print(monthly_precipitation_type_df.head())




#==========
# TRANSFORM
#==========

def read_and_index_data(path_to_file: Path | str):
    df = pd.read_csv(path_to_file)
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], format="%Y-%m-%d %H:%M:%S.%f %z", errors="raise", utc=True)

    df = df.set_index("Formatted Date")
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """Clean Dataframe by dropping duplicates and dropping row with NaN
    values in the critical columns.

    Args:
        df (DataFrame): DataFrame with weather history data


    Returns:
        DataFrame: Cleaned Dataframe
    """
    critical_columns = [
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Wind Bearing (degrees)",
        "Visibility (km)",
        "Cloud Cover",
        "Pressure (millibars)",
    ]

    df = df.dropna(subset=critical_columns)

    df = df.drop_duplicates()

    return df

def validate_pre_aggregated_data(df: DataFrame) -> DataFrame:
    """Function that adds columns to the Dataframe that flag if the row value is valid.

    Args:
        df (DataFrame): Cleaned DataFrame with weather history data

    Returns:
        DataFrame: DataFrame with additional is_valid_temp_c, is_valid_humidity, is_valid_wind_speed
    """

    df["is_valid_temp_c"] = df["Temperature (C)"].between(-50.0, 50.0)
    df["is_valid_humidity"] = df["Humidity"].between(0.0, 1.0)
    df["is_valid_wind_speed"] = df["Wind Speed (km/h)"] >= 0

    return df

def generate_daily_averages(df: DataFrame) -> DataFrame:
    """Generate a daily average report with Humidity, Temperature (C), Wind Speed (km/h) columns

    Args:
        df (DataFrame): Cleaned and validated DataFrame with weather history data

    Returns:
        DataFrame: Daily average report with Humidity, Temperature (C), Wind Speed (km/h)
    """

    valid_humidity = df[df["is_valid_humidity"]]
    valid_temparature = df[df["is_valid_temp_c"]]
    valid_wind_speed = df[df["is_valid_wind_speed"]]

    daily_humidity = valid_humidity["Humidity"].resample("D").mean()
    daily_temparature = valid_temparature["Temperature (C)"].resample("D").mean()
    daily_wind_speed = valid_wind_speed["Wind Speed (km/h)"].resample("D").mean()

    data = {
        "Humidity": daily_humidity,
        "Temperature (C)": daily_temparature,
        "Wind Speed (km/h)": daily_wind_speed
    }
    daily_averages_df = pd.DataFrame(data)

    return daily_averages_df

def get_monthly_precipitation_type(df: DataFrame) -> DataFrame:
    """Create DataFrame with mode of the precipitation type by month

    Args:
        df (DataFrame): Cleaned and validated DataFrame with weather history data


    Returns:
        DataFrame: DataFrame with mode of the precipitation type by month
    """
    monthly_df = df[["Precip Type"]].resample("ME").agg(pd.Series.mode)
    return monthly_df


if __name__ == "__main__":
    TEMP_PATH = Path() / "tmp"
    FILE_NAME = "weatherHistory.csv"
    path_to_file = TEMP_PATH / FILE_NAME
    
    df = read_and_index_data(path_to_file)
    df = clean_data(df)
    df = validate_pre_aggregated_data(df)

    daily_averages_df = generate_daily_averages(df)
    monthly_precipitation_type_df = get_monthly_precipitation_type(df)

    print("----------DF----------")
    print(df.head())
    print("----------DAILY DF----------")
    print(daily_averages_df.head())
    print("----------MONTHLY DF----------")
    print(monthly_precipitation_type_df.head())


# Wind strength categorization. Convert wind speed from km/h to m/s and categorize the instance of windspeed to different levels of wind strength.

def convert_kmh_to_ms(df):
    #Transform the windspeed from km/h to m/s
    df['Wind Speed (m/s)'] = df['Wind Speed (km/h)'] / 3.6
    return df

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

def add_wind_strength_column(df):
    #Create the wind strength column based on the converted wind speeds divided into categories.
    df['Wind Strength'] = df['Wind Speed (m/s)'].apply((categorize_wind_strength))
    return df

def calculate_monthly_averages(df):
    df['YearMonth'] = df.index.to_period('M')
    
    #Make a dataframe of the monthly data
    monthly_df = df.groupby('YearMonth').agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean'
    }).reset_index()

    monthly_averages_df = pd.DataFrame(monthly_df)

    return monthly_averages_df

def transform_weather(**kwargs):
    ti = kwargs["ti"]
    #Execute transformations in the correct order.
    #Pull extracted df
    df = ti.xcom_pull(task_ids="extract_task", key="df_extracted")

    #Transformations and new columns
    df = convert_kmh_to_ms(df)
    df = add_wind_strength_column(df)

    #Daily and monthly averages
    daily_df = generate_daily_averages(df)
    monthly_df = calculate_monthly_averages(df)

    #Daily and monthly averages paths
    daily_averages_path = '/tmp/daily_averages.csv'
    monthly_averages_path = '/tmp/monthly_averages.csv'

    #Daily and monthly to csv files
    daily_df.to_csv(daily_averages_path, index=False)
    monthly_df.to_csv(monthly_averages_path, index=False)

    #Changing the date index back to a column called "Formatted Date"
    df = df.copy()
    df['Formatted Date'] = df.index  #First copy the datetime index into a column.
    df.reset_index(drop=True, inplace=True)  #Then drop the index.
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])  #Lastly ensure that "Formatted Date" column has proper dtype of datetime.


    #Transformed dataframe to csv file for easier pushing via xcom.
    transformed_path = "/tmp/transformed_df.csv"
    df.to_csv(transformed_path, index=False)

    #Xcom pushing the transformed df, monthly averages, and daily averages.
    ti.xcom_push(key='Daily_averages_file_path', value=daily_averages_path)
    ti.xcom_push(key='monthly_averages_file_path', value=monthly_averages_path)
    ti.xcom_push(key='transformed_df_path', value=transformed_path)
    return




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
        print(f"Outliers logged to: {outlier_path}")
    else:
        print("No outliers detected.")

    
    return mask.any(), outlier_rows

def validate_weather(**kwargs):
    ti = kwargs["ti"]
    #Execute validations in the correct order.
    #Pull transformed df
    path = ti.xcom_pull(task_ids="transform_task", key="transformed_df_path")
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
        'Cloud Cover',
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

    if outliers_found:
        error_messages.append(f"Outliers detected: {len(outliers)} rows. Outliers saved to: {outlier_path}")

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

    # Pull CSV paths from XCom
    daily_path = ti.xcom_pull(task_ids="transform_task", key="Daily_averages_file_path")
    monthly_path = ti.xcom_pull(task_ids="transform_task", key="monthly_averages_file_path")

    engine = create_engine("postgresql+psycopg2://postgres:1063@localhost:5432/historical_weather")


# LOAD DAILY WEATHER
    if daily_path:
        daily_df = pd.read_csv(daily_path)

        # Rename according to SQL schema
        daily_df = daily_df.rename(columns={
            "Formatted Date": "formatted_date",
            "Precip Type": "precip_type",
            "Temperature (C)": "temperature_c",
            "Apparent Temperature (C)": "apparent_temperature_c",
            "Humidity": "humidity",
            "Wind Speed (km/h)": "wind_speed_kmh",
            "Visibility (km)": "visibility_km",
            "Pressure (millibars)": "pressure_millibars",
            "Wind Strength": "wind_strength",
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




# LOAD MONTHLY WEATHER
    if monthly_path:
        monthly_df = pd.read_csv(monthly_path)

        # Rename according to SQL schema
        monthly_df = monthly_df.rename(columns={
            "Temperature (C)": "avg_temperature_c",
            "Apparent Temperature (C)": "apparent_temperature_c",
            "Humidity": "avg_humidity",
            "Visibility (km)": "avg_visibility_km",
            "Wind Speed (km/h)": "avg_wind_speed_kmh",
            "Pressure (millibars)": "avg_pressure_millibars",
            "Precip Type": "mode_precip_type"
        })

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

#extract_task

#clean_task?
 
transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_weather,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True
)

validate_task = PythonOperator(
    task_id="validate_task",
    python_callable=validate_weather,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)


#  >> transform_task >> validate_task >> load_task