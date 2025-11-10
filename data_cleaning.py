import pandas as pd
from pandas import DataFrame
from pathlib import Path


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

if __name__ == "__main__":
    TEMP_PATH = Path() / "tmp"
    FILE_NAME = "weatherHistory.csv"
    path_to_file = TEMP_PATH / FILE_NAME
    
    df = read_and_index_data(path_to_file)
    df = clean_data(df)
