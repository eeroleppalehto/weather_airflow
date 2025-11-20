import pandas as pd
from pandas import DataFrame

def cleanse_data(df: DataFrame) -> DataFrame:
    """Clean Dataframe by dropping duplicates and dropping row with NaN
    values in the critical columns. Also discard any rows containing 
    outlier or erroneous data

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

    df["is_valid_temp_c"] = df["Temperature (C)"].between(-50.0, 50.0)
    df["is_valid_humidity"] = df["Humidity"].between(0.0, 1.0)
    df["is_valid_wind_speed"] = df["Wind Speed (km/h)"] >= 0

    df = df[df["is_valid_temp_c"]]
    df = df[df["is_valid_humidity"]]
    df = df[df["is_valid_wind_speed"]]

    df = df.drop(columns=["is_valid_temp_c", "is_valid_humidity", "is_valid_wind_speed"])

    return df

def monthly_aggregation_mean_and_mode(df: DataFrame) -> DataFrame:
    """Generate a pandas DataFrame with monthly aggregates for the mean values of
    'Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)' columns
    and the mode of the "Precip Type" column.

    Args:
        df (DataFrame): Cleaned DataFrame with datetime index

    Returns:
        DataFrame: Monthly aggregate DataFrame
    """


    # Aggregate Precip Type mode values
    monthly_mode_df = df[["Precip Type"]].resample("M").apply(lambda x: pd.Series.mode(x, dropna=False))

    # Aggregate mean values
    monthly_mean_df = df[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']].resample("M").mean()

    # Join the two seperate Dataframes together
    month_df = monthly_mean_df.join(monthly_mode_df)

    return month_df



