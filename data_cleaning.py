import pandas as pd
from pathlib import Path

TEMP_PATH = Path() / "tmp"


def data_cleaning(file_name):
    file_path = TEMP_PATH / file_name
    
    df = pd.read_csv(file_path)
    
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], format="%Y-%m-%d %H:%M:%S.%f %z", errors="raise", utc=True)

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

    transformed_file_name = "transformedWeatherData.csv"
    transformed_file_path = TEMP_PATH / transformed_file_name
    df.to_csv(transformed_file_path, index=False)

    # validate erroneous data

    return transformed_file_name

def validate_pre_aggragated_data():...

if __name__ == "__main__":
    FILE_NAME = "weatherHistory.csv"
    data_cleaning(FILE_NAME)