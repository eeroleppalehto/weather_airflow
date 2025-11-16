import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# TODO: Currently loading daily/monthly CSVs from transform task, which may include invalid data.
#       Should use validated data (df_validated_path) and recalculate daily/monthly aggregates?



def load_weather(**kwargs):
    ti = kwargs["ti"]

    # Pull CSV paths from XCom
    validated_path = ti.xcom_pull(task_ids="validate_task", key="df_validated_path")
    daily_path = ti.xcom_pull(task_ids="transform_task", key="Daily_averages_file_path")
    monthly_path = ti.xcom_pull(task_ids="transform_task", key="monthly_averages_file_path")

    """
    invalid_path = ti.xcom_pull(task_ids="validate_task", key="invalid_rows_path")
    outlier_path = ti.xcom_pull(task_ids="validate_task", key="outlier_rows_path")
    """

    engine = create_engine("postgresql+psycopg2://postgres:1063@localhost:5432/historical_weather")

    # ==========
    # LOAD DAILY WEATHER
    # ==========
    if daily_path:
        daily_df = pd.read_csv(daily_path)

        # Rename according to SQL schema
        daily_df = daily_df.rename(columns={
            "Formatted Date": "formatted_date",
            "Temperature (C)": "avg_temperature_c",
            "Humidity": "avg_humidity",
            "Wind Speed (km/h)": "avg_wind_speed_kmh"
        })

        # function read_and_index_data sets formatted_date as index > change back to column
        # remove df.set_index()?
        if "Unnamed: 0" in daily_df.columns:
            daily_df.rename(columns={"Unnamed: 0": "formatted_date"}, inplace=True)

        """
        # Add missing columns
        daily_df["precip_type"] = None
        daily_df["apparent_temperature_c"] = None
        daily_df["visibility_km"] = None
        daily_df["pressure_millibars"] = None
        daily_df["wind_strength"] = None
        daily_df["wind_speed_kmh"] = None
        """
    
        daily_df.to_sql(
        'daily_weather',
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
        )


    # ==========
    # LOAD MONTHLY WEATHER
    # ==========
    if monthly_path:
        monthly_df = pd.read_csv(monthly_path)

        # Rename according to SQL schema
        monthly_df = monthly_df.rename(columns={
            "Temperature (C)": "avg_temperature_c",
            "Humidity": "avg_humidity",
            "Wind Speed (km/h)": "avg_wind_speed_kmh",
            "Visibility (km)": "avg_visibility_km",
            "Pressure (millibars)": "avg_pressure_millibars"
        })

        # SQL table has DATE datatype - YearMonth is period
        # do this in transform task?
        if "YearMonth" in monthly_df.columns:
            monthly_df["month"] = pd.to_datetime(monthly_df["YearMonth"].astype(str))
            monthly_df.drop(columns=["YearMonth"], inplace=True)

        # Placeholder
        monthly_df["mode_precip_type"] = None

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