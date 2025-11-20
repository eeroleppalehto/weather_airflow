import pandas as pd
import psycopg2
from sqlalchemy import create_engine



def load_weather(**kwargs):
    ti = kwargs["ti"]

    # Pull CSV paths from XCom
    daily_path = ti.xcom_pull(task_ids="transform_task", key="Daily_averages_file_path")
    monthly_path = ti.xcom_pull(task_ids="transform_task", key="monthly_averages_file_path")

    engine = create_engine("postgresql+psycopg2://postgres:1063@localhost:5432/historical_weather")

    # ==========
    # LOAD DAILY WEATHER
    # ==========
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



    # ==========
    # LOAD MONTHLY WEATHER
    # ==========
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



load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_weather,
    dag = dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)