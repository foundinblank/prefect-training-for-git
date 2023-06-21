import httpx
from datetime import datetime, timedelta

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact

WEATHER_MEASURES = "temperature_2m,relativehumidity_2m,rain,windspeed_10m"


@task(name="Fetch weather", retries=3, retry_delay_seconds=1)
def fetch_hourly_weather(lat: float, lon: float):
    """Query the open-meteo API for the forecast"""
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(
            latitude=lat,
            longitude=lon,
            hourly=WEATHER_MEASURES,
        ),
    )
    # Raise an exception if the request failed
    if weather.status_code != 200:
        raise Exception(f"Failed to fetch weather: {weather.text}")
    return weather.json()["hourly"]


@task(
    name="Save weather",
    persist_result=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def save_weather(weather: dict):
    """Save the weather data to a csv file"""
    logger = get_run_logger()

    # csv headers
    contents = f"time,{WEATHER_MEASURES}\n"
    # csv contents
    try:
        logger.debug("Parsing JSON contents to csv")
        for i in range(len(weather["time"])):
            contents += weather["time"][i]
            for measure in WEATHER_MEASURES.split(","):
                contents += "," + str(weather[measure][i])

            contents += "\n"
        logger.debug("Writing csv")
        with open("weather.csv", "w+") as w:
            w.write(contents)
        return "Successfully wrote csv"
    except Exception as e:
        return f"Failed to write csv: {e}"


@task(
    name="Log next forecast",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def log_forecast(weather: str, lat: float, lon: float):
    """Create a markdown file with the results of the next hour forecast"""
    log = f"# Weather in {lat}, {lon}\n\nThe forecast for the next hour as of {datetime.now()} is...\n\n"
    # Find the next hour
    try:
        next_hour = weather["time"].index(
            (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00")
        )
    except KeyError:
        # Default to the first hour
        next_hour = 0
    # Log the results
    for measure in WEATHER_MEASURES.split(","):
        log += f"- {measure}: {weather[measure][next_hour]}\n"

    # Create artifact
    create_markdown_artifact(
        key="weather-forecast",
        markdown=log,
        description="The forecast for the next hour",
    )
    
    # Save the data
    with open("most_recent_results.md", "w") as f:
        f.write(log)


@flow
def gemma_pipeline(lat: float, lon: float):
    """Main pipeline"""
    weather = fetch_hourly_weather(lat, lon)
    result = save_weather(weather)
    log_forecast(weather, lat, lon)
    print(lat)
    print(lon)
    return result


if __name__ == "__main__":
    gemma_pipeline(50.7913957952127, -1.9014254856972352)