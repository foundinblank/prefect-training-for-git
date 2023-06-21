import httpx  # requests capability, but can work with async
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def fetch_temp(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def fetch_pp(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="precipitation_probability"),
    )
    most_recent_pp = float(weather.json()["hourly"]["precipitation_probability"][0])
    return most_recent_pp

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def fetch_humidity(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="relativehumidity_2m"),
    )
    most_recent_humidity = float(weather.json()["hourly"]["relativehumidity_2m"][0])
    print(f"CACHE EXPIRED, UPDATING CACHE WITH NEW VALUE: {most_recent_humidity}") # Should not print if the result is cached and has not expired yet :-D 
    return most_recent_humidity

@task
def print_weather_report(temp, humidity, precipitation):
    print(f"Current temp: {temp}")
    print(f"Current humidity: {humidity}")
    print(f"Current precipitation probability: {precipitation}")

# @task
# def save_weather(temp: float):
#     with open("weather.csv", "w+") as w:
#         w.write(str(temp))
#     return "Successfully wrote temp"

@flow(retries=2, log_prints=True, persist_result=True)
def weather_pipeline_102(lat: float, lon: float):
    temp = fetch_temp(lat, lon)
    humidity = fetch_humidity(lat, lon)
    precipitation = fetch_pp(lat, lon)
    print_weather_report(temp, humidity, precipitation)


if __name__ == "__main__":
    weather_pipeline_102(32.72, -117.16)