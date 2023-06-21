import httpx  # requests capability, but can work with async
from prefect import flow, taskexport

@task
def fetch_temp(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp

@task
def fetch_pp(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="precipitation_probability"),
    )
    most_recent_pp = float(weather.json()["hourly"]["precipitation_probability"][0])
    return most_recent_pp

@task
def fetch_humidity(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="relativehumidity_2m"),
    )
    most_recent_humidity = float(weather.json()["hourly"]["relativehumidity_2m"][0])
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

@flow
def pipeline(lat: float, lon: float):
    temp = fetch_temp(lat, lon)
    humidity = fetch_humidity(lat, lon)
    precipitation = fetch_pp(lat, lon)
    print_weather_report(temp, humidity, precipitation)


if __name__ == "__main__":
    pipeline(32.72, -117.16)