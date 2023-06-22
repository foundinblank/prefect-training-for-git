import httpx  # requests capability, but can work with async
from prefect import flow, task
from prefect_aws.s3 import S3Bucket
from prefect.blocks.notifications import SlackWebhook
from datetime import datetime

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
def create_filename():
    # create a filename based on the current time
    return f"weather_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv"

@task
def save_weather(temp: float, filename: str):
    with open(filename, "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"

@task
def upload_to_s3(filename: str):
    s3_bucket = S3Bucket.load("upload-to-s3-my-prefect-bucket")
    s3_bucket.upload_from_path(filename, filename)

@task
def notify_slack():
    slack_webhook_block = SlackWebhook.load("slack-dt8-devs")
    slack_webhook_block.notify("Hello from Prefect!")

@flow
def my_pipeline(lat: float, lon: float):
    temp = fetch_temp(lat, lon)
    csv_filename = create_filename()
    save_weather(temp, csv_filename)
    upload_to_s3(csv_filename)
    notify_slack()


if __name__ == "__main__":
    my_pipeline(32.72, -117.16)