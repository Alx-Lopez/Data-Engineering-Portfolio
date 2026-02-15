import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub


class ParseWeatherData(beam.DoFn):
    def process(self, element):
        """Parse incoming Pub/Sub messages into structured dicts for BigQuery."""
        try:
            # Decode and parse JSON message
            message = json.loads(element.decode("utf-8"))
            print(f"🟢 Received Pub/Sub message: {message}")

            # Extract data with fallbacks
            city = message.get("city", "Unknown")
            temperature = message.get("temperature") or message.get("current_weather", {}).get("temperature")
            windspeed = message.get("windspeed") or message.get("current_weather", {}).get("windspeed")
            timestamp = message.get("timestamp") or message.get("current_weather", {}).get("time")

            if not (temperature and windspeed and timestamp):
                print(f"⚠️ Skipping message due to missing fields: {message}")
                return

            # Convert safely
            try:
                temperature = float(temperature)
                windspeed = float(windspeed)
            except ValueError:
                print(f"⚠️ Invalid numeric values in message: {message}")
                return

            record = {
                "city": city,
                "temperature": temperature,
                "windspeed": windspeed,
                "timestamp": timestamp,
            }

            print(f"✅ Parsed record ready for BigQuery: {record}")
            yield record

        except Exception as e:
            print(f"❌ Error parsing message: {e}")
            return []


def run():
    # ----------------------------
    # 1️⃣ Set pipeline options
    # ----------------------------
    options = PipelineOptions(save_main_session=True, streaming=True)
    google_cloud_options = options.view_as(GoogleCloudOptions)

    google_cloud_options.project = "ramp-up-pro"
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = "gs://demo-test-bckt/staging"
    google_cloud_options.temp_location = "gs://demo-test-bckt/temp"
    google_cloud_options.job_name = "weather-streaming-to-bq-588296"

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = "DataflowRunner"
    standard_options.streaming = True

    # ----------------------------
    # 2️⃣ BigQuery Schema
    # ----------------------------
    table_schema = {
        "fields": [
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "temperature", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "windspeed", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    }

    # ----------------------------
    # 3️⃣ Define I/O Paths
    # ----------------------------
    input_subscription = "projects/ramp-up-pro/subscriptions/weathertopic-sub"
    output_table = "ramp-up-pro.weather_dataset.weather_data"

    # ----------------------------
    # 4️⃣ Build the Pipeline
    # ----------------------------
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Pub/Sub" >> ReadFromPubSub(subscription=input_subscription)
            | "Parse and Clean Data" >> beam.ParDo(ParseWeatherData())
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()


"""
This Apache Beam pipeline reads weather data from a Pub/Sub subscription,
parses and cleans the data, and writes it to a BigQuery table.
It includes error handling for missing or invalid fields in the incoming messages.

python dataflow_weather_to_bq.py \
  --project=ramp-up-pro \
  --region=us-central1 \
  --runner=DataflowRunner \
  --temp_location=gs://demo-test-bckt/temp \
  --staging_location=gs://demo-test-bckt/staging \
  --input_subscription=projects/ramp-up-pro/subscriptions/weathertopic-sub \
  --output_table=ramp-up-pro.weather_dataset.weather_data

"""
