import requests
import json
import time
from kafka import KafkaProducer

# ✅ Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Real PeMS-aligned traffic station locations (rounded to 1 decimal)
# Selected 20 unique rounded stations for Bay Area coverage
stations = [
    {"region": "San Francisco", "lat": 37.8, "lon": -122.4},
    {"region": "Oakland", "lat": 37.8, "lon": -122.3},
    {"region": "San Jose", "lat": 37.3, "lon": -121.9},
    {"region": "Fremont", "lat": 37.6, "lon": -122.0},
    {"region": "Berkeley", "lat": 37.9, "lon": -122.3},
    {"region": "Hayward", "lat": 37.7, "lon": -122.1},
    {"region": "Sunnyvale", "lat": 37.4, "lon": -122.0},
    {"region": "Milpitas", "lat": 37.4, "lon": -121.9},
    {"region": "Santa Clara", "lat": 37.4, "lon": -121.9},
    {"region": "Palo Alto", "lat": 37.4, "lon": -122.1},
    {"region": "Redwood City", "lat": 37.5, "lon": -122.2},
    {"region": "Daly City", "lat": 37.7, "lon": -122.5},
    {"region": "Concord", "lat": 37.9, "lon": -122.0},
    {"region": "Walnut Creek", "lat": 37.9, "lon": -122.1},
    {"region": "Livermore", "lat": 37.7, "lon": -121.8},
    {"region": "Pleasanton", "lat": 37.7, "lon": -121.9},
    {"region": "Mountain View", "lat": 37.4, "lon": -122.1},
    {"region": "San Mateo", "lat": 37.6, "lon": -122.3},
    {"region": "Union City", "lat": 37.6, "lon": -122.0},
    {"region": "Alameda", "lat": 37.8, "lon": -122.3}
]

# ✅ Run forever: poll weather for each location
while True:
    for loc in stations:
        url = f"https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "current_weather": "true"
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "current_weather" in data:
                weather = data["current_weather"]
                message = {
                    "region": loc["region"],
                    "timestamp": weather["time"],
                    "location": {
                        "latitude": loc["lat"],
                        "longitude": loc["lon"]
                    },
                    "temperature": weather["temperature"],
                    "windspeed": weather["windspeed"],
                    "winddirection": weather["winddirection"],
                    "weathercode": weather["weathercode"]
                }

                print(f"🌦️ Streaming: {message}")
                producer.send("weather-data", value=message)
            else:
                print(f"⚠️ No weather data for {loc['region']}")

        except Exception as e:
            print(f"❌ Error for {loc['region']}: {e}")

        time.sleep(1)  # avoid rate limits (1 per second)

    # 🔁 wait 1 minute before next cycle
    time.sleep(60)


