import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# ✅ Load cleaned traffic data
csv_path = '/Users/spartan/Desktop/pems_5min_cleaned_with_location.csv'
df = pd.read_csv(csv_path)

# ✅ Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Stream each row with live timestamp (UTC, rounded to minute)
for index, row in df.iterrows():
    live_time = datetime.utcnow().replace(second=0, microsecond=0)

    message = {
        'timestamp': live_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'station_id': int(row['StationID']),
        'location': {
            'latitude': round(row['Lat'], 6),
            'longitude': round(row['Lon'], 6)
        },
        'freeway': str(row['Freeway']),
        'direction': row['Direction'],
        'lane_type': row['LaneType'],
        'traffic': {
            'flow': row['TotalFlow'],
            'occupancy': row['AvgOccupancy'],
            'speed': row['AvgSpeed']
        }
    }

    print("🚦 Sending traffic message:", message)
    producer.send('traffic-data', value=message)
    time.sleep(1)  # simulate real-time stream

producer.flush()
print("✅ Done streaming real-time traffic data.")
