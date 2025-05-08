# --------------------------------------------
# Predict traffic speed using trained XGBoost model
# --------------------------------------------

import pandas as pd
import joblib

# Load the trained model
model = joblib.load("traffic_speed_model.pkl")
print("✅ Model loaded!")

# Sample input (can be replaced with real-time Kafka or user input later)
new_data = pd.DataFrame([{
    "freeway": 680,
    "direction": 1,           # Already label encoded
    "lane_type": 2,           # Already label encoded
    "flow": 120,
    "occupancy": 0.028,
    "region": 3,              # Already label encoded
    "temperature": 17.5,
    "windspeed": 10.2,
    "weathercode": 2,
    "incident_type": 0,       # Already label encoded
    "severity": 1             # Already label encoded
}])

# Predict
predicted_speed = model.predict(new_data)[0]
print(f"\n🚗 Predicted Speed: {predicted_speed:.2f} units")

