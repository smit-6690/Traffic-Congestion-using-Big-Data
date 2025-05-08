# -------------------------------
# Smart Traffic Model Training Script
# -------------------------------

import pandas as pd
from pymongo import MongoClient
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np   # 🔥 Added for manual RMSE calculation

# -------------------------
# Step 1: Load and Preprocess Data
# -------------------------

# Connect to MongoDB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client.smart_traffic_db
collection = db.traffic_weather_incidents

# Fetch data
data = list(collection.find())
df = pd.DataFrame(data)
df.drop(columns=['_id'], inplace=True)

# Handle missing values
df.fillna(0, inplace=True)

# Convert categorical columns to string
for colname in ["direction", "lane_type", "region", "incident_type", "severity", "description"]:
    if colname in df.columns:
        df[colname] = df[colname].astype(str)

# Encode categorical columns
categorical_cols = ["direction", "lane_type", "region", "incident_type", "severity"]
le = LabelEncoder()
for colname in categorical_cols:
    if colname in df.columns:
        df[colname] = le.fit_transform(df[colname])

# -------------------------
# Step 2: Feature Selection
# -------------------------

feature_cols = [
    "freeway",
    "direction",
    "lane_type",
    "flow",
    "occupancy",
    "region",
    "temperature",
    "windspeed",
    "weathercode",
    "incident_type",
    "severity"
]

X = df[feature_cols]
y = df["speed"]

# -------------------------
# Step 3: Train-Test Split
# -------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42)

# -------------------------
# Step 4: Train Linear Regression Model
# -------------------------

model = LinearRegression()
model.fit(X_train, y_train)

print("\n✅ Model training completed!")

# -------------------------
# Step 5: Predict and Evaluate (Manual RMSE calculation)
# -------------------------

y_pred = model.predict(X_test)

# Calculate Mean Squared Error first
mse = mean_squared_error(y_test, y_pred)

# Take square root manually to get RMSE
rmse = np.sqrt(mse)

# Calculate R2 score
r2 = r2_score(y_test, y_pred)

print(f"\n✅ Model Evaluation:")
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
print(f"R² Score: {r2:.4f}")

# -------------------------
# Step 6: Visualization
# -------------------------

plt.figure(figsize=(8, 6))
sns.scatterplot(x=y_test, y=y_pred)
plt.xlabel("Actual Speed")
plt.ylabel("Predicted Speed")
plt.title("Actual vs Predicted Traffic Speeds")
plt.grid(True)
plt.show()


