# -----------------------------------------
# XGBoost Model for Smart Traffic Prediction
# -----------------------------------------

import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
import joblib

# -------------------------
# Step 1: Load Data from MongoDB
# -------------------------

client = MongoClient("mongodb://127.0.0.1:27017/")
db = client.smart_traffic_db
collection = db.traffic_weather_incidents
data = list(collection.find())
df = pd.DataFrame(data)

# Drop MongoDB internal ID column
if '_id' in df.columns:
    df.drop(columns=['_id'], inplace=True)

# Fill missing values with 0
df.fillna(0, inplace=True)

# Convert object columns to strings
for colname in ["direction", "lane_type", "region", "incident_type", "severity", "description"]:
    if colname in df.columns:
        df[colname] = df[colname].astype(str)

# -------------------------
# Step 2: Encode Categorical Columns
# -------------------------

categorical_cols = ["direction", "lane_type", "region", "incident_type", "severity"]
le = LabelEncoder()
for colname in categorical_cols:
    df[colname] = le.fit_transform(df[colname])

# 🔥 FIX: Convert freeway to numeric (XGBoost doesn't accept object dtype)
df["freeway"] = pd.to_numeric(df["freeway"], errors='coerce')
df["freeway"] = df["freeway"].fillna(0).astype(int)

# -------------------------
# Step 3: Feature Selection
# -------------------------

feature_cols = [
    "freeway", "direction", "lane_type", "flow", "occupancy",
    "region", "temperature", "windspeed", "weathercode",
    "incident_type", "severity"
]

X = df[feature_cols]
y = df["speed"]

# -------------------------
# Step 4: Train-Test Split
# -------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42)

# -------------------------
# Step 5: Train XGBoost Model
# -------------------------

model = xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1
)

model.fit(X_train, y_train)

print("\n✅ XGBoost model training completed!")

# -------------------------
# Step 6: Predict and Evaluate
# -------------------------

y_pred = model.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"\n✅ Model Evaluation:")
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
print(f"R² Score: {r2:.4f}")

# -------------------------
# Step 7: Feature Importance Chart
# -------------------------

plt.figure(figsize=(10, 6))
xgb.plot_importance(model, importance_type='gain', show_values=False)
plt.title("Feature Importance (by Gain)")
plt.grid(True)
plt.tight_layout()
plt.show()

# -------------------------
# Step 8: Actual vs Predicted Scatter Plot
# -------------------------

plt.figure(figsize=(8, 6))
sns.scatterplot(x=y_test, y=y_pred, alpha=0.6)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')  # identity line
plt.xlabel("Actual Speed")
plt.ylabel("Predicted Speed")
plt.title("XGBoost: Actual vs Predicted Traffic Speeds")
plt.grid(True)
plt.tight_layout()
plt.show()

# Save model
joblib.dump(model, "traffic_speed_model.pkl")
print("\n✅ XGBoost model saved as 'traffic_speed_model.pkl'")
