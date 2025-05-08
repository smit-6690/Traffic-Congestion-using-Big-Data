# -------------------------------
# Smart Traffic Data Preprocessing Script
# -------------------------------

# 1. Import libraries
import pandas as pd
from pymongo import MongoClient
from sklearn.preprocessing import LabelEncoder

# 2. Connect to MongoDB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client.smart_traffic_db
collection = db.traffic_weather_incidents

# 3. Fetch data from MongoDB
data = list(collection.find())

# 4. Convert to Pandas DataFrame
df = pd.DataFrame(data)

# 5. Drop MongoDB's internal _id field
if '_id' in df.columns:
    df.drop(columns=['_id'], inplace=True)

# 6. Print basic info about the DataFrame
print("\n✅ Data loaded from MongoDB!")
print(df.info())
print("\nFirst 5 rows before cleaning:")
print(df.head())

# -------------------------
# Step 1: Handle Missing Values
# -------------------------

# Fill missing numeric values with 0
df.fillna(0, inplace=True)

print("\n✅ Missing values handled (numeric NaNs filled with 0)!")

# -------------------------
# Step 2: Convert Categorical Columns to Strings Safely
# -------------------------

# Convert all categorical/text columns to string to avoid encoding errors
for colname in ["direction", "lane_type", "region", "incident_type", "severity", "description"]:
    if colname in df.columns:
        df[colname] = df[colname].astype(str)

print("\n✅ Categorical columns safely converted to string!")

# -------------------------
# Step 3: Encode Categorical Columns
# -------------------------

# List of columns you want to encode (exclude description)
categorical_cols = ["direction", "lane_type", "region", "incident_type", "severity"]

# Create LabelEncoder object
le = LabelEncoder()

# Apply label encoding
for colname in categorical_cols:
    if colname in df.columns:
        df[colname] = le.fit_transform(df[colname])

print("\n✅ Categorical columns encoded!")

# -------------------------
# Step 4: Final DataFrame Verification
# -------------------------

print("\n✅ Final DataFrame Info after cleaning and encoding:")
print(df.info())

print("\n✅ First 5 rows after full preprocessing:")
print(df.head())

# -------------------------
# Step 5: Select Features (X) and Target (y)
# -------------------------

# Input features
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

# Target variable
y = df["speed"]

print("\n✅ Features and target selected!")
print("X Shape:", X.shape)
print("y Shape:", y.shape)

# -------------------------
# Step 6: Train-Test Split
# -------------------------

from sklearn.model_selection import train_test_split

# 80% train, 20% test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42)

print("\n✅ Data split into training and testing sets!")
print("X_train Shape:", X_train.shape)
print("X_test Shape:", X_test.shape)
print("y_train Shape:", y_train.shape)
print("y_test Shape:", y_test.shape)





