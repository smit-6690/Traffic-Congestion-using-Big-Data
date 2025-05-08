# IMPORT LIBRARIES
import pandas as pd
from pymongo import MongoClient

# CONNECT TO MONGODB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client.smart_traffic_db
collection = db.traffic_weather_incidents

# FETCH DATA
data = list(collection.find())

# CONVERT TO PANDAS DATAFRAME
df = pd.DataFrame(data)

# DROP MongoDB internal ID (_id field)
df.drop(columns=['_id'], inplace=True)

# SHOW THE RESULT
print("✅ Data successfully exported from MongoDB!")
print("✅ Shape of Data:", df.shape)
print("✅ First few rows:")
print(df.head(10))

