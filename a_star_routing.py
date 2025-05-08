# --------------------------------------------------
# Bay Area Road Network with Real ML Predicted Speeds
# --------------------------------------------------

import networkx as nx
import matplotlib.pyplot as plt
from geopy.distance import geodesic
import joblib
import pandas as pd
import os

# -------------------------
# Step 1: Define Stations
# -------------------------

stations = {
    "SF Downtown": (37.7749, -122.4194),
    "Daly City": (37.6879, -122.4702),
    "Oakland": (37.8044, -122.2711),
    "Berkeley": (37.8715, -122.2730),
    "Alameda": (37.7652, -122.2416),
    "Hayward": (37.6688, -122.0808),
    "San Mateo": (37.5630, -122.3255),
    "Fremont": (37.5485, -121.9886),
    "Palo Alto": (37.4419, -122.1430),
    "Redwood City": (37.4852, -122.2364),
    "Union City": (37.5934, -122.0438),
    "San Leandro": (37.7249, -122.1561),
    "Milpitas": (37.4323, -121.8996),
    "Santa Clara": (37.3541, -121.9552),
    "Mountain View": (37.3861, -122.0839),
    "Los Altos": (37.3852, -122.1141)
}

# -------------------------
# Step 2: Load ML Model
# -------------------------

model_path = os.path.expanduser("~/Desktop/traffic_speed_model.pkl")
model = joblib.load(model_path)

print("✅ Loaded XGBoost model for speed prediction.")

# -------------------------
# Step 3: Create Graph and Logical Connections
# -------------------------

G = nx.Graph()

# Add nodes
for station, coord in stations.items():
    G.add_node(station, pos=coord)

# Logical connections
connections = [
    ("SF Downtown", "Daly City"),
    ("SF Downtown", "Oakland"),
    ("Daly City", "San Mateo"),
    ("Oakland", "Berkeley"),
    ("Oakland", "Alameda"),
    ("Oakland", "San Leandro"),
    ("San Leandro", "Hayward"),
    ("Hayward", "Union City"),
    ("Union City", "Fremont"),
    ("Fremont", "Milpitas"),
    ("Milpitas", "Santa Clara"),
    ("Santa Clara", "Mountain View"),
    ("Mountain View", "Los Altos"),
    ("Los Altos", "Palo Alto"),
    ("San Mateo", "Redwood City"),
    ("Redwood City", "Palo Alto"),
    ("Palo Alto", "Mountain View"),
    ("Union City", "Milpitas"),
    ("San Leandro", "Union City")
]

# -------------------------
# Step 4: Assign Edge Weights Based on Predicted Speeds
# -------------------------

for (a, b) in connections:
    coord_a = stations[a]
    coord_b = stations[b]
    distance = geodesic(coord_a, coord_b).km  # kilometers

    # Prepare dummy input for ML model
    # We simulate some values — in real deployment, this will come from real-time MongoDB or Kafka
    sample_input = pd.DataFrame([{
        "freeway": 680,
        "direction": 1,
        "lane_type": 2,
        "flow": 100,
        "occupancy": 0.02,
        "region": 3,
        "temperature": 15,
        "windspeed": 10,
        "weathercode": 2,
        "incident_type": 0,
        "severity": 1
    }])

    predicted_speed = model.predict(sample_input)[0]  # km/h

    if predicted_speed < 1:  # Safety check
        predicted_speed = 5.0

    travel_time = distance / predicted_speed  # hours

    G.add_edge(a, b, distance=distance, speed=predicted_speed, travel_time=travel_time)

# -------------------------
# Step 5: A* Heuristic (Straight-Line Distance)
# -------------------------

def haversine_distance(u, v):
    coord_u = stations[u]
    coord_v = stations[v]
    return geodesic(coord_u, coord_v).km

# -------------------------
# Step 6: Ask User for Start and End Station
# -------------------------

print("\nAvailable Stations:")
for station in stations:
    print("-", station)

source_station = input("\nEnter Start Station Name exactly: ")
destination_station = input("Enter End Station Name exactly: ")

if source_station not in stations or destination_station not in stations:
    print("❌ Invalid station name entered. Please check spelling and try again.")
    exit()

# -------------------------
# Step 7: Run A* Search
# -------------------------

try:
    path = nx.astar_path(G, source=source_station, target=destination_station, heuristic=haversine_distance, weight='travel_time')
except nx.NetworkXNoPath:
    print("\n❌ No path found between the selected stations.")
    exit()

print("\n✅ Best Route Found:")

total_time_hours = 0

for i in range(len(path) - 1):
    u, v = path[i], path[i + 1]
    distance = G[u][v]['distance']
    speed = G[u][v]['speed']
    travel_time = G[u][v]['travel_time']
    
    total_time_hours += travel_time
    
    print(f"\nFrom {u} ➔ {v}:")
    print(f"Distance: {distance:.2f} km")
    print(f"Predicted Speed: {speed:.2f} km/h")
    print(f"Travel Time: {travel_time*60:.2f} minutes")

total_time_minutes = total_time_hours * 60
print(f"\n🕒 Total Estimated Travel Time: {total_time_minutes:.2f} minutes")

# -------------------------
# Step 8: Draw Network and Best Path
# -------------------------

pos = nx.get_node_attributes(G, 'pos')
path_edges = list(zip(path[:-1], path[1:]))

plt.figure(figsize=(14, 9))
nx.draw(G, pos, with_labels=True, node_size=700, node_color="lightblue", font_size=8)
nx.draw_networkx_edges(G, pos, edgelist=path_edges, edge_color='red', width=3)
plt.title(f"Best Route: {source_station} ➔ {destination_station}", fontsize=14)
plt.grid(True)
plt.show()




