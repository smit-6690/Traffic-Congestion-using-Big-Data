# ----------------------------------------------------
# Smart Traffic Routing Dashboard (Streamlit + A*)
# ----------------------------------------------------

import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt
import joblib
import pandas as pd
from geopy.distance import geodesic
import os

# -------------------------
# Setup: Load Data
# -------------------------

# Define stations
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

model_path = os.path.expanduser("~/Desktop/traffic_speed_model.pkl")
model = joblib.load(model_path)
print("✅ Loaded XGBoost model for speed prediction.")

# Build the road network
G = nx.Graph()

for station, coord in stations.items():
    G.add_node(station, pos=coord)

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

for (a, b) in connections:
    coord_a = stations[a]
    coord_b = stations[b]
    distance = geodesic(coord_a, coord_b).km

    # Predict speed using ML model
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

    predicted_speed = model.predict(sample_input)[0]

    if predicted_speed < 1:
        predicted_speed = 5.0

    travel_time = distance / predicted_speed

    G.add_edge(a, b, distance=distance, speed=predicted_speed, travel_time=travel_time)

# -------------------------
# Streamlit App
# -------------------------

st.title("🚦 Smart Traffic Routing Dashboard")

st.sidebar.header("Select Your Route")
start_station = st.sidebar.selectbox("Start Station:", list(stations.keys()))
end_station = st.sidebar.selectbox("End Station:", list(stations.keys()))

if st.sidebar.button("Find Best Route"):
    try:
        path = nx.astar_path(G, source=start_station, target=end_station, heuristic=lambda u, v: geodesic(stations[u], stations[v]).km, weight='travel_time')

        st.success(f"✅ Best Route: {' ➔ '.join(path)}")

        total_time_hours = 0

        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            distance = G[u][v]['distance']
            speed = G[u][v]['speed']
            travel_time = G[u][v]['travel_time']
            total_time_hours += travel_time

            st.write(f"From {u} ➔ {v}:")
            st.write(f"Distance: {distance:.2f} km | Predicted Speed: {speed:.2f} km/h | Travel Time: {travel_time*60:.2f} minutes")

        total_time_minutes = total_time_hours * 60
        st.subheader(f"🕒 Total Estimated Travel Time: {total_time_minutes:.2f} minutes")

        # Draw Map
        pos = nx.get_node_attributes(G, 'pos')
        path_edges = list(zip(path[:-1], path[1:]))

        fig, ax = plt.subplots(figsize=(12, 8))
        nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=700, font_size=8)
        nx.draw_networkx_edges(G, pos, edgelist=path_edges, edge_color='red', width=3)
        plt.title(f"Best Route: {start_station} ➔ {end_station}")
        st.pyplot(fig)

    except nx.NetworkXNoPath:
        st.error("❌ No path found between selected stations.")


