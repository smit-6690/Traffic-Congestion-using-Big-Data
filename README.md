# Traffic-Congestion-using-Big-Data

A big data project for real-time traffic congestion analysis, prediction, and visualization using streaming data, machine learning, and modern data engineering tools.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Model Training](#model-training)
- [Dashboard](#dashboard)


## Overview

This project leverages big data technologies to analyze and predict traffic congestion using real-time data streams. It integrates data from various sources (traffic, weather, incidents), processes them using Apache Spark, stores results in MongoDB, and provides predictive analytics and routing using machine learning models.

## Features

- Real-time data ingestion using Kafka producers for traffic, weather, and incidents.
- Data cleaning and preprocessing pipelines.
- Machine learning models (XGBoost, others) for traffic speed prediction.
- A* algorithm for optimal routing.
- Interactive dashboard for visualization.
- Integration with MongoDB for data storage.


## Getting Started

### Prerequisites

- Python 3.7+
- Apache Spark
- Kafka
- MongoDB
- Required Python packages (see below)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/smit-6690/Traffic-Congestion-using-Big-Data.git
   cd Traffic-Congestion-using-Big-Data
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   *(Create a `requirements.txt` with packages such as pandas, numpy, scikit-learn, xgboost, pyspark, kafka-python, pymongo, etc.)*

3. Set up and start Kafka and MongoDB services.

## Usage

- **Data Producers:**  
  Start the Kafka producers to stream traffic, weather, and incident data:
  ```bash
  python kafka_traffic_producer.py
  python kafka_weather_producer.py
  python kafka_incident_producer.py
  ```

- **Spark Streaming:**  
  Run Spark streaming jobs to process incoming data:
  ```bash
  python spark_traffic_stream.py
  python spark_combined_stream.py
  ```

- **Data Storage:**  
  Use `spark_mongodb_writer.py` to write processed data to MongoDB.

- **Model Prediction:**  
  Use `predict_speed.py` to predict traffic speed using the trained model.

- **Routing:**  
  Use `a_star_routing.py` for optimal route calculation.

- **Dashboard:**  
  Run `dashboard.py` to launch the visualization dashboard.

## Model Training

- Use `model_training.py` and `xgboost_model.py` to train and evaluate machine learning models.
- The trained model is saved as `traffic_speed_model.pkl`.

## Dashboard

- The dashboard provides real-time visualization of traffic congestion and predictions.
- Run:
  ```bash
  python dashboard.py
  ```


