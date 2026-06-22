import base64
import json
import joblib
import pandas as pd
import numpy as np
import functions_framework
import os
from google.cloud import pubsub_v1
from datetime import datetime

# Initialize Pub/Sub Publisher globally
publisher = pubsub_v1.PublisherClient()
project_id = os.environ.get("GCP_PROJECT_ID", "YOUR-PROJECT-ID")
alert_topic_path = publisher.topic_path(project_id, "fraud_alerts")

# Load the model globally so it persists across warm function invocations
print("Loading XGBoost Model...")
# Note: Ensure fraud_model.joblib is copied into this directory before deployment
try:
    model = joblib.load('fraud_model.joblib')
except FileNotFoundError:
    print("Warning: fraud_model.joblib not found. Ensure it is included in the deployment package.")
    model = None

@functions_framework.cloud_event
def process_transaction(cloud_event):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    """
    if model is None:
        print("Error: Model not loaded.")
        return

    # Decode the Pub/Sub message
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    tx = json.loads(pubsub_message)
    ingestion_time = cloud_event.data["message"].get("publishTime", datetime.now().isoformat())
    
    # Feature Engineering (Must match exactly what was done in training!)
    distance = np.sqrt((tx['lat'] - tx['merch_lat'])**2 + (tx['long'] - tx['merch_long'])**2)
    hour = pd.to_datetime(tx['trans_date_trans_time']).hour
    
    # Extract features in the exact order the model expects them
    features = pd.DataFrame([{
        'amt': tx['amt'],
        'distance': distance,
        'city_pop': tx['city_pop'],
        'hour': hour
    }])
    
    # Run Inference
    fraud_prob = model.predict_proba(features)[0][1]
    is_fraud = model.predict(features)[0]
    
    # Alerting Logic
    if is_fraud == 1:
        term_id = tx.get('terminal_id', 'Unknown')
        print(f"🚨 FRAUD ALERT! Term: {term_id} | TX: {tx['trans_num']} | User: {tx['first']} {tx['last']} | Amount: ${tx['amt']:.2f} | Prob: {fraud_prob:.2f}")
        # Send Alert back to UI via Pub/Sub
        alert_payload = {
            "trans_num": tx['trans_num'],
            "first": tx['first'],
            "last": tx['last'],
            "amt": float(tx['amt']),
            "prob": float(fraud_prob),
            "merchant": tx.get('merchant', 'Unknown').replace('fraud_', ''),
            "terminal_id": tx.get('terminal_id', 'Unknown'),
            "event_time": tx.get('trans_date_trans_time', 'Unknown'),
            "ingestion_time": ingestion_time,
            "processed_time": datetime.now().isoformat()
        }
        publisher.publish(alert_topic_path, json.dumps(alert_payload).encode("utf-8"))
    else:
        print(f"✅ Approved. TX: {tx['trans_num']} | Amount: ${tx['amt']:.2f}")
