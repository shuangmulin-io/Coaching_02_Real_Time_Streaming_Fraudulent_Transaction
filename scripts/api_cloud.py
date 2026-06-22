from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
import json
import asyncio
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
app = FastAPI(title="Fraud Detection Dashboard API")

APP_VERSION = "1.0.0"
import datetime
LAST_UPDATED = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Ensure dashboard directory exists for static files
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")

# Mount static files
app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")

project_id = os.environ.get("GCP_PROJECT_ID", "YOUR-PROJECT-ID")
publisher = None
topic_path = None

try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, "fraud_resolutions")
except Exception as e:
    print("Failed to initialize Pub/Sub Publisher:", e)

class ActionRequest(BaseModel):
    action_type: str
    trans_num: str
    user: str = ""

@app.get("/")
async def get_index():
    return FileResponse(os.path.join(DASHBOARD_DIR, "index.html"))

@app.get("/api/version")
async def get_version():
    return {"version": APP_VERSION, "last_updated": LAST_UPDATED}

@app.post("/api/action")
async def process_action(req: ActionRequest, request: Request):
    user_identity = req.user.strip()
    if not user_identity:
        user_identity = request.client.host

    # Publish to fraud_resolutions for POS terminal to read
    if publisher and topic_path:
        publisher.publish(topic_path, json.dumps({'trans_num': req.trans_num, 'action_type': req.action_type, 'user': user_identity}).encode('utf-8'))
        
    print(f"Action received: {req.action_type} for transaction {req.trans_num} by {user_identity}")
    return {"status": "success", "message": f"Processed {req.action_type}"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriber = None
    sub_path = None
    try:
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(project_id, "fraud-alerts-sub")
    except Exception as e:
        print("Pub/Sub connection failed:", e)

    try:
        while True:
            if subscriber and sub_path:
                try:
                    response = subscriber.pull(request={"subscription": sub_path, "max_messages": 5}, timeout=1.0)
                    for msg in response.received_messages:
                        alert = json.loads(msg.message.data.decode('utf-8'))
                        await websocket.send_json(alert)
                        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [msg.ack_id]})
                except Exception as e:
                    if "WebSocketDisconnect" in str(type(e)) or "RuntimeError" in str(type(e)):
                        raise e
                    if "DeadlineExceeded" not in str(type(e)) and "RetryError" not in str(type(e)):
                        print(f"Pull error: {type(e).__name__} - {e}")
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    finally:
        pass
