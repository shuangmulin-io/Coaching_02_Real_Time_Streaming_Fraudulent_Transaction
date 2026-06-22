resource "google_pubsub_topic" "fraud_transactions" {
  name = "transactions"
  
  labels = {
    environment = "coaching-session"
  }
}

resource "google_pubsub_topic" "fraud_alerts" {
  name = "fraud_alerts"
  
  labels = {
    environment = "coaching-session"
  }
}

resource "google_pubsub_topic" "fraud_resolutions" {
  name = "fraud_resolutions"
  
  labels = {
    environment = "coaching-session"
  }
}

resource "google_pubsub_subscription" "fraud_alerts_sub" {
  name  = "fraud-alerts-sub"
  topic = google_pubsub_topic.fraud_alerts.name

  message_retention_duration = "1200s"
  retain_acked_messages      = false
}

resource "google_pubsub_subscription" "fraud_alerts_pos_sub" {
  name  = "fraud-alerts-pos-sub"
  topic = google_pubsub_topic.fraud_alerts.name

  message_retention_duration = "1200s"
  retain_acked_messages      = false
}

resource "google_pubsub_subscription" "fraud_resolutions_sub" {
  name  = "fraud-resolutions-sub"
  topic = google_pubsub_topic.fraud_resolutions.name

  message_retention_duration = "1200s"
  retain_acked_messages      = false
}
