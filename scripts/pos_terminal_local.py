import streamlit as st
import pandas as pd
import numpy as np
import json
import time
import random
import warnings
import plotly.express as px
import socket
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

st.set_page_config(page_title="POS Terminal", layout="wide", page_icon="💳")

# Initialize session state
if 'run_stream' not in st.session_state:
    st.session_state.run_stream = False
    st.session_state.tx_history = []
    st.session_state.alerts_history = []
    st.session_state.latency_metrics = []
    st.session_state.flagged_trans_nums = set()
    st.session_state.resolved_trans_nums = set()
    st.session_state.total_tx = 0
    st.session_state.total_amt = 0.0
    st.session_state.alert_counter = 0
    st.session_state.alert_amounts = {}
    st.session_state.pending_resolutions = {}
    st.session_state.df_index = 0
    st.session_state.render_key = 0
    st.session_state.default_terminal_id = f"TERM-{random.randint(1000, 9999)}"

def start_stream():
    st.session_state.run_stream = True

def stop_stream():
    st.session_state.run_stream = False

import streamlit.config as st_config
port = st_config.get_option("server.port")
st.markdown(f"<h1>💳 Point of Sale (POS) Terminal <span style='font-size: 1.8rem; color: gray; vertical-align: middle;'>({get_local_ip()}:{port})</span></h1>", unsafe_allow_html=True)
st.markdown("Simulate a live stream of credit card transactions from a merchant pushing to your message broker.")

# Sidebar Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    st.info("Operating in Local Kafka Mode")
    terminal_id = st.text_input("POS Terminal ID", value=st.session_state.default_terminal_id)
    st.markdown("---")
    st.sidebar.button("🚀 Start Transactions", width="stretch", disabled=st.session_state.run_stream, on_click=start_stream)
    st.sidebar.button("❌ Stop Transactions", width="stretch", disabled=not st.session_state.run_stream, on_click=stop_stream)

# Load data
@st.cache_data
def load_data():
    try:
        df = pd.read_csv('data/fraudTest.csv').sample(frac=1).reset_index(drop=True)
        
        # Inject Singaporean names
        sg_first_names = ["Wei Ming", "Jia Hui", "Zhi Hao", "Xin Yi", "Siti", "Nurul", "Muhammad", "Arif", "Raj", "Priya", "Wei Ling", "Jun Jie", "Desmond", "Alvin", "Jolene"]
        sg_last_names = ["Tan", "Lim", "Lee", "Ng", "Ong", "Wong", "Goh", "Chua", "Chan", "Koh", "Teo", "Bin Abdullah", "Binte Rahman", "Ramasamy", "Kumar"]
        
        mask = np.random.rand(len(df)) < 0.3
        df.loc[mask, 'first'] = np.random.choice(sg_first_names, size=mask.sum())
        df.loc[mask, 'last'] = np.random.choice(sg_last_names, size=mask.sum())
        
        return df
    except FileNotFoundError:
        return pd.DataFrame()

df_test = load_data()

if df_test.empty:
    st.error("🚨 `data/fraudTest.csv` not found! Please download it and place it in the `data/` folder.")
    st.stop()

# Dashboard Layout
col1, col2, col3 = st.columns(3)
metric_tx_count = col1.empty()
metric_total_amt = col2.empty()
metric_status = col3.empty()

st.subheader("📊 Outbound Transaction Feed")
chart_placeholder = st.empty()

colA, colB = st.columns(2)
with colA:
    st.markdown("**Recent Transactions**")
    table_placeholder = st.empty()
with colB:
    st.markdown("**Alerts & Bank Actions**")
    alerts_placeholder = st.empty()



metric_status.metric("Status", "🟢 Processing" if st.session_state.run_stream else "🔴 Idle")

def render_ui(r_key):
    metric_tx_count.metric("Transactions Sent", f"{st.session_state.total_tx:,}")
    metric_total_amt.metric("Total Value Processed", f"${st.session_state.total_amt:,.2f}")
    
    if st.session_state.tx_history:
        df_hist = pd.DataFrame(st.session_state.tx_history)
        df_hist.index = range(st.session_state.total_tx - len(df_hist) + 1, st.session_state.total_tx + 1)
        df_hist['is_fraud'] = df_hist['trans_num'].isin(st.session_state.flagged_trans_nums)
        
        # Line Chart
        chart_df = df_hist.tail(50)
        fig = px.line(chart_df, x=chart_df.index, y='amt', title="Recent Transaction Amounts")
        fig.update_traces(line_color='#3b82f6')
        
        fraud_df = chart_df[chart_df['is_fraud']]
        if not fraud_df.empty:
            fig.add_scatter(x=fraud_df.index, y=fraud_df['amt'], mode='markers', marker=dict(color='red', size=10), name='Fraud Alert')
            
        fig.update_layout(xaxis_title="Recent Transactions", yaxis_title="Amount ($)", height=300, showlegend=False)
        fig.update_xaxes(tickformat="d")
        chart_placeholder.plotly_chart(fig, width="stretch", key=f"chart_{r_key}")
        
        # Data Table
        df_display = df_hist[['terminal_id', 'merchant', 'trans_num', 'first', 'last', 'amt', 'city_pop', 'lat', 'long']].copy()
        df_display['merchant'] = df_display['merchant'].str.replace('fraud_', '')
        df_display['trans_num'] = df_display['trans_num'].str.slice(0, 16) + '...'
        table_placeholder.dataframe(df_display.iloc[::-1], width="stretch", height=400)
        
    if st.session_state.alerts_history:
        df_alerts = pd.DataFrame(st.session_state.alerts_history)
        if '_trans_num' in df_alerts.columns:
            df_alerts = df_alerts.drop(columns=['_trans_num'])
        alerts_placeholder.dataframe(df_alerts.iloc[::-1], width="stretch", height=400, hide_index=True)

render_ui(st.session_state.render_key)

@st.cache_resource
def get_kafka_clients():
    from kafka import KafkaProducer, KafkaConsumer
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    producer = KafkaProducer(bootstrap_servers=[kafka_broker])
    alert_consumer = KafkaConsumer(
        'fraud_alerts',
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='latest',
        consumer_timeout_ms=10
    )
    resolution_consumer = KafkaConsumer(
        'fraud_resolutions',
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='latest',
        consumer_timeout_ms=10
    )
    return producer, alert_consumer, resolution_consumer

try:
    producer, alert_consumer, resolution_consumer = get_kafka_clients()
except Exception as e:
    st.error(f"Failed to connect to Kafka: {e}")
    st.stop()

tx_history = st.session_state.tx_history
alerts_history = st.session_state.alerts_history
flagged_trans_nums = st.session_state.flagged_trans_nums
resolved_trans_nums = st.session_state.resolved_trans_nums
alert_amounts = st.session_state.alert_amounts
pending_resolutions = st.session_state.pending_resolutions

# Stream Loop
while True:
    ui_needs_update = False
    
    if st.session_state.run_stream and st.session_state.df_index < len(df_test):
        ui_needs_update = True
        row = df_test.iloc[st.session_state.df_index]
        st.session_state.df_index += 1
        transaction = row.to_dict()
        transaction['terminal_id'] = terminal_id
        
        # Publish Event
        producer.send('transactions', value=json.dumps(transaction).encode('utf-8'))
        
        # Update Metrics immediately
        st.session_state.total_tx += 1
        st.session_state.total_amt += transaction['amt']
        tx_history.append(transaction)
        
        # Keep only last 1000 transactions for performance
        if len(tx_history) > 1000:
            removed_tx = tx_history.pop(0)
            st.session_state.flagged_trans_nums.discard(removed_tx['trans_num'])
            st.session_state.resolved_trans_nums.discard(removed_tx['trans_num'])
    
    # Check if this transaction was instantly declined
    if alert_consumer:
        for alert_msg in alert_consumer:
            ui_needs_update = True
            alert = json.loads(alert_msg.value.decode('utf-8'))
            flagged_trans_nums.add(alert['trans_num'])
            merch_str = alert.get('merchant', 'Unknown')
            term_str = alert.get('terminal_id', 'Unknown')
            alert_amounts[alert['trans_num']] = {'amt': alert['amt'], 'merchant': merch_str, 'terminal': term_str}
            st.session_state.alert_counter += 1
            new_alert = {'ID': st.session_state.alert_counter, 'Status': '⏳ PENDING', 'Terminal': term_str, 'Merchant': merch_str, 'Card End': alert['trans_num'][-4:], 'Amount': f"${alert['amt']:.2f}", 'Time': time.strftime("%H:%M:%S"), '_trans_num': alert['trans_num']}
            alerts_history.append(new_alert)
            
            now = datetime.now()
            event_t = pd.to_datetime(alert.get('event_time', now.isoformat())).replace(tzinfo=None)
            ingest_t = pd.to_datetime(alert.get('ingestion_time', now.isoformat())).replace(tzinfo=None)
            process_t = pd.to_datetime(alert.get('processed_time', now.isoformat())).replace(tzinfo=None)
            
            if (now - event_t).total_seconds() > 86400:
                event_t = ingest_t
            
            e_to_i = max(0, (ingest_t - event_t).total_seconds() * 1000)
            i_to_p = max(0, (process_t - ingest_t).total_seconds() * 1000)
            p_to_r = max(0, (now - process_t).total_seconds() * 1000)
            
            st.session_state.latency_metrics.append({
                'Card End': alert['trans_num'][-4:],
                'Event->Ingest (ms)': f"{e_to_i:.1f}",
                'Ingest->Process (ms)': f"{i_to_p:.1f}",
                'Process->UI (ms)': f"{p_to_r:.1f}",
                'Total System Latency (ms)': f"{(e_to_i + i_to_p + p_to_r):.1f}"
            })
            if len(st.session_state.latency_metrics) > 1000:
                st.session_state.latency_metrics.pop(0)
            
            # Check if we received the resolution before the alert
            if alert['trans_num'] in pending_resolutions:
                res = pending_resolutions.pop(alert['trans_num'])
                if res['trans_num'] not in resolved_trans_nums:
                    resolved_trans_nums.add(res['trans_num'])
                    st.session_state.alert_counter += 1
                    user_str = res.get('user', 'Unknown')
                    amt_str = f"${alert['amt']:.2f}"
                    if res['action_type'] == 'whitelist':
                        alerts_history.append({'ID': st.session_state.alert_counter, 'Status': f'🟢 APPROVED by {user_str}', 'Terminal': term_str, 'Merchant': merch_str, 'Card End': alert['trans_num'][-4:], 'Amount': amt_str, 'Time': time.strftime("%H:%M:%S")})
                    elif res['action_type'] == 'freeze':
                        alerts_history.append({'ID': st.session_state.alert_counter, 'Status': f'❌ DECLINED by {user_str}', 'Terminal': term_str, 'Merchant': merch_str, 'Card End': alert['trans_num'][-4:], 'Amount': amt_str, 'Time': time.strftime("%H:%M:%S")})

    # Check for any bank resolutions
    if resolution_consumer:
        for res_msg in resolution_consumer:
            ui_needs_update = True
            res = json.loads(res_msg.value.decode('utf-8'))
            if res['trans_num'] not in alert_amounts:
                # Alert hasn't arrived yet, queue it
                pending_resolutions[res['trans_num']] = res
            else:
                if res['trans_num'] not in resolved_trans_nums:
                    resolved_trans_nums.add(res['trans_num'])
                    user_str = res.get('user', 'Unknown')
                    
                    st.session_state.alert_counter += 1
                    user_str = res.get('user', 'Unknown')
                    alert_info = alert_amounts.get(res['trans_num'], {'amt': 0.0, 'merchant': 'Unknown', 'terminal': 'Unknown'})
                    if isinstance(alert_info, float): # For backwards compatibility during hot reload
                        alert_info = {'amt': alert_info, 'merchant': 'Unknown', 'terminal': 'Unknown'}
                    amt_str = f"${alert_info['amt']:.2f}"
                    term_str = alert_info['terminal']
                    merch_str = alert_info['merchant']
                    if res['action_type'] == 'whitelist':
                        alerts_history.append({'ID': st.session_state.alert_counter, 'Status': f'🟢 APPROVED by {user_str}', 'Terminal': term_str, 'Merchant': merch_str, 'Card End': res['trans_num'][-4:], 'Amount': amt_str, 'Time': time.strftime("%H:%M:%S")})
                    elif res['action_type'] == 'freeze':
                        alerts_history.append({'ID': st.session_state.alert_counter, 'Status': f'❌ DECLINED by {user_str}', 'Terminal': term_str, 'Merchant': merch_str, 'Card End': res['trans_num'][-4:], 'Amount': amt_str, 'Time': time.strftime("%H:%M:%S")})

    # Keep only last 1000 alerts for performance
    if len(alerts_history) > 1000:
        alerts_history.pop(0)

    # Update UI
    if ui_needs_update:
        st.session_state.render_key += 1
        render_ui(st.session_state.render_key)

    time.sleep(random.uniform(0.1, 0.5) if st.session_state.run_stream else 0.5)
