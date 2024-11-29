import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from dash import dash_table
from kafka import KafkaConsumer
import threading
import json
import logging
import plotly.graph_objs as go
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.WARNING)

# Create a Dash app
app = dash.Dash(__name__)

# Global storage for ticker data
ticker_data = {
    "INBGM.NL": {"timestamps": [], "prices": []},
    "A1JX4P.ETR": {"timestamps": [], "prices": []}
}

# EMA data
ema_data = []
ema_lock = threading.Lock()  # Lock for thread-safe access

# Kafka Consumer Setup
TICKER_TOPIC = "ticker_data"
EMA_TOPIC = "ema_data"
KAFKA_BROKER = "kafka:9092"

# Kafka Consumer function for ticker data
def consume_ticker_data():
    consumer_ticker = KafkaConsumer(
        TICKER_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id="ticker_consumer_group"
    )

    global ticker_data
    for message in consumer_ticker:
        data = message.value
        logging.info(f"Received ticker data: {data}")
        if "f1" in data:  # Ticker data
            timestamp = str(data.get("f0"))
            symbol = data.get("f1")
            last_price = data.get("f2")

            if symbol in ticker_data:
                ticker_data[symbol]["timestamps"].append(timestamp)
                ticker_data[symbol]["prices"].append(last_price)

    # Log the updated ticker_data
    logging.info(f"Updated ticker data: {ticker_data}")

# Kafka Consumer function for EMA data
def consume_ema_data():
    consumer_ema = KafkaConsumer(
        EMA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id="ema_consumer_group"
    )

    global ema_data
    for message in consumer_ema:
        data = message.value
        logging.info(f"Received EMA data: {data}")
        if "f0" in data:  # EMA data
            with ema_lock:
                ema_data.append({
                    "symbol": data["f0"],
                    "EMA_38": data["f1"],
                    "EMA_100": data["f2"],
                    "advice": data["f3"],
                    "advice_timestamp": datetime.utcfromtimestamp(int(data["f4"]) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    "latency": data["f5"]
                })

    # Log the updated ema_data
    logging.info(f"Updated EMA data: {ema_data}")

# Run Kafka Consumers in separate threads
ticker_thread = threading.Thread(target=consume_ticker_data, daemon=True)
ema_thread = threading.Thread(target=consume_ema_data, daemon=True)

ticker_thread.start()
ema_thread.start()

# Dash Layout
app.layout = html.Div(
    children=[
        html.H1("Real-Time Ticker and EMA Data Dashboard"),
        dcc.Graph(id="ticker-graph"),
        html.Div([
            html.H3("EMA Data Table"),
            dash_table.DataTable(
                id="ema-table",
                columns=[
                    {"name": "Symbol", "id": "symbol"},
                    {"name": "EMA_38", "id": "EMA_38"},
                    {"name": "EMA_100", "id": "EMA_100"},
                    {"name": "Advice", "id": "advice"},
                    {"name": "Advice Timestamp", "id": "advice_timestamp"},
                    {"name": "Latency", "id": "latency"}
                ],
                data=[],
                style_table={'height': '300px', 'overflowY': 'auto'},
                style_cell={'textAlign': 'center'}
            )
        ]),
        dcc.Interval(
            id="update-interval",
            interval=1000,  # Update every second
            n_intervals=0
        )
    ]
)

# Callback to update the graph and EMA table
@app.callback(
    [Output("ticker-graph", "figure"),
     Output("ema-table", "data")],
    [Input("update-interval", "n_intervals")]
)
def update_dashboard(n):
    # Process Ticker Data for Graph
    traces = []
    x_axis_range = []

    for ticker, data in ticker_data.items():
        if data["timestamps"]:
            # Convert timestamps to datetime objects
            try:
                timestamps = [datetime.strptime(ts, '%d-%m-%Y %H:%M:%S.%f') for ts in data["timestamps"]]
            except ValueError:
                timestamps = [datetime.strptime(ts, '%d-%m-%Y %H:%M:%S') for ts in data["timestamps"]]
            
            prices = data["prices"]
            traces.append(go.Scatter(
                x=timestamps, y=prices, mode='lines+markers', name=ticker
            ))
            x_axis_range.extend(timestamps)

    # Set x-axis range
    if x_axis_range:
        x_min = min(x_axis_range)
        x_max = max(x_axis_range)
        x_min = x_min + (x_max - x_min) / 2 
    else:
        x_min = x_max = datetime.now()

    ticker_figure = {
        "data": traces,
        "layout": go.Layout(
            title="Ticker Prices Over Time",
            xaxis={"title": "Timestamp", "type": "date", "range": [x_min, x_max]},
            yaxis={"title": "Price"},
            showlegend=True
        )
    }

    # Process EMA Data for Table
    with ema_lock:
        sorted_ema_data = sorted(
            ema_data,
            key=lambda x: datetime.strptime(x["advice_timestamp"], '%Y-%m-%d %H:%M:%S'),
            reverse=True
        )

    ema_table_data = [
        {
            "symbol": ema["symbol"],
            "EMA_38": ema["EMA_38"],
            "EMA_100": ema["EMA_100"],
            "advice": ema["advice"],
            "advice_timestamp": ema["advice_timestamp"],
            "latency": ema["latency"]
        }
        for ema in sorted_ema_data
    ]

    return ticker_figure, ema_table_data


if __name__ == "__main__":
    app.run_server(port=8040, debug=True, host='0.0.0.0')
