import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from kafka import KafkaConsumer
import json
import pandas as pd
import threading
from datetime import datetime, timedelta, timezone

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Kafka Stream Visualization"

# Globals for real-time updates
ema_data_lock = threading.Lock()
ticker_data_lock = threading.Lock()

ema_data = pd.DataFrame(columns=["timestamp", "symbol", "EMA_38", "EMA_100", "advice", "advice_timestamp", "latency"])
ticker_data = pd.DataFrame(columns=["timestamp", "symbol", "last_price"])

MAX_ROWS = 5000  # Limit the number of rows to keep in DataFrames
BATCH_SIZE = 100  # Batch size for consumer processing

# Kafka Consumers for EMA and ticker data
def consume_kafka_stream(topic, global_df, data_lock):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        group_id=f"{topic}_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    for message in consumer:
        data = message.value
        try:
            # Prepare the new data
            new_data = None
            if topic == "ema_data":
                ema_timestamp = datetime.fromtimestamp(data["f4"] / 1000, timezone.utc)
                new_data = {
                    "timestamp": ema_timestamp,
                    "symbol": data["f0"],
                    "EMA_38": data["f1"],
                    "EMA_100": data["f2"],
                    "advice": data["f3"],
                    "advice_timestamp": data["f4"],
                    "latency": data["f5"]
                }

            elif topic == "ticker_data":
                ticker_timestamp = datetime.strptime(data["f0"], "%d-%m-%Y %H:%M:%S.%f").replace(tzinfo=timezone.utc)
                new_data = {
                    "timestamp": ticker_timestamp,
                    "symbol": data["f1"],
                    "last_price": data["f2"]
                }

            if new_data:
                with data_lock:
                    # Append new data to the DataFrame
                    global_df.loc[len(global_df)] = new_data

                    # Ensure the DataFrame does not exceed MAX_ROWS
                    if len(global_df) > MAX_ROWS:
                        global_df.drop(global_df.index[0], inplace=True)

                # Update the global variable (required for Dash callbacks)
                if topic == "ema_data":
                    global ema_data
                    ema_data = global_df
                elif topic == "ticker_data":
                    global ticker_data
                    ticker_data = global_df

        except Exception as e:
            print(f"Error processing data from topic {topic}: {e}")


# Start Kafka consumers in separate threads
threading.Thread(target=consume_kafka_stream, args=("ema_data", ema_data, ema_data_lock), daemon=True).start()
threading.Thread(target=consume_kafka_stream, args=("ticker_data", ticker_data, ticker_data_lock), daemon=True).start()

# Callback to update the plots
@app.callback(
    [Output("ema-graph", "figure"), Output("ticker-graph", "figure")],
    [Input("update-interval", "n_intervals"), Input("symbol-filter", "value")]
)
def update_graphs(n_intervals, selected_symbols):
    global ema_data, ticker_data

    # Ensure thread safety when reading global DataFrames
    with ema_data_lock:
        ema_snapshot = ema_data.copy()
    with ticker_data_lock:
        ticker_snapshot = ticker_data.copy()
    print(len(ema_snapshot),len(ticker_snapshot))
    # Filter by selected symbols if any
    if selected_symbols:
        ema_snapshot = ema_snapshot[ema_snapshot["symbol"].isin(selected_symbols)]
        ticker_snapshot = ticker_snapshot[ticker_snapshot["symbol"].isin(selected_symbols)]

    # EMA Graph
    ema_fig = go.Figure()
    for symbol in ema_snapshot["symbol"].unique():
    # Filter data for the current symbol
        df = ema_snapshot[ema_snapshot["symbol"] == symbol]

        # Check if there are exactly two rows (one for EMA_38 and one for EMA_100)
        if len(df) == 2:
            # Get the EMA_38 and EMA_100 values for plotting
            ema_38_data = df[df['EMA_38'].notna()]
            ema_100_data = df[df['EMA_100'].notna()]

            # Get the advice value for this symbol (buy or sell)
            advice = df['advice'].iloc[0]  # Assume the advice is the same for both EMA values

            # Set marker properties based on the advice (color and shape)
            if advice == "buy":
                advice_color = 'green'
                advice_shape = 'circle'
            elif advice == "sell":
                advice_color = 'red'
                advice_shape = 'square'
            else:
                advice_color = 'blue'  # Default color if advice is unclear
                advice_shape = 'diamond'

            # Plot EMA_38 as a marker with advice-based color and shape
            ema_fig.add_trace(go.Scatter(
                x=ema_38_data["timestamp"], 
                y=ema_38_data["EMA_38"], 
                mode="markers", 
                marker=dict(size=12, color=advice_color, symbol=advice_shape),
                name=f"{symbol} EMA_38 - {advice.capitalize()} Decision"
            ))
            
            # Plot EMA_100 as a marker with advice-based color and shape
            ema_fig.add_trace(go.Scatter(
                x=ema_100_data["timestamp"], 
                y=ema_100_data["EMA_100"], 
                mode="markers", 
                marker=dict(size=12, color=advice_color, symbol=advice_shape),
                name=f"{symbol} EMA_100 - {advice.capitalize()} Decision"
            ))

# Update layout for better visualization
    ema_fig.update_layout(
        title="Exponential Moving Averages (EMA) with Buy/Sell Decisions",
        xaxis_title="Time",
        yaxis_title="EMA Value",
        showlegend=True,  # Ensure the legend is visible
        hovermode="closest"  # Display the values when hovering over points
    )


    # Ticker Graph
    ticker_fig = go.Figure()
    for symbol in ticker_snapshot["symbol"].unique():
        df = ticker_snapshot[ticker_snapshot["symbol"] == symbol]
        ticker_fig.add_trace(
            go.Scatter(
                x=df["timestamp"], 
                y=df["last_price"], 
                mode="lines", 
                name=f"{symbol} Prices"
            )
        )

    ticker_fig.update_layout(
        title="Ticker Data (Last 5 Minutes)",
        xaxis_title="Time",
        yaxis_title="Price",
        legend_title="Symbols",
        xaxis_rangeslider_visible=False,
        showlegend=True 
    )

    return ema_fig, ticker_fig

# Dynamic dropdown options
@app.callback(
    Output("symbol-filter", "options"),
    [Input("update-interval", "n_intervals")]
)
def update_dropdown_options(n_intervals):
    with ema_data_lock, ticker_data_lock:
        unique_symbols = pd.concat([ema_data["symbol"], ticker_data["symbol"]]).dropna().unique()
    return [{"label": symbol, "value": symbol} for symbol in unique_symbols]

# Dash Layout
app.layout = html.Div([
    html.H1("Kafka Stream Real-Time Visualization", style={"textAlign": "center"}),

    dcc.Dropdown(
        id="symbol-filter",
        options=[],
        multi=True,
        placeholder="Select symbols to display"
    ),

    dcc.Graph(id="ema-graph", style={"height": "50vh"}),
    dcc.Graph(id="ticker-graph", style={"height": "50vh"}),

    dcc.Interval(
        id="update-interval",
        interval=1000,  # Update every second
        n_intervals=0
    ),
])

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
