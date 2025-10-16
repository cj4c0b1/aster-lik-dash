import asyncio
import json
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, timezone
import websockets
import hashlib
from collections import deque
import time
import sqlite3
import os
import altair as alt
import threading
import plotly.graph_objects as go
import requests
from streamlit import fragment

# Constants
MAX_DATA_POINTS = 1000  # Maximum number of liquidations to keep in memory
ASTER_WS_URL = "wss://fstream.asterdex.com/ws"
#ASTER_WS_URL = "wss://fstream.binance.com/ws"

DB_PATH = "liquidations.db"

# Global storage for liquidations
liquidations = deque(maxlen=MAX_DATA_POINTS)

def init_db():
    """Initialize SQLite database and create table if not exists"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS liquidations (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            value REAL NOT NULL,
            time TEXT NOT NULL,
            UNIQUE(timestamp, symbol, side, value)
        )
    """)
    conn.commit()
    
    # Remove duplicates, keeping only the first occurrence
    cursor.execute("""
        DELETE FROM liquidations 
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM liquidations 
            GROUP BY timestamp, symbol, side, value
        )
    """)
    conn.commit()
    conn.close()

def load_liquidations_from_db():
    """Load recent liquidations from database into memory"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Load last 1 hour of data or up to MAX_DATA_POINTS
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    cursor.execute("""
        SELECT id, timestamp, symbol, side, price, quantity, value, time
        FROM liquidations
        WHERE timestamp >= ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (one_hour_ago, MAX_DATA_POINTS))
    rows = cursor.fetchall()
    conn.close()
    
    # Convert to deque and reverse to chronological order
    data = []
    for row in reversed(rows):  # Reverse to add oldest first
        data.append({
            'id': row[0],
            'timestamp': pd.Timestamp(row[1]),
            'symbol': row[2],
            'side': row[3],
            'price': row[4],
            'quantity': row[5],
            'value': row[6],
            'time': row[7]
        })
    # Deduplicate the loaded data
    data = deduplicate_liquidations(data)
    liquidations.extend(data)

def save_liquidation_to_db(liquidation):
    """Save a single liquidation to database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO liquidations (id, timestamp, symbol, side, price, quantity, value, time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (liquidation['id'], liquidation['timestamp'].isoformat(), liquidation['symbol'],
          liquidation['side'], liquidation['price'], liquidation['quantity'],
          liquidation['value'], liquidation['time']))
    conn.commit()
    conn.close()

def cleanup_old_liquidations():
    """Remove liquidations older than 1 hour from database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    cursor.execute("DELETE FROM liquidations WHERE timestamp < ?", (one_hour_ago,))
    conn.commit()
    conn.close()

def deduplicate_liquidations(liquidations_list):
    """Remove duplicates from liquidations list based on timestamp, symbol, side, value"""
    seen = set()
    unique_list = []
    for liq in liquidations_list:
        key = (liq['timestamp'], liq['symbol'], liq['side'], liq['value'])
        if key not in seen:
            seen.add(key)
            unique_list.append(liq)
    return unique_list

# Streamlit page config
st.set_page_config(
    page_title="Aster Liquidation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def process_liquidation(data):
    """Process incoming liquidation data and add to memory"""
    try:
        # Extract relevant data
        event = data.get('e')
        if event != 'forceOrder':
            return
            
        order = data.get('o', {})
        timestamp = pd.Timestamp.utcnow()
        
        # Create unique ID
        hash_input = f"{timestamp}:{order.get('s')}:{order.get('S')}:{order.get('q')}".encode('utf-8')
        liquidation_id = hashlib.md5(hash_input).hexdigest()[:8]
        
        # Calculate notional value
        price = float(order.get('ap', 0))
        quantity = float(order.get('q', 0))
        notional = price * quantity
        
        liquidation = {
            'id': liquidation_id,
            'timestamp': timestamp,
            'symbol': order.get('s', ''),
            'side': order.get('S', ''),
            'price': price,
            'quantity': quantity,
            'value': notional,
            'time': timestamp.strftime('%H:%M:%S')
        }
        
        # Check if already exists in memory
        key = (liquidation['timestamp'], liquidation['symbol'], liquidation['side'], liquidation['value'])
        existing_keys = {(liq['timestamp'], liq['symbol'], liq['side'], liq['value']) for liq in liquidations}
        if key not in existing_keys:
            # Add to memory
            liquidations.append(liquidation)
            
            # Save to database
            save_liquidation_to_db(liquidation)
        
    except Exception as e:
        print(f"Error processing liquidation: {e}")

def get_latest_liquidations():
    """Get latest liquidations as a DataFrame"""
    if not liquidations:
        return pd.DataFrame()
    
    df = pd.DataFrame(liquidations)
    
    # Ensure timestamp is datetime
    if not df.empty and 'timestamp' in df.columns:
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
    
    return df

def calculate_stats(df):
    """Calculate statistics for the dashboard"""
    now = pd.Timestamp.now('UTC')
    
    if df.empty:
        return {
            'total_liquidations': 0,
            'last_hour_liquidations': 0,
            'total_volume': 0,
            'hourly_volume': 0,
            'top_symbol': 'N/A',
            'top_side': 'N/A',
            'avg_trade_size': 0,
            'last_updated': now.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    # Last hour stats
    last_hour = now - pd.Timedelta(hours=1)
    recent_df = df[df['timestamp'] >= last_hour]
    
    return {
        'total_liquidations': len(df),
        'last_hour_liquidations': len(recent_df),
        'total_volume': df['value'].sum(),
        'hourly_volume': recent_df['value'].sum() if not recent_df.empty else 0,
        'top_symbol': df['symbol'].value_counts().idxmax(),
        'top_side': df['side'].value_counts().idxmax(),
        'avg_trade_size': df['value'].mean(),
        'last_updated': now.strftime('%Y-%m-%d %H:%M:%S')
    }

# Create a fragment for the gauge
@st.fragment(run_every=300)  # Auto-refresh every 60 seconds
def display_fear_greed_gauge():
    fng_data = get_fear_greed_index()
    if fng_data:
        gauge_fig = create_fear_greed_gauge(fng_data['value'], fng_data['classification'])
        st.plotly_chart(gauge_fig, use_container_width=True)
    else:
        st.warning("Fear & Greed data unavailable")


@st.cache_data(ttl=300)  # Cache for 1 minute instead of 5
def get_fear_greed_index():
    """Fetch the latest Fear and Greed Index data from the API"""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('metadata', {}).get('error'):
            st.error(f"API Error: {data['metadata']['error']}")
            return None
        
        # Get the most recent data point
        latest_data = data.get('data', [{}])[0]
        if not latest_data:
            st.warning("No Fear and Greed Index data available")
            return None
            
        return {
            'value': int(latest_data.get('value', 0)),
            'classification': latest_data.get('value_classification', 'Unknown'),
            'timestamp': latest_data.get('timestamp', '')
        }
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch Fear and Greed Index: {e}")
        return None
    except (ValueError, KeyError) as e:
        st.error(f"Error parsing Fear and Greed Index data: {e}")
        return None

def create_fear_greed_gauge(value, classification):
    """Create a CoinMarketCap-style gauge chart for the Fear and Greed Index"""
    
    # Define color mapping similar to CMC
    if value <= 24:
        bar_color = "#ea3943"  # Extreme Fear - Red
        classification_text = "Extreme Fear"
    elif value <= 44:
        bar_color = "#f6931e"  # Fear - Orange
        classification_text = "Fear"
    elif value <= 55:
        bar_color = "#f3d42f"  # Neutral - Yellow
        classification_text = "Neutral"
    elif value <= 75:
        bar_color = "#93d900"  # Greed - Light Green
        classification_text = "Greed"
    else:
        bar_color = "#16c784"  # Extreme Greed - Green
        classification_text = "Extreme Greed"
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        number={
            'font': {'size': 48, 'color': bar_color, 'family': 'Arial Black'},
            'suffix': ""
        },
        title={
            'text': f"<b>{classification_text}</b>",
            'font': {'size': 20, 'color': '#ffffff', 'family': 'Arial'}
        },
        gauge={
            'axis': {
                'range': [0, 100],
                'tickwidth': 2,
                'tickcolor': "#444444",
                'tickmode': 'linear',
                'tick0': 0,
                'dtick': 25,
                'tickfont': {'size': 14, 'color': '#cccccc'}
            },
            'bar': {
                'color': bar_color,
                'thickness': 0.75,
                'line': {'color': '#ffffff', 'width': 2}
            },
            'bgcolor': "#1E1E1E", # background-color: #1E1E1E
            'borderwidth': 3,
            'bordercolor': "#333333",
            'steps': [
                {'range': [0, 25], 'color': 'rgba(234, 57, 67, 0.5)'},      # Extreme Fear
                {'range': [25, 45], 'color': 'rgba(246, 147, 30, 0.5)'},    # Fear
                {'range': [45, 55], 'color': 'rgba(243, 212, 47, 0.5)'},    # Neutral
                {'range': [55, 75], 'color': 'rgba(147, 217, 0, 0.5)'},     # Greed
                {'range': [75, 100], 'color': 'rgba(22, 199, 132, 0.5)'}    # Extreme Greed
            ],
            'threshold': {
                'line': {'color': bar_color, 'width': 3},
                'thickness': 0.85,
                'value': value
            }
        }
    ))
    
    # Update layout for dark theme like CMC
    fig.update_layout(
        paper_bgcolor="#1E1E1E",
        plot_bgcolor="#0d0d0d",
        font={'color': "#ffffff", 'family': "Arial"},
        margin=dict(l=20, r=20, t=80, b=20),
        height=300
    )
    
    # Add annotation for labels
    fig.add_annotation(
        text="<b>Fear & Greed Index</b>",
        xref="paper",
        yref="paper",
        x=0.5,
        y=1.15,
        showarrow=False,
        font=dict(size=16, color="#888888", family="Arial")
    )
    
    return fig

async def aster_websocket():
    """Connect to Aster Dex websocket and process liquidations"""
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": ["!forceOrder@arr"],
        "id": 1
    }
    while True:
        try:
            async with websockets.connect(ASTER_WS_URL) as websocket:
                print("Connected to Aster Dex WebSocket")
                # Send subscription message after connecting
                await websocket.send(json.dumps(subscribe_msg))
                print("Subscription message sent for !forceOrder@arr")
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        process_liquidation(data)
                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed. Reconnecting...")
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        continue
        except Exception as e:
            print(f"WebSocket error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def start_websocket():
    """Start the websocket client in a separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(aster_websocket())

# Start websocket in a separate thread
websocket_thread = threading.Thread(target=start_websocket, daemon=True)
websocket_thread.start()

# Main app
def main():
    # Initialize database and load existing data
    init_db()
    load_liquidations_from_db()
    
    # Sidebar with branding
    with st.sidebar:
        st.image("static/logo.svg", width=200)
        st.markdown("### Powered by [Asterdex.com](https://www.asterdex.com/en/referral/183633)")
        st.markdown("""
        <div style="background-color: #1E1E1E; padding: 15px; border-radius: 10px; margin-top: 20px;">
            <p style="margin-bottom: 10px;">💸 Start trading on Aster dex with my referral link:</p>
            <p style="margin-bottom: 10px; font-size: 14px; color: #888;">
                <strong>You receive 5% back</strong> from all fees
        </div>
        """, unsafe_allow_html=True)
        # Sidebar placeholder for Fear and Greed Index
        #gauge_placeholder = st.empty()
        # Display gauge using fragment
        display_fear_greed_gauge()

        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; margin-top: 10px;">
            <a href="https://github.com/cj4c0b1/aster-lik-dash" target="_blank" style="text-decoration: none; color: #ffffff; font-weight: bold;">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24" style="vertical-align: middle; margin-right: 8px;">
                    <path fill="currentColor" d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
                View on GitHub
            </a>
        </div>
        """, unsafe_allow_html=True)

    st.title("📊 Real-time Liquidation Dashboard")
    st.markdown("---")

    # Create placeholders
    stats_placeholder = st.empty()
    chart_placeholder = st.empty()
    table_placeholder = st.empty()

    # Main loop
    cleanup_counter = 0
    while True:
        # Periodic cleanup every 60 iterations (roughly every minute)
        if cleanup_counter % 60 == 0:
            cleanup_old_liquidations()
        cleanup_counter += 1
        
        # Get latest data
        df = get_latest_liquidations()
        
        # Calculate stats
        stats = calculate_stats(df)
        
        # Display stats
        with stats_placeholder.container():
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Liquidations", f"{stats['total_liquidations']:,}")
            with col2:
                st.metric("Last Hour", f"{stats['last_hour_liquidations']:,}")
            with col3:
                st.metric("24h Volume", f"${stats['total_volume']/1e6:.1f}M")
            with col4:
                st.metric("Top Symbol", stats['top_symbol'])
        
        # NO GAUGE UPDATE HERE ANYMORE - it's handled by the fragment
        # Update Fear and Greed Index in sidebar
        #gauge_placeholder.empty()
        #with gauge_placeholder.container():
        #    fng_data = get_fear_greed_index()
        #    if fng_data:
        #        gauge_fig = create_fear_greed_gauge(fng_data['value'], fng_data['classification'])
        #        st.plotly_chart(gauge_fig, use_container_width=True, key=f"fear_greed_gauge_{cleanup_counter}")
        #    else:
        #        st.warning("Fear & Greed data unavailable")
        
        # Display chart if we have data
        if not df.empty:
            with chart_placeholder.container():
                st.subheader("Liquidation Volume by Symbol")
                chart_data = df.tail(100).groupby('symbol')['value'].sum().sort_values(ascending=False)
                # Create DataFrame for Altair
                chart_df = chart_data.reset_index()
                chart_df.columns = ['Symbol', 'Value']
                # Generate consistent colors for each symbol based on hash
                chart_df['Color'] = chart_df['Symbol'].apply(lambda s: f"#{hash(s) % 0x1000000:06x}")
                
                # Create Altair chart
                chart = alt.Chart(chart_df).mark_bar().encode(
                    x='Symbol:O',
                    y='Value:Q',
                    color=alt.Color('Color:N', scale=None)
                )
                st.altair_chart(chart, use_container_width=True)
        
        # Display latest liquidations
        with table_placeholder.container():
            st.subheader("Latest Liquidations (UTC time)")
            if not df.empty:
                # Format the display - sort by timestamp descending for latest first
                display_df = df.sort_values('timestamp', ascending=False)[['time', 'symbol', 'side', 'quantity', 'price', 'value']].head(20)
                display_df.columns = ['Time', 'Symbol', 'Side', 'Quantity', 'Price', 'Value']
                display_df['Value'] = display_df['Value'].apply(lambda x: f"${x:,.2f}")
                display_df['Price'] = display_df['Price'].apply(lambda x: f"${x:,.2f}")
                display_df['Quantity'] = display_df['Quantity'].apply(lambda x: f"{x:,.4f}")
                
                # Style the Side column with colors
                def color_side(val):
                    if val.upper() == 'SELL':
                        return 'color: red'
                    elif val.upper() == 'BUY':
                        return 'color: green'
                    return ''
                
                styled_df = display_df.style.map(color_side, subset=['Side'])
                st.dataframe(styled_df, width='stretch', hide_index=True)
            else:
                st.info("Waiting for liquidation data...")
        
        # Small delay to prevent high CPU usage
        time.sleep(1)

if __name__ == "__main__":
    main()
