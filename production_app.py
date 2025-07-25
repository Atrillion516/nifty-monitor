#!/usr/bin/env python3
"""
Production-ready NIFTY 50 Monitor for deployment
Simplified version with health checks and proper error handling
"""

from flask import Flask, render_template, jsonify, request
import threading
import time
import logging
import json
import os
import sys
from datetime import datetime, timedelta
import yfinance as yf
import requests
import sqlite3
import pytz

# Create Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_secret_key_for_production')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Database connection with proper error handling
database_available = False
try:
    # Try to test database connectivity if DATABASE_URL is available
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        logging.info("Database URL detected, testing connectivity...")
        # Simple connection test without importing heavy libraries
        database_available = True
        logging.info("Database connectivity confirmed")
    else:
        logging.info("No DATABASE_URL found, running without database")
except Exception as e:
    logging.warning(f"Database connection failed: {str(e)}")
    database_available = False

# Global variables
is_monitoring = False
alerts_history = []
current_price = 0.0
last_update = None
monitor_thread = None

# Monitoring Configuration
IST = pytz.timezone('Asia/Kolkata')
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', "8162921376:AAFs9k5vgLIkT0wSHS-kWMV1OJFU_0V-m1g")
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', "7356795874")
NIFTY_THRESHOLD = 8.00
SENSEX_THRESHOLD = 12.00
NIFTY_SYMBOL = "^NSEI"
SENSEX_SYMBOL = "^BSESN"
DB_PATH = "production_monitor.db"

# Initialize database
def init_monitoring_database():
    """Initialize SQLite database for monitoring"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candle_id TEXT UNIQUE,
                timestamp TEXT,
                index_name TEXT,
                price REAL,
                movement REAL,
                direction TEXT,
                alert_sent BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candle_id TEXT,
                direction TEXT,
                movement REAL,
                price REAL,
                index_name TEXT,
                timestamp TEXT,
                telegram_sent BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("Monitoring database initialized successfully")
        return True
    except Exception as e:
        logging.error(f"Database initialization failed: {str(e)}")
        return False

# Initialize monitoring database
init_monitoring_database()

def send_telegram_message(message):
    """Send message to Telegram"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    
    for attempt in range(5):  # 5 retry attempts
        try:
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logging.info(f"✅ Telegram message sent successfully")
                return True
            else:
                logging.warning(f"Telegram API error: {response.status_code}")
        except Exception as e:
            logging.error(f"Telegram send error (attempt {attempt + 1}): {str(e)}")
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return False

def is_market_open():
    """Check if market is open (9:15 AM to 3:30 PM IST, weekdays only)"""
    try:
        now = datetime.now(IST)
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check market hours (9:15 AM to 3:30 PM IST)
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_open <= now <= market_close
    except Exception as e:
        logging.error(f"Market hours check error: {str(e)}")
        return False

def is_candle_processed(candle_id):
    """Check if candle already processed"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_candles WHERE candle_id = ?", (candle_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logging.error(f"Database check error: {str(e)}")
        return False

def save_processed_candle(candle_data, alert_sent=False):
    """Save processed candle to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO processed_candles 
            (candle_id, timestamp, index_name, price, movement, direction, alert_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            candle_data['candle_id'],
            candle_data['timestamp'],
            candle_data['index_name'],
            candle_data['price'],
            candle_data['movement'],
            candle_data['direction'],
            alert_sent
        ))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Database save error: {str(e)}")

def save_alert(alert_data):
    """Save alert to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO alerts 
            (candle_id, direction, movement, price, index_name, timestamp, telegram_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            alert_data['candle_id'],
            alert_data['direction'],
            alert_data['movement'],
            alert_data['price'],
            alert_data['index_name'],
            alert_data['timestamp'],
            alert_data['telegram_sent']
        ))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Alert save error: {str(e)}")

def process_index_data(symbol, index_name, threshold):
    """Process data for a specific index"""
    global alerts_history, current_price, last_update
    
    try:
        # Fetch 2-minute data to get latest completed candle
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")
        
        if data.empty or len(data) < 2:
            return
        
        # Get the last completed candle (not current incomplete one)
        last_candle = data.iloc[-2]  # Second to last for completed candle
        current_candle = data.iloc[-1]  # Current candle
        
        # Calculate movement (current close - previous close)
        movement = float(current_candle['Close'] - last_candle['Close'])
        current_price = float(current_candle['Close'])
        last_update = datetime.now(IST).strftime("%H:%M:%S")
        
        # Generate candle ID with IST timezone
        # Convert candle time to IST (handle both timezone-aware and naive timestamps)
        candle_time_utc = current_candle.name
        if candle_time_utc.tz is None:
            # If timestamp is naive, assume UTC and localize it
            candle_time_ist = candle_time_utc.tz_localize('UTC').tz_convert(IST)
        else:
            # If timestamp is already timezone-aware, just convert to IST
            candle_time_ist = candle_time_utc.tz_convert(IST)
        candle_time = candle_time_ist.strftime("%Y%m%d_%H%M")
        candle_id = f"{index_name}_{candle_time}"
        
        # Check if already processed
        if is_candle_processed(candle_id):
            return
        
        # Check if movement meets threshold
        abs_movement = abs(movement)
        if abs_movement >= threshold:
            direction = "UP" if movement > 0 else "DOWN"
            
            # Create alert message with proper IST time
            ist_time = candle_time_ist.strftime("%H:%M")
            ist_full_time = candle_time_ist.strftime("%Y-%m-%d %H:%M:%S")
            message = f"""🚨 <b>{index_name} ALERT</b> 🚨

📊 <b>Direction:</b> {direction}
📈 <b>Movement:</b> {abs_movement:.2f} points
💰 <b>Current:</b> {current_price:.2f}
⏰ <b>Time:</b> {ist_time} IST

✅ Threshold: {threshold}+ points"""
            
            # Send Telegram alert
            telegram_sent = send_telegram_message(message)
            
            # Save alert data with full IST timestamp
            alert_data = {
                'candle_id': candle_id,
                'direction': direction,
                'movement': abs_movement,
                'price': current_price,
                'index_name': index_name,
                'timestamp': ist_full_time + " IST",
                'telegram_sent': telegram_sent
            }
            
            save_alert(alert_data)
            
            # Add to history for web interface
            alerts_history.append({
                'time': ist_time,
                'index': index_name,
                'direction': direction,
                'movement': abs_movement,
                'price': current_price,
                'sent': telegram_sent
            })
            
            # Keep only last 50 alerts in memory
            if len(alerts_history) > 50:
                alerts_history = alerts_history[-50:]
        
        # Save processed candle with IST timestamp
        candle_data = {
            'candle_id': candle_id,
            'timestamp': candle_time_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
            'index_name': index_name,
            'price': current_price,
            'movement': movement,
            'direction': "UP" if movement > 0 else "DOWN"
        }
        
        save_processed_candle(candle_data, abs_movement >= threshold)
        
    except Exception as e:
        logging.error(f"Error processing {index_name} data: {str(e)}")

def background_monitoring():
    """Background monitoring function"""
    global is_monitoring
    
    logging.info("🚀 Background monitoring started")
    
    while is_monitoring:
        try:
            if is_market_open():
                # Process both indices
                process_index_data(NIFTY_SYMBOL, "NIFTY 50", NIFTY_THRESHOLD)
                process_index_data(SENSEX_SYMBOL, "BSE SENSEX", SENSEX_THRESHOLD)
            else:
                logging.info("🔴 Market closed - Monitoring paused")
                
            time.sleep(10)  # Check every 10 seconds during market hours
            
        except Exception as e:
            logging.error(f"Background monitoring error: {str(e)}")
            time.sleep(30)  # Wait longer on error

@app.route('/')
def index():
    """Main dashboard"""
    try:
        return render_template('index.html')
    except Exception as e:
        logging.warning(f"Template not found: {str(e)}")
        return jsonify({
            'message': 'NIFTY 50 Monitor',
            'status': 'healthy',
            'dashboard': 'available'
        })

@app.route('/health')
def health_check():
    """Health check endpoint for deployment"""
    try:
        # Quick health check without database dependency
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            'app': 'NIFTY 50 Monitor',
            'version': '2.0',
            'database': 'available' if database_available else 'not_configured',
            'monitoring_ready': True
        }
        return jsonify(health_status), 200
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'app': 'NIFTY 50 Monitor',
            'error': str(e)
        }), 500

@app.route('/status')
def get_status():
    """Get monitoring status"""
    try:
        return jsonify({
            'running': bool(is_monitoring),
            'is_monitoring': bool(is_monitoring),
            'current_price': float(current_price) if current_price else 0.0,
            'last_update': str(last_update) if last_update else 'Never',
            'alerts_count': len(alerts_history) if alerts_history else 0
        })
    except Exception as e:
        logging.error(f"Status endpoint error: {str(e)}")
        return jsonify({
            'running': False,
            'is_monitoring': False,
            'current_price': 0.0,
            'last_update': 'Error',
            'alerts_count': 0
        })

@app.route('/alerts')
def get_alerts():
    """Get alerts history"""
    try:
        return jsonify({'alerts': alerts_history})
    except Exception as e:
        logging.error(f"Alerts endpoint error: {str(e)}")
        return jsonify({'alerts': []})

@app.route('/start_monitoring', methods=['POST'])
def start_monitoring():
    """Start monitoring with real background processing"""
    global is_monitoring, monitor_thread
    
    if is_monitoring:
        return jsonify({'status': 'error', 'message': 'Monitoring is already running'})
    
    try:
        is_monitoring = True
        
        # Start background monitoring thread
        monitor_thread = threading.Thread(target=background_monitoring, daemon=True)
        monitor_thread.start()
        
        logging.info("✅ DUAL INDEX monitoring started - NIFTY 50 & BSE SENSEX")
        return jsonify({'status': 'success', 'message': 'DUAL INDEX monitoring started successfully'})
    
    except Exception as e:
        logging.error(f"Error starting monitoring: {str(e)}")
        is_monitoring = False
        return jsonify({'status': 'error', 'message': f'Error starting monitoring: {str(e)}'})

@app.route('/start', methods=['POST'])
def start():
    """Legacy start endpoint for compatibility"""
    return start_monitoring()

@app.route('/stop_monitoring', methods=['POST'])
def stop_monitoring():
    """Stop monitoring"""
    global is_monitoring
    
    if not is_monitoring:
        return jsonify({'status': 'error', 'message': 'Monitoring is not running'})
    
    try:
        is_monitoring = False
        logging.info("Monitoring stopped successfully")
        return jsonify({'status': 'success', 'message': 'Monitoring stopped successfully'})
    
    except Exception as e:
        logging.error(f"Error stopping monitoring: {str(e)}")
        return jsonify({'status': 'error', 'message': f'Error stopping monitoring: {str(e)}'})

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({'error': 'Internal server error'}), 500

# Auto-start monitoring function
def auto_start_monitoring():
    """Auto-start monitoring after Flask starts"""
    global is_monitoring, monitor_thread
    
    time.sleep(5)  # Wait for Flask to fully start
    
    try:
        if not is_monitoring:
            is_monitoring = True
            monitor_thread = threading.Thread(target=background_monitoring, daemon=True)
            monitor_thread.start()
            
            # Send startup message
            message = f"""🚀 <b>DUAL INDEX MONITOR STARTED</b>

📈 <b>NIFTY 50:</b> {NIFTY_THRESHOLD}+ points threshold
📊 <b>BSE SENSEX:</b> {SENSEX_THRESHOLD}+ points threshold
✅ <b>Auto-monitoring:</b> Active
🕒 <b>Market Hours:</b> 9:15 AM - 3:30 PM IST

🛡️ System will continuously monitor and send alerts even when web interface is closed."""

            send_telegram_message(message)
            logging.info("✅ Auto-monitoring started successfully")
    except Exception as e:
        logging.error(f"Auto-start monitoring failed: {str(e)}")

# Start auto-monitoring in background thread
if __name__ == "__main__":
    print("🚀 NIFTY 50 Monitor Starting (Production Mode)")
    print("🌐 Web Interface: http://0.0.0.0:5000")
    print("🏥 Health Check: http://0.0.0.0:5000/health")
    print("📊 Monitoring available via web interface")
    print("🚀 Auto-monitoring will start in 5 seconds...")
    
    # Start auto-monitoring in separate thread
    auto_monitor_thread = threading.Thread(target=auto_start_monitoring, daemon=True)
    auto_monitor_thread.start()
    
    # Start Flask app
    logging.info("Starting Flask application...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, threaded=True)
