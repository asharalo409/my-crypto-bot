import os

# আপনার বাইন্যান্স এপিআই কি ও সিক্রেট
BINANCE_API_KEY = "0QzotPVZt9eAbkMSuPpDSe0OQXwbQqsYM59o7HmrDHlu46ClaAemMNlnDTIXrNkep"
BINANCE_API_SECRET = "qvhNafkMmHSgsFuOX4bPafqk45bDU1x8HUcasCSDFPBgMd9msH4uoTv2X6CjhQNb"

# টেলিগ্রাম কনফিগারেশন
TELEGRAM_TOKEN = "9089337002:AAFDg1eJBFuQzvEkN5FD1gVh8zk1J1WhZdo"
TELEGRAM_CHAT_ID = "7741119923"

# সার্ভার ও মার্কেট পেয়ার
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8080))
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT"]
MAX_OPEN_POSITIONS = 5
DB_FILE = "trading_history.db"
