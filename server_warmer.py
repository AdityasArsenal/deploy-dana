import requests
import schedule
import time
import os

def ping_server():
    try:
        # Replace with your actual Render service URL
        response = requests.get("https://dana-test-v.onrender.com/")
        response2 = requests.get("https://dana-test-v-qif0.onrender.com/")
        print(f"Ping status of first server: {response.status_code}")
        print(f"Ping status of second server: {response2.status_code}")
    except Exception as e:
        print(f"Ping error: {e}")

# Schedule pings every 10 minutes
schedule.every(13).minutes.do(ping_server)

# Optional: Run immediately on start
ping_server()

# Keep script running
while True:
    schedule.run_pending()
    time.sleep(1)