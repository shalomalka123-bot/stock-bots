import subprocess

print("🚀 Starting all bots...")

subprocess.Popen(["python", "catalyst_news_bot.py"])
subprocess.Popen(["python", "minute_5_percent_bot.py"])

print("✅ All bots started")

while True:
    pass
