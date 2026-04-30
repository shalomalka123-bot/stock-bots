import subprocess
import time

# מריץ את בוט החדשות
subprocess.Popen(["python", "catalyst_news_bot.py"])

# מריץ את בוט 5% בדקה
subprocess.Popen(["python", "minute_5_percent_bot.py"])

# שומר את השרת חי
while True:
    time.sleep(60)
