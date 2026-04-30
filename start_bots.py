import subprocess
import time

# מריץ את בוט החדשות
subprocess.Popen(["python", "catalyst_news_bot.py"])

# מריץ את בוט ה-5% בדקה
subprocess.Popen(["python", "minute_5_percent_bot.py"])

# שומר את התהליך הראשי חי
while True:
    time.sleep(60)
