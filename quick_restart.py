#!/usr/bin/env python3
import os
import time

print("=" * 60)
print("Restarting Follow The Goat Services")
print("=" * 60)
print()

# Stop services
print("Stopping services...")
os.system("screen -S master -X quit 2>/dev/null")
os.system("screen -S website_api -X quit 2>/dev/null")
os.system("screen -S master2 -X quit 2>/dev/null")
os.system("pkill -f 'scheduler/master.py' 2>/dev/null")
os.system("pkill -f 'scheduler/master2.py' 2>/dev/null")
os.system("pkill -f 'scheduler/website_api.py' 2>/dev/null")
time.sleep(2)

# Start services
print("Starting master.py...")
os.system("screen -dmS master bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py'")
time.sleep(3)

print("Starting website_api.py...")
os.system("screen -dmS website_api bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py'")
time.sleep(2)

print("Starting master2.py...")
os.system("screen -dmS master2 bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py'")
time.sleep(2)

print()
print("✅ Services restarted!")
print()
print("Screen sessions:")
os.system("screen -ls")
print()
print("Check health:")
print("  curl http://127.0.0.1:5051/health")
