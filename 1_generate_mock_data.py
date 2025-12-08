import pandas as pd
import random
from datetime import datetime, timedelta

# Create a folder to act as our "Azure Landing Zone"
import os
os.makedirs("local_azure/landing_zone", exist_ok=True)

# Generate 10 random lab samples
data = {
    'sample_id': [f"SAMP-{i}" for i in range(100, 110)],
    'test_date': [datetime.now().strftime('%Y-%m-%d') for _ in range(10)],
    'result': [random.choice(['POS', 'NEG', 'Positive', 'N/A', 'FAIL']) for _ in range(10)], # 'Positive' and 'FAIL' are errors
    'viral_load': [random.randint(0, 5000) for _ in range(10)]
}

df = pd.DataFrame(data)

# Save to our fake landing zone
filename = f"local_azure/landing_zone/lab_results_{datetime.now().strftime('%M%S')}.csv"
df.to_csv(filename, index=False)

print(f"✅ Generated mock data at: {filename}")