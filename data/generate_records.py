import csv
import random
from datetime import datetime, timedelta

def generate_records():
    output_file = "/data/cdr_data.csv"
    num_records = 2000100  # Just over 2M to ensure 2M+ records
    
    print(f"Starting dataset generation of {num_records} records to {output_file}...")
    
    # We want exactly 10% whale caller records. Let's make every 10th record a whale caller.
    whale_caller = "caller_whale"
    other_callers = [f"caller_{i}" for i in range(1, 1001)]
    towers = [f"tower_{i}" for i in range(1, 51)]
    
    # Base start time
    base_time = datetime(2026, 6, 1, 0, 0, 0)
    
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(["caller_id", "duration_sec", "charge_amount", "timestamp", "tower_id"])
        
        for i in range(num_records):
            # Deterministic 10% whale caller distribution
            if i % 10 == 0:
                caller = whale_caller
            else:
                caller = random.choice(other_callers)
            
            # Anomaly injection: 0.1% chance of a very long call (3600 to 7200 seconds)
            # Otherwise normal call duration (10 to 600 seconds)
            if random.random() < 0.001:
                duration = random.randint(3600, 7200)
            else:
                duration = random.randint(10, 600)
            
            # Charge amount is proportional to duration: $0.02 per minute + base charge
            charge_amount = round(0.05 + (duration * 0.0003), 2)
            
            # Spread times randomly over 10 days
            offset_seconds = random.randint(0, 10 * 24 * 3600)
            timestamp = (base_time + timedelta(seconds=offset_seconds)).strftime("%Y-%m-%d %H:%M:%S")
            
            tower = random.choice(towers)
            
            writer.writerow([caller, duration, charge_amount, timestamp, tower])
            
            if (i + 1) % 500000 == 0:
                print(f"Generated {i + 1} records...")

    print("Generation complete!")

if __name__ == "__main__":
    generate_records()
