import json
import os
import random
import sys
import time

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)
# Retrieve User-defined env vars
SLEEP_MS = os.getenv("SLEEP_MS", 0)
FAIL_RATE = os.getenv("FAIL_RATE", 0)
MULTIPLY_BY = os.getenv("MULTIPLY_BY", 1)
DATA_PATH = os.getenv("DATA_PATH", ".")
INPUT_PATH = f"{DATA_PATH}/{os.getenv('INPUT_UPSTREAM', '.')}"
OUTPUT_PATH = f"{DATA_PATH}/{os.getenv('OUTPUT_PATH', '.')}"

# Define main script
def main(sleep_ms=0, fail_rate=0):
    """Program that simulates work using the sleep method and random failures.

    Args:
        sleep_ms: number of milliseconds to sleep
        fail_rate: rate of simulated errors
    """
    print(f"Starting Task #{TASK_INDEX}, Attempt #{TASK_ATTEMPT}...")
    # Simulate work by waiting for a specific amount of time
    time.sleep(float(sleep_ms) / 1000)  # Convert to seconds

    # Simulate errors
    random_failure(float(fail_rate))

    cwd = os.getcwd()
    print(f"WORKING DIRECTORY: {cwd}")
    print(f"LS: {os.listdir()}")

    input_file = f"{INPUT_PATH}/{TASK_INDEX}.json"
    print(f"INPUT FILE: {input_file}")
    
    with open(input_file) as file:
        input = json.load(file)
        print(f"INPUT: {input}")

    payload = {
        "index": TASK_INDEX,
        "attempt": TASK_ATTEMPT,
        "value": int(input.get("value", TASK_INDEX)) * int(MULTIPLY_BY)
    }

    print(f"PAYLOAD: {payload}")

    out_file = f"{OUTPUT_PATH}/{TASK_INDEX}.json"
    print(f"OUT_FILE: {out_file}")

    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    with open(out_file, "w") as file:
        json.dump(payload, file)

    print(f"LS OUTPUT_PATH: {os.listdir(OUTPUT_PATH)}")
    print(f"Completed Task #{TASK_INDEX}.")


def random_failure(rate):
    """Throws an error based on fail rate

    Args:
        rate: a float between 0 and 1
    """
    if rate < 0 or rate > 1:
        # Return without retrying the Job Task
        print(
            f"Invalid FAIL_RATE env var value: {rate}. "
            + "Must be a float between 0 and 1 inclusive."
        )
        return

    random_failure = random.random()
    if random_failure < rate:
        raise Exception("Task failed.")


# Start script
if __name__ == "__main__":
    try:
        main(SLEEP_MS, FAIL_RATE)
    except Exception as err:
        message = (
            f"Task #{TASK_INDEX}, " + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"
        )

        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process