import sys
import time
import requests


def main():
    print(f"Python version in Docker: {sys.version}")
    print("Checking connection to poe.ninja...")

    try:
        # Пробуем просто достучаться до API
        response = requests.get("https://poe.ninja/api/data/itemoverview?league=Settlers&type=Currency")
        if response.status_code == 200:
            print("Success! poe.ninja is reachable.")
        else:
            print(f"Connected, but got status code: {response.status_code}")
    except Exception as e:
        print(f"Connection failed: {e}")

    print("Collector is going to sleep for 1 minute...")
    time.sleep(60)


if __name__ == "__main__":
    main()
