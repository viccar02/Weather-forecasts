import os
import sqlite3
import requests
from groq import Groq
from datetime import datetime, UTC

DB_NAME = "weather.db"

LOCATIONS = [
    {"name": "Úbeda", "latitude": 38.0111, "longitude": -3.3714},
    {"name": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
    {"name": "Aalborg", "latitude": 57.0488, "longitude": 9.9217},
]

DAILY_VARS = [
    "temperature_2m_max",
    "precipitation_sum",
    "wind_speed_10m_max"
]


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT NOT NULL,
            forecast_date TEXT NOT NULL,
            temperature_2m_max REAL,
            precipitation_sum REAL,
            wind_speed_10m_max REAL,
            fetched_at TEXT NOT NULL,
            UNIQUE(location_name, forecast_date)
        )
    """)

    conn.commit()
    conn.close()


def fetch_weather(location):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "daily": ",".join(DAILY_VARS),
        "timezone": "auto",
        "forecast_days": 2
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    daily = data["daily"]

    forecast = {
        "location_name": location["name"],
        "forecast_date": daily["time"][1],
        "temperature_2m_max": daily["temperature_2m_max"][1],
        "precipitation_sum": daily["precipitation_sum"][1],
        "wind_speed_10m_max": daily["wind_speed_10m_max"][1],
        "fetched_at": datetime.now(UTC).isoformat()
    }

    return forecast


def save_forecast(row):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO forecasts (
            location_name,
            forecast_date,
            temperature_2m_max,
            precipitation_sum,
            wind_speed_10m_max,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        row["location_name"],
        row["forecast_date"],
        row["temperature_2m_max"],
        row["precipitation_sum"],
        row["wind_speed_10m_max"],
        row["fetched_at"]
    ))

    conn.commit()
    conn.close()


def get_latest_forecasts():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT location_name, forecast_date, temperature_2m_max, precipitation_sum, wind_speed_10m_max
        FROM forecasts
        ORDER BY location_name
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


def generate_poem(rows):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("Missing GROQ_API_KEY environment variable.")

    client = Groq(api_key=api_key)

    weather_text = "\n".join([
        f"{row[0]}: date={row[1]}, max temp={row[2]}°C, precipitation={row[3]} mm, wind speed={row[4]} km/h"
        for row in rows
    ])

    prompt = f"""
Write a short weather poem in TWO languages: English and Spanish.

The poem must:
- compare the weather in these three locations
- describe the differences
- suggest where it would be nicest to be tomorrow
- be short, creative, and easy to read

Weather data:
{weather_text}
"""

    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": "You are a creative assistant that writes short bilingual poems based on structured weather data."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.8
    )

    poem = completion.choices[0].message.content.strip()
    return poem


def save_poem(poem):
    with open("poem.txt", "w", encoding="utf-8") as f:
        f.write(poem)


def main():
    init_db()

    for location in LOCATIONS:
        forecast = fetch_weather(location)
        save_forecast(forecast)
        print(f"Saved forecast for {forecast['location_name']} on {forecast['forecast_date']}")

    rows = get_latest_forecasts()
    poem = generate_poem(rows)
    save_poem(poem)

    print("\nGenerated poem:\n")
    print(poem)


if __name__ == "__main__":
    main()
