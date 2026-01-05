import os
from datetime import datetime, timedelta
from garminconnect import Garmin
from notion_client import Client

# Configuration
GARMIN_EMAIL = os.environ.get("GARMIN_EMAIL")
GARMIN_PASSWORD = os.environ.get("GARMIN_PASSWORD")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("NOTION_SLEEP_DB_ID")

# Number of days to fetch (adjust as needed - 730 = ~2 years)
DAYS_TO_FETCH = 730


def format_duration(seconds):
    """Convert seconds to human-readable format (Xh Xm)."""
    if not seconds:
        return "0h 0m"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def seconds_to_hours(seconds):
    """Convert seconds to hours (decimal)."""
    if not seconds:
        return 0
    return round(seconds / 3600, 2)


def sleep_exists(client, database_id, sleep_date):
    """Check if sleep data already exists for a given date."""
    query = client.databases.query(
        database_id=database_id,
        filter={
            "property": "Long Date",
            "date": {
                "equals": sleep_date
            }
        }
    )
    return len(query["results"]) > 0


def write_to_notion(client, database_id, sleep_data, sleep_date):
    """Write sleep data to Notion database."""
    
    # Extract sleep values safely
    daily_sleep = sleep_data.get("dailySleepDTO", {})
    
    sleep_start = daily_sleep.get("sleepStartTimestampLocal")
    sleep_end = daily_sleep.get("sleepEndTimestampLocal")
    
    # Format times
    times_str = ""
    if sleep_start and sleep_end:
        try:
            start_time = datetime.fromisoformat(sleep_start.replace("Z", "")).strftime("%H:%M")
            end_time = datetime.fromisoformat(sleep_end.replace("Z", "")).strftime("%H:%M")
            times_str = f"{start_time} - {end_time}"
        except:
            times_str = ""
    
    # Get sleep stages (in seconds)
    deep_sleep_seconds = daily_sleep.get("deepSleepSeconds", 0) or 0
    light_sleep_seconds = daily_sleep.get("lightSleepSeconds", 0) or 0
    rem_sleep_seconds = daily_sleep.get("remSleepSeconds", 0) or 0
    awake_seconds = daily_sleep.get("awakeSleepSeconds", 0) or 0
    
    # Total sleep
    total_sleep_seconds = deep_sleep_seconds + light_sleep_seconds + rem_sleep_seconds
    
    # Resting heart rate
    resting_hr = daily_sleep.get("restingHeartRate", 0) or 0
    
    # Sleep goal (assume 8 hours = 28800 seconds)
    sleep_goal_met = total_sleep_seconds >= 25200  # 7 hours
    
    client.pages.create(
        parent={"database_id": database_id},
        properties={
            "Date": {
                "title": [{"text": {"content": sleep_date}}]
            },
            "Long Date": {
                "date": {"start": sleep_date}
            },
            "Times": {
                "rich_text": [{"text": {"content": times_str}}]
            },
            "Total Sleep": {
                "rich_text": [{"text": {"content": format_duration(total_sleep_seconds)}}]
            },
            "Total Sleep (h)": {
                "number": seconds_to_hours(total_sleep_seconds)
            },
            "Deep Sleep": {
                "rich_text": [{"text": {"content": format_duration(deep_sleep_seconds)}}]
            },
            "Deep Sleep (h)": {
                "number": seconds_to_hours(deep_sleep_seconds)
            },
            "Light Sleep": {
                "rich_text": [{"text": {"content": format_duration(light_sleep_seconds)}}]
            },
            "Light Sleep (h)": {
                "number": seconds_to_hours(light_sleep_seconds)
            },
            "REM Sleep": {
                "rich_text": [{"text": {"content": format_duration(rem_sleep_seconds)}}]
            },
            "REM Sleep (h)": {
                "number": seconds_to_hours(rem_sleep_seconds)
            },
            "Awake Time": {
                "rich_text": [{"text": {"content": format_duration(awake_seconds)}}]
            },
            "Awake Time (h)": {
                "number": seconds_to_hours(awake_seconds)
            },
            "Resting HR": {
                "number": resting_hr
            },
            "Sleep Goal": {
                "checkbox": sleep_goal_met
            }
        }
    )


def main():
    if not DATABASE_ID:
        print("NOTION_SLEEP_DB_ID not set, skipping sleep data sync")
        return
    
    # Initialize clients
    garmin = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
    garmin.login()
    notion = Client(auth=NOTION_TOKEN)
    
    # Get dates to fetch
    today = datetime.now().date()
    
    # Counter for stats
    added_count = 0
    skipped_count = 0
    error_count = 0
    
    print(f"Fetching sleep data for the last {DAYS_TO_FETCH} days...")
    
    for i in range(DAYS_TO_FETCH):
        current_date = today - timedelta(days=i)
        date_str = current_date.isoformat()
        
        try:
            # Check if already exists
            if sleep_exists(notion, DATABASE_ID, date_str):
                skipped_count += 1
                continue
            
            # Fetch from Garmin
            sleep_data = garmin.get_sleep_data(date_str)
            
            if sleep_data and sleep_data.get("dailySleepDTO"):
                daily_sleep = sleep_data.get("dailySleepDTO", {})
                total_sleep = (daily_sleep.get("deepSleepSeconds", 0) or 0) + \
                              (daily_sleep.get("lightSleepSeconds", 0) or 0) + \
                              (daily_sleep.get("remSleepSeconds", 0) or 0)
                
                if total_sleep > 0:
                    write_to_notion(notion, DATABASE_ID, sleep_data, date_str)
                    added_count += 1
                    print(f"Created sleep entry for: {date_str} ({seconds_to_hours(total_sleep)}h)")
                else:
                    skipped_count += 1
            else:
                skipped_count += 1
                
        except Exception as e:
            error_count += 1
            if "404" not in str(e) and "No data" not in str(e):
                print(f"Error fetching {date_str}: {e}")
    
    print(f"\nSleep sync complete: {added_count} added, {skipped_count} skipped, {error_count} errors")


if __name__ == "__main__":
    main()
