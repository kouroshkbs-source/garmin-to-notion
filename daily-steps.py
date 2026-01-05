import os
from datetime import datetime, timedelta
from garminconnect import Garmin
from notion_client import Client

# Configuration
GARMIN_EMAIL = os.environ.get("GARMIN_EMAIL")
GARMIN_PASSWORD = os.environ.get("GARMIN_PASSWORD")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("NOTION_STEPS_DB_ID")

# Number of days to fetch (adjust as needed - 730 = ~2 years)
DAYS_TO_FETCH = 730


def daily_steps_exist(client, database_id, steps_date):
    """Check if steps data already exists for a given date."""
    query = client.databases.query(
        database_id=database_id,
        filter={
            "property": "Date",
            "date": {
                "equals": steps_date
            }
        }
    )
    return len(query["results"]) > 0


def write_to_notion(client, database_id, steps_data, steps_date):
    """Write steps data to Notion database."""
    total_steps = steps_data.get("totalSteps", 0)
    total_distance = steps_data.get("totalDistanceMeters", 0) / 1000  # Convert to km
    step_goal = steps_data.get("dailyStepGoal", 0)
    
    client.pages.create(
        parent={"database_id": database_id},
        properties={
            "Activity Type": {
                "title": [{"text": {"content": "Daily Steps"}}]
            },
            "Date": {
                "date": {"start": steps_date}
            },
            "Total Steps": {
                "number": total_steps
            },
            "Total Distance (km)": {
                "number": round(total_distance, 2)
            },
            "Step Goal": {
                "number": step_goal
            }
        }
    )


def main():
    if not DATABASE_ID:
        print("NOTION_STEPS_DB_ID not set, skipping daily steps sync")
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
    
    print(f"Fetching steps data for the last {DAYS_TO_FETCH} days...")
    
    for i in range(DAYS_TO_FETCH):
        current_date = today - timedelta(days=i)
        date_str = current_date.isoformat()
        
        try:
            # Check if already exists
            if daily_steps_exist(notion, DATABASE_ID, date_str):
                skipped_count += 1
                continue
            
            # Fetch from Garmin
            steps_data = garmin.get_user_summary(date_str)
            
            if steps_data and steps_data.get("totalSteps", 0) > 0:
                write_to_notion(notion, DATABASE_ID, steps_data, date_str)
                added_count += 1
                print(f"Created steps entry for: {date_str} ({steps_data.get('totalSteps', 0)} steps)")
            else:
                skipped_count += 1
                
        except Exception as e:
            error_count += 1
            if "404" not in str(e) and "No data" not in str(e):
                print(f"Error fetching {date_str}: {e}")
    
    print(f"\nSteps sync complete: {added_count} added, {skipped_count} skipped, {error_count} errors")


if __name__ == "__main__":
    main()
