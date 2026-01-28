from datetime import datetime, timezone, timedelta
from garminconnect import Garmin
from notion_client import Client
from dotenv import load_dotenv
import pytz
import os

# Your local time zone - Belgium
local_tz = pytz.timezone('Europe/Brussels')

ACTIVITY_ICONS = {
    "Barre": "https://img.icons8.com/?size=100&id=66924&format=png&color=000000",
    "Breathwork": "https://img.icons8.com/?size=100&id=9798&format=png&color=000000",
    "Cardio": "https://img.icons8.com/?size=100&id=71221&format=png&color=000000",
    "Cycling": "https://img.icons8.com/?size=100&id=47443&format=png&color=000000",
    "Hiking": "https://img.icons8.com/?size=100&id=9844&format=png&color=000000",
    "Indoor Cardio": "https://img.icons8.com/?size=100&id=62779&format=png&color=000000",
    "Indoor Cycling": "https://img.icons8.com/?size=100&id=47443&format=png&color=000000",
    "Indoor Rowing": "https://img.icons8.com/?size=100&id=71098&format=png&color=000000",
    "Pilates": "https://img.icons8.com/?size=100&id=9774&format=png&color=000000",
    "Meditation": "https://img.icons8.com/?size=100&id=9798&format=png&color=000000",
    "Rowing": "https://img.icons8.com/?size=100&id=71491&format=png&color=000000",
    "Running": "https://img.icons8.com/?size=100&id=k1l1XFkME39t&format=png&color=000000",
    "Strength Training": "https://img.icons8.com/?size=100&id=107640&format=png&color=000000",
    "Stretching": "https://img.icons8.com/?size=100&id=djfOcRn1m_kh&format=png&color=000000",
    "Swimming": "https://img.icons8.com/?size=100&id=9777&format=png&color=000000",
    "Treadmill Running": "https://img.icons8.com/?size=100&id=9794&format=png&color=000000",
    "Walking": "https://img.icons8.com/?size=100&id=9807&format=png&color=000000",
    "Yoga": "https://img.icons8.com/?size=100&id=9783&format=png&color=000000",
}

# Sentinel value for multiple matches (collision)
MULTIPLE_MATCH = "MULTIPLE_MATCH"

# DRY_RUN will be set in main() after load_dotenv()
DRY_RUN = False


def approx_equal(a, b, eps=0.01):
    """Compare two numbers with tolerance to avoid float comparison issues."""
    if a is None or b is None:
        return a == b
    return abs(a - b) <= eps


def get_all_activities(garmin, limit=10000):
    """
    Fetch all activities from Garmin with pagination.
    Default limit is 10000 to cover full history.
    """
    all_activities = []
    start = 0
    batch_size = 200  # Garmin API optimal batch size
    
    while True:
        chunk = garmin.get_activities(start, batch_size)
        if not chunk:
            break
        all_activities.extend(chunk)
        start += batch_size
        if len(all_activities) >= limit:
            break
    
    return all_activities[:limit]


def format_activity_type(activity_type, activity_name=""):
    formatted_type = activity_type.replace('_', ' ').title() if activity_type else "Unknown"
    activity_subtype = formatted_type
    activity_type = formatted_type

    activity_mapping = {
        "Barre": "Strength",
        "Indoor Cardio": "Cardio",
        "Indoor Cycling": "Cycling",
        "Indoor Rowing": "Rowing",
        "Speed Walking": "Walking",
        "Strength Training": "Strength",
        "Treadmill Running": "Running"
    }

    if formatted_type == "Rowing V2":
        activity_type = "Rowing"
    elif formatted_type in ["Yoga", "Pilates"]:
        activity_type = "Yoga/Pilates"
        activity_subtype = formatted_type

    if formatted_type in activity_mapping:
        activity_type = activity_mapping[formatted_type]
        activity_subtype = formatted_type

    if activity_name and "meditation" in activity_name.lower():
        return "Meditation", "Meditation"
    if activity_name and "barre" in activity_name.lower():
        return "Strength", "Barre"
    if activity_name and "stretch" in activity_name.lower():
        return "Stretching", "Stretching"
    
    return activity_type, activity_subtype


def format_entertainment(activity_name):
    if not activity_name:
        return "Unnamed Activity"
    return activity_name.replace('ENTERTAINMENT', 'Netflix')


def format_training_message(message):
    if not message:
        return "Unknown"
    messages = {
        'NO_': 'No Benefit',
        'MINOR_': 'Some Benefit',
        'RECOVERY_': 'Recovery',
        'MAINTAINING_': 'Maintaining',
        'IMPROVING_': 'Impacting',
        'IMPACTING_': 'Impacting',
        'HIGHLY_': 'Highly Impacting',
        'OVERREACHING_': 'Overreaching'
    }
    for key, value in messages.items():
        if message.startswith(key):
            return value
    return message


def format_training_effect(trainingEffect_label):
    if not trainingEffect_label:
        return "Unknown"
    return trainingEffect_label.replace('_', ' ').title()


def format_pace(average_speed):
    if average_speed and average_speed > 0:
        pace_min_km = 1000 / (average_speed * 60)
        minutes = int(pace_min_km)
        seconds = int((pace_min_km - minutes) * 60)
        return f"{minutes}:{seconds:02d} min/km"
    else:
        return ""


# ============================================================================
# FIX #1: Robust UTC parsing - no string heuristics
# ============================================================================

def parse_utc_datetime(dt_str):
    """
    Parse a datetime string from Garmin (UTC) robustly.
    Handles: "2026-01-28T18:37:00.0", "2026-01-28T18:37:00Z", "2026-01-28T18:37:00+00:00"
    
    Returns: datetime object with UTC timezone
    """
    if not dt_str:
        return None
    
    s = dt_str.strip()
    
    # Remove trailing Z and replace with +00:00
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    
    # Remove microseconds if present (before any timezone)
    if '.' in s:
        # Split at the dot, keep only the part before and any timezone after
        base, rest = s.split('.', 1)
        # Find timezone indicator in rest
        tz_part = ''
        for i, c in enumerate(rest):
            if c in ['+', '-']:
                tz_part = rest[i:]
                break
        s = base + tz_part
    
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Last resort: try basic format
        try:
            dt = datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    
    # If naive (no timezone), assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt


def convert_gmt_to_local(gmt_datetime_str):
    """
    Convert Garmin's startTimeGMT (UTC) to local timezone (Europe/Brussels).
    Returns ISO format string with timezone offset.
    """
    dt_utc = parse_utc_datetime(gmt_datetime_str)
    if dt_utc is None:
        return gmt_datetime_str  # Fallback to original
    
    dt_local = dt_utc.astimezone(local_tz)
    return dt_local.isoformat()


def get_local_date_range(gmt_datetime_str):
    """
    Get start and end of the LOCAL day for date range filtering.
    Returns: (start_iso, end_iso) in local timezone
    """
    dt_utc = parse_utc_datetime(gmt_datetime_str)
    if dt_utc is None:
        # Fallback: use date string directly
        date_str = gmt_datetime_str.split('T')[0]
        next_day = (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime('%Y-%m-%d')
        return date_str, next_day
    
    dt_local = dt_utc.astimezone(local_tz)
    
    # Start of local day (00:00:00)
    start_of_day = dt_local.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # End of local day (next day 00:00:00)
    end_of_day = start_of_day + timedelta(days=1)
    
    return start_of_day.isoformat(), end_of_day.isoformat()


# ============================================================================
# Collision guard - handle multiple matches
# ============================================================================

def activity_exists_by_garmin_id(client, database_id, garmin_activity_id):
    """
    Check if an activity exists using the unique Garmin Activity ID.
    
    Returns:
        - The existing Notion page if exactly 1 found
        - (MULTIPLE_MATCH, [page_ids]) tuple if >1 found (collision)
        - None if 0 found
    """
    if garmin_activity_id is None:
        return None
    
    query = client.databases.query(
        database_id=database_id,
        filter={
            "property": "Garmin ID",
            "number": {"equals": int(garmin_activity_id)}
        }
    )
    results = query.get('results', [])
    
    if len(results) == 0:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        # Return collision with page IDs for debugging
        page_ids = [r['id'] for r in results[:5]]  # First 5 for logging
        return (MULTIPLE_MATCH, page_ids)


def activity_exists_by_date_fallback(client, database_id, activity_date_gmt, activity_type, activity_name):
    """
    Fallback method for activities without Garmin ID.
    Uses LOCAL date range + type + name matching.
    
    Returns:
        - The existing Notion page if exactly 1 found
        - (MULTIPLE_MATCH, [page_ids]) tuple if >1 found (collision)
        - None if 0 found
    """
    if isinstance(activity_type, tuple):
        main_type, _ = activity_type
    else:
        main_type = activity_type[0] if isinstance(activity_type, (list, tuple)) else activity_type
    
    lookup_type = "Stretching" if "stretch" in activity_name.lower() else main_type
    
    start_iso, end_iso = get_local_date_range(activity_date_gmt)
    
    query = client.databases.query(
        database_id=database_id,
        filter={
            "and": [
                {"property": "Date", "date": {"on_or_after": start_iso}},
                {"property": "Date", "date": {"before": end_iso}},
                {"property": "Activity Type", "select": {"equals": lookup_type}},
                {"property": "Activity Name", "title": {"equals": activity_name}}
            ]
        }
    )
    results = query.get('results', [])
    
    if len(results) == 0:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        # Return collision with page IDs for debugging
        page_ids = [r['id'] for r in results[:5]]  # First 5 for logging
        return (MULTIPLE_MATCH, page_ids)


# ============================================================================
# FIX #2: Safe property access with .get() everywhere
# ============================================================================

def safe_get_number(props, key, default=0):
    """Safely get a number property from Notion, with default."""
    prop = props.get(key)
    if prop is None:
        return default
    num = prop.get('number')
    return num if num is not None else default


def safe_get_select(props, key, default=""):
    """Safely get a select property name from Notion, with default."""
    prop = props.get(key)
    if prop is None:
        return default
    select = prop.get('select')
    if select is None:
        return default
    return select.get('name', default)


def safe_get_checkbox(props, key, default=False):
    """Safely get a checkbox property from Notion, with default."""
    prop = props.get(key)
    if prop is None:
        return default
    return prop.get('checkbox', default)


def safe_get_rich_text(props, key, default=""):
    """Safely get rich_text content from Notion, with default."""
    prop = props.get(key)
    if prop is None:
        return default
    rich_text = prop.get('rich_text', [])
    if not rich_text:
        return default
    first = rich_text[0] if rich_text else {}
    text = first.get('text', {})
    return text.get('content', default)


def activity_needs_update(existing_activity, new_activity):
    """Check if an existing activity needs to be updated."""
    props = existing_activity['properties']
    
    activity_name = new_activity.get('activityName', '').lower()
    activity_type, activity_subtype = format_activity_type(
        new_activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    # Check if Garmin ID is missing (needs backfill)
    garmin_id_missing = safe_get_number(props, 'Garmin ID', None) is None
    if garmin_id_missing:
        return True
    
    # Compare all fields using safe accessors
    new_distance = round(new_activity.get('distance', 0) / 1000, 2)
    new_duration = round(new_activity.get('duration', 0) / 60, 2)
    new_calories = round(new_activity.get('calories', 0))
    new_pace = format_pace(new_activity.get('averageSpeed', 0))
    new_avg_power = round(new_activity.get('avgPower', 0), 1)
    new_max_power = round(new_activity.get('maxPower', 0), 1)
    new_training_effect = format_training_effect(new_activity.get('trainingEffectLabel', 'Unknown'))
    new_aerobic = round(new_activity.get('aerobicTrainingEffect', 0), 1)
    new_aerobic_effect = format_training_message(new_activity.get('aerobicTrainingEffectMessage', 'Unknown'))
    new_anaerobic = round(new_activity.get('anaerobicTrainingEffect', 0), 1)
    new_anaerobic_effect = format_training_message(new_activity.get('anaerobicTrainingEffectMessage', 'Unknown'))
    new_pr = new_activity.get('pr', False)
    new_fav = new_activity.get('favorite', False)
    
    needs_update = (
        not approx_equal(safe_get_number(props, 'Distance (km)', 0), new_distance, 0.01) or
        not approx_equal(safe_get_number(props, 'Duration (min)', 0), new_duration, 0.01) or
        safe_get_number(props, 'Calories', 0) != new_calories or
        safe_get_rich_text(props, 'Avg Pace', '') != new_pace or
        not approx_equal(safe_get_number(props, 'Avg Power', 0), new_avg_power, 0.1) or
        not approx_equal(safe_get_number(props, 'Max Power', 0), new_max_power, 0.1) or
        safe_get_select(props, 'Training Effect', 'Unknown') != new_training_effect or
        not approx_equal(safe_get_number(props, 'Aerobic', 0), new_aerobic, 0.1) or
        safe_get_select(props, 'Aerobic Effect', 'Unknown') != new_aerobic_effect or
        not approx_equal(safe_get_number(props, 'Anaerobic', 0), new_anaerobic, 0.1) or
        safe_get_select(props, 'Anaerobic Effect', 'Unknown') != new_anaerobic_effect or
        safe_get_checkbox(props, 'PR', False) != new_pr or
        safe_get_checkbox(props, 'Fav', False) != new_fav or
        safe_get_select(props, 'Activity Type', '') != activity_type
    )
    
    # Check subactivity only if we have one to compare
    existing_subtype = safe_get_select(props, 'Subactivity Type', '')
    if existing_subtype:
        needs_update = needs_update or (existing_subtype != activity_subtype)
    elif activity_subtype and activity_subtype != activity_type:
        # Subactivity missing but we have one to add
        needs_update = True
    
    return needs_update


# ============================================================================
# CREATE / UPDATE OPERATIONS
# FIX #3: update_activity now also updates Date
# ============================================================================

def create_activity(client, database_id, activity):
    """Create a new activity in Notion with Garmin ID."""
    
    garmin_id = activity.get('activityId')
    activity_date_gmt = activity.get('startTimeGMT')
    activity_name = format_entertainment(activity.get('activityName', 'Unnamed Activity'))
    activity_type, activity_subtype = format_activity_type(
        activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    activity_date_local = convert_gmt_to_local(activity_date_gmt)
    icon_url = ACTIVITY_ICONS.get(activity_subtype if activity_subtype != activity_type else activity_type)
    
    # DRY_RUN: preview without writing
    if DRY_RUN:
        return True  # Signal success for counting
    
    def build_properties(act_type, act_subtype):
        props = {
            "Date": {"date": {"start": activity_date_local}},
            "Activity Type": {"select": {"name": act_type}},
            "Subactivity Type": {"select": {"name": act_subtype}},
            "Activity Name": {"title": [{"text": {"content": activity_name}}]},
            "Distance (km)": {"number": round(activity.get('distance', 0) / 1000, 2)},
            "Duration (min)": {"number": round(activity.get('duration', 0) / 60, 2)},
            "Calories": {"number": round(activity.get('calories', 0))},
            "Avg Pace": {"rich_text": [{"text": {"content": format_pace(activity.get('averageSpeed', 0))}}]},
            "Avg Power": {"number": round(activity.get('avgPower', 0), 1)},
            "Max Power": {"number": round(activity.get('maxPower', 0), 1)},
            "Training Effect": {"select": {"name": format_training_effect(activity.get('trainingEffectLabel', 'Unknown'))}},
            "Aerobic": {"number": round(activity.get('aerobicTrainingEffect', 0), 1)},
            "Aerobic Effect": {"select": {"name": format_training_message(activity.get('aerobicTrainingEffectMessage', 'Unknown'))}},
            "Anaerobic": {"number": round(activity.get('anaerobicTrainingEffect', 0), 1)},
            "Anaerobic Effect": {"select": {"name": format_training_message(activity.get('anaerobicTrainingEffectMessage', 'Unknown'))}},
            "PR": {"checkbox": activity.get('pr', False)},
            "Fav": {"checkbox": activity.get('favorite', False)}
        }
        if garmin_id is not None:
            props["Garmin ID"] = {"number": int(garmin_id)}
        return props
    
    page = {
        "parent": {"database_id": database_id},
        "properties": build_properties(activity_type, activity_subtype),
    }
    
    if icon_url:
        page["icon"] = {"type": "external", "external": {"url": icon_url}}
    
    try:
        client.pages.create(**page)
        return True
    except Exception as e:
        error_msg = str(e).lower()
        # If select option doesn't exist, retry with "Unknown" fallback
        if any(x in error_msg for x in ["select", "is not a valid", "does not exist", "validation_error"]):
            print(f"⚠️ Unknown select option for {activity_name}, falling back to 'Unknown'")
            page["properties"] = build_properties("Unknown", "Unknown")
            try:
                client.pages.create(**page)
                return True
            except Exception as e2:
                print(f"ERROR creating {activity_name} (Garmin ID: {garmin_id}): {e2}")
                return False
        print(f"ERROR creating {activity_name} (Garmin ID: {garmin_id}): {e}")
        return False


def update_activity(client, existing_activity, new_activity):
    """Update an existing activity (including Garmin ID backfill and Date alignment)."""
    
    garmin_id = new_activity.get('activityId')
    activity_date_gmt = new_activity.get('startTimeGMT')
    activity_name = format_entertainment(new_activity.get("activityName"))
    activity_type, activity_subtype = format_activity_type(
        new_activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    activity_date_local = convert_gmt_to_local(activity_date_gmt)
    icon_url = ACTIVITY_ICONS.get(activity_subtype if activity_subtype != activity_type else activity_type)
    
    # DRY_RUN: preview without writing
    if DRY_RUN:
        return True  # Signal success for counting
    
    def build_properties(act_type, act_subtype):
        props = {
            "Date": {"date": {"start": activity_date_local}},
            "Activity Type": {"select": {"name": act_type}},
            "Subactivity Type": {"select": {"name": act_subtype}},
            "Distance (km)": {"number": round(new_activity.get('distance', 0) / 1000, 2)},
            "Duration (min)": {"number": round(new_activity.get('duration', 0) / 60, 2)},
            "Calories": {"number": round(new_activity.get('calories', 0))},
            "Avg Pace": {"rich_text": [{"text": {"content": format_pace(new_activity.get('averageSpeed', 0))}}]},
            "Avg Power": {"number": round(new_activity.get('avgPower', 0), 1)},
            "Max Power": {"number": round(new_activity.get('maxPower', 0), 1)},
            "Training Effect": {"select": {"name": format_training_effect(new_activity.get('trainingEffectLabel', 'Unknown'))}},
            "Aerobic": {"number": round(new_activity.get('aerobicTrainingEffect', 0), 1)},
            "Aerobic Effect": {"select": {"name": format_training_message(new_activity.get('aerobicTrainingEffectMessage', 'Unknown'))}},
            "Anaerobic": {"number": round(new_activity.get('anaerobicTrainingEffect', 0), 1)},
            "Anaerobic Effect": {"select": {"name": format_training_message(new_activity.get('anaerobicTrainingEffectMessage', 'Unknown'))}},
            "PR": {"checkbox": new_activity.get('pr', False)},
            "Fav": {"checkbox": new_activity.get('favorite', False)}
        }
        if garmin_id is not None:
            props["Garmin ID"] = {"number": int(garmin_id)}
        return props
    
    update = {
        "page_id": existing_activity['id'],
        "properties": build_properties(activity_type, activity_subtype),
    }
    
    if icon_url:
        update["icon"] = {"type": "external", "external": {"url": icon_url}}
    
    try:
        client.pages.update(**update)
        return True
    except Exception as e:
        error_msg = str(e).lower()
        # If select option doesn't exist, retry with "Unknown" fallback
        if any(x in error_msg for x in ["select", "is not a valid", "does not exist", "validation_error"]):
            print(f"⚠️ Unknown select option for {activity_name}, falling back to 'Unknown'")
            update["properties"] = build_properties("Unknown", "Unknown")
            try:
                client.pages.update(**update)
                return True
            except Exception as e2:
                print(f"ERROR updating {activity_name} (Garmin ID: {garmin_id}): {e2}")
                return False
        print(f"ERROR updating {activity_name} (Garmin ID: {garmin_id}): {e}")
        return False


# ============================================================================
# MAIN SYNC LOGIC
# ============================================================================

def main():
    global DRY_RUN
    load_dotenv()
    
    # DRY_RUN mode: set DRY_RUN=true in environment to test without writing
    DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
    
    # Check required environment variables
    required_vars = ["GARMIN_EMAIL", "GARMIN_PASSWORD", "NOTION_TOKEN", "NOTION_DB_ID"]
    missing = [k for k in required_vars if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DB_ID")

    garmin = Garmin(garmin_email, garmin_password)
    garmin.login()
    client = Client(auth=notion_token)
    
    activities = get_all_activities(garmin)
    
    # Counters for logging
    created_count = 0
    updated_count = 0
    unchanged_count = 0
    skipped_collision = 0
    error_count = 0
    found_by_id = 0
    found_by_fallback = 0
    
    if DRY_RUN:
        print("=" * 50)
        print("DRY_RUN MODE - No changes will be made")
        print("=" * 50)

    for activity in activities:
        garmin_id = activity.get('activityId')
        activity_date_gmt = activity.get('startTimeGMT')
        activity_name = format_entertainment(activity.get('activityName', 'Unnamed Activity'))
        activity_type, activity_subtype = format_activity_type(
            activity.get('activityType', {}).get('typeKey', 'Unknown'),
            activity_name
        )
        
        # =====================================================================
        # DEDUPLICATION STRATEGY:
        # 1. PRIMARY: Check by Garmin ID (idempotent, no timezone issues)
        # 2. FALLBACK: Check by date+type+name (for old activities without ID)
        # 3. COLLISION GUARD: Skip if multiple matches found
        # =====================================================================
        
        existing_activity = activity_exists_by_garmin_id(client, database_id, garmin_id)
        lookup_method = "ID"
        
        # Check for collision (returns tuple with page IDs)
        if isinstance(existing_activity, tuple) and existing_activity[0] == MULTIPLE_MATCH:
            page_ids = existing_activity[1]
            print(f"SKIP_COLLISION: Garmin ID {garmin_id} - {activity_name}")
            print(f"  → Duplicate page IDs: {page_ids}")
            skipped_collision += 1
            continue
        
        if existing_activity:
            found_by_id += 1
        else:
            # Fallback for old activities that don't have Garmin ID yet
            existing_activity = activity_exists_by_date_fallback(
                client, database_id, activity_date_gmt, activity_type, activity_name
            )
            lookup_method = "FALLBACK"
            
            # Check for collision in fallback
            if isinstance(existing_activity, tuple) and existing_activity[0] == MULTIPLE_MATCH:
                page_ids = existing_activity[1]
                print(f"SKIP_COLLISION: Fallback {activity_name} on {activity_date_gmt}")
                print(f"  → Duplicate page IDs: {page_ids}")
                skipped_collision += 1
                continue
            
            if existing_activity:
                found_by_fallback += 1
        
        if existing_activity:
            if activity_needs_update(existing_activity, activity):
                success = update_activity(client, existing_activity, activity)
                if success:
                    updated_count += 1
                    if DRY_RUN:
                        print(f"WOULD_UPDATE ({lookup_method}): {activity_name} (Garmin ID: {garmin_id})")
                    else:
                        print(f"UPDATED ({lookup_method}): {activity_name} (Garmin ID: {garmin_id})")
                else:
                    error_count += 1
            else:
                unchanged_count += 1
        else:
            success = create_activity(client, database_id, activity)
            if success:
                created_count += 1
                if DRY_RUN:
                    print(f"WOULD_CREATE: {activity_name} (Garmin ID: {garmin_id})")
                else:
                    print(f"CREATED: {activity_name} (Garmin ID: {garmin_id})")
            else:
                error_count += 1
    
    # Summary
    print(f"\n{'=' * 50}")
    print(f"=== SYNC COMPLETE {'(DRY_RUN)' if DRY_RUN else ''} ===")
    print(f"{'=' * 50}")
    print(f"Created: {created_count}")
    print(f"Updated: {updated_count}")
    print(f"Unchanged: {unchanged_count}")
    print(f"Found by ID: {found_by_id}")
    print(f"Found by fallback: {found_by_fallback}")
    print(f"Skipped (collision): {skipped_collision}")
    print(f"Errors: {error_count}")


if __name__ == '__main__':
    main()
