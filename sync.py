#!/usr/bin/env python3
"""
Unified Garmin to Notion Sync Script

This script consolidates all sync operations into a single process,
using a shared Garmin session to minimize login calls and improve reliability.

Supports two authentication methods:
1. TOKENSTORE (recommended): Uses persisted OAuth tokens from GARMIN_TOKENSTORE_PATH
2. EMAIL/PASSWORD (fallback): Traditional login with GARMIN_EMAIL/GARMIN_PASSWORD

Environment Variables:
- GARMIN_TOKENSTORE_PATH: Path to tokenstore JSON file (recommended)
- GARMIN_EMAIL / GARMIN_PASSWORD: Fallback credentials
- NOTION_TOKEN: Notion API token
- NOTION_DB_ID: Activities database ID
- NOTION_PR_DB_ID: Personal Records database ID (optional)
- NOTION_STEPS_DB_ID: Daily Steps database ID (optional)
- NOTION_SLEEP_DB_ID: Sleep database ID (optional)
- SYNC_DAYS: Number of days to sync (default: 7)
- SYNC_ALL: Set to 'true' for full history sync
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

from garminconnect import Garmin
from notion_client import Client
import pytz

# Import sync functions from individual modules
# We'll define them inline to keep everything self-contained

# =============================================================================
# CONFIGURATION
# =============================================================================

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

MULTIPLE_MATCH = "MULTIPLE_MATCH"


# =============================================================================
# GARMIN AUTHENTICATION
# =============================================================================

def init_garmin_client():
    """
    Initialize Garmin client with tokenstore (preferred) or email/password fallback.
    Returns authenticated Garmin client.
    """
    tokenstore_path = os.getenv("GARMIN_TOKENSTORE_PATH")
    
    if tokenstore_path and Path(tokenstore_path).exists():
        print(f"🔑 Using tokenstore authentication: {tokenstore_path}")
        try:
            garmin = Garmin()
            garmin.login(tokenstore_path)
            print("✅ Tokenstore login successful")
            return garmin
        except Exception as e:
            print(f"⚠️ Tokenstore login failed: {e}")
            print("Falling back to email/password...")
    
    # Fallback to email/password
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    
    if not email or not password:
        raise RuntimeError(
            "No valid authentication method found. "
            "Set GARMIN_TOKENSTORE_PATH or GARMIN_EMAIL/GARMIN_PASSWORD"
        )
    
    print("🔑 Using email/password authentication")
    garmin = Garmin(email, password)
    garmin.login()
    print("✅ Email/password login successful")
    return garmin


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_utc_datetime(dt_str):
    """Parse a datetime string from Garmin (UTC) robustly."""
    if not dt_str:
        return None
    
    s = dt_str.strip()
    
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    
    if '.' in s:
        base, rest = s.split('.', 1)
        tz_part = ''
        for i, c in enumerate(rest):
            if c in ['+', '-']:
                tz_part = rest[i:]
                break
        s = base + tz_part
    
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt


def convert_gmt_to_local(gmt_datetime_str):
    """Convert Garmin's startTimeGMT (UTC) to local timezone."""
    dt_utc = parse_utc_datetime(gmt_datetime_str)
    if dt_utc is None:
        return gmt_datetime_str
    return dt_utc.astimezone(local_tz).isoformat()


def get_local_date_range(gmt_datetime_str):
    """Get start and end of the LOCAL day for date range filtering."""
    dt_utc = parse_utc_datetime(gmt_datetime_str)
    if dt_utc is None:
        return None, None
    
    dt_local = dt_utc.astimezone(local_tz)
    start_of_day = dt_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
    
    return start_of_day.isoformat(), end_of_day.isoformat()


def approx_equal(a, b, eps=0.01):
    """Compare two numbers with tolerance."""
    if a is None or b is None:
        return a == b
    return abs(a - b) <= eps


def format_activity_type(activity_type, activity_name=""):
    """Format activity type and subtype."""
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


def format_training_effect(label):
    if not label:
        return "Unknown"
    return label.replace('_', ' ').title()


def format_pace(average_speed):
    if average_speed and average_speed > 0:
        pace_min_km = 1000 / (average_speed * 60)
        minutes = int(pace_min_km)
        seconds = int((pace_min_km - minutes) * 60)
        return f"{minutes}:{seconds:02d} min/km"
    return ""


def format_duration(seconds):
    """Convert seconds to Xh Xm format."""
    if not seconds:
        return "0h 0m"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def seconds_to_hours(seconds):
    """Convert seconds to decimal hours."""
    if not seconds:
        return 0
    return round(seconds / 3600, 2)


# =============================================================================
# ACTIVITIES SYNC
# =============================================================================

def get_recent_activities(garmin, days=7):
    """Fetch recent activities with cutoff + overlap."""
    all_activities = []
    start = 0
    batch_size = 100
    # Add 3-day overlap for edge cases
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days + 3)
    
    while True:
        chunk = garmin.get_activities(start, batch_size)
        if not chunk:
            break
        
        for activity in chunk:
            activity_date_str = activity.get('startTimeGMT')
            if activity_date_str:
                activity_date = parse_utc_datetime(activity_date_str)
                if activity_date and activity_date < cutoff_date:
                    print(f"  Reached cutoff date ({days} days ago), stopping fetch")
                    return all_activities
            all_activities.append(activity)
        
        start += batch_size
    
    return all_activities


def get_all_activities(garmin, limit=10000):
    """Fetch all activities (full sync mode)."""
    all_activities = []
    start = 0
    batch_size = 200
    
    while True:
        chunk = garmin.get_activities(start, batch_size)
        if not chunk:
            break
        all_activities.extend(chunk)
        start += batch_size
        if len(all_activities) >= limit:
            break
    
    return all_activities[:limit]


def activity_exists_by_garmin_id(client, database_id, garmin_id):
    """Check if activity exists by Garmin ID."""
    if garmin_id is None:
        return None
    
    query = client.databases.query(
        database_id=database_id,
        filter={"property": "Garmin ID", "number": {"equals": int(garmin_id)}}
    )
    
    results = query.get("results", [])
    if len(results) == 0:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        return (MULTIPLE_MATCH, [r['id'] for r in results])


def activity_exists_by_date_fallback(client, database_id, activity_date_gmt, activity_type, activity_name):
    """Fallback check by date + type + name."""
    start_iso, end_iso = get_local_date_range(activity_date_gmt)
    if not start_iso or not end_iso:
        return None
    
    query = client.databases.query(
        database_id=database_id,
        filter={
            "and": [
                {"property": "Date", "date": {"on_or_after": start_iso}},
                {"property": "Date", "date": {"on_or_before": end_iso}},
                {"property": "Activity Type", "select": {"equals": activity_type}},
                {"property": "Activity Name", "title": {"equals": activity_name}}
            ]
        }
    )
    
    results = query.get("results", [])
    if len(results) == 0:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        return (MULTIPLE_MATCH, [r['id'] for r in results])


def activity_needs_update(existing_activity, new_activity):
    """Check if existing activity needs update."""
    props = existing_activity.get('properties', {})
    
    # Check Garmin ID backfill
    garmin_id_prop = props.get('Garmin ID', {})
    existing_garmin_id = garmin_id_prop.get('number')
    new_garmin_id = new_activity.get('activityId')
    
    if existing_garmin_id is None and new_garmin_id is not None:
        return True
    
    # Check distance
    distance_prop = props.get('Distance (km)', {})
    existing_distance = distance_prop.get('number', 0) or 0
    new_distance = round(new_activity.get('distance', 0) / 1000, 2)
    
    if not approx_equal(existing_distance, new_distance):
        return True
    
    # Check duration
    duration_prop = props.get('Duration (min)', {})
    existing_duration = duration_prop.get('number', 0) or 0
    new_duration = round(new_activity.get('duration', 0) / 60, 2)
    
    if not approx_equal(existing_duration, new_duration, eps=0.1):
        return True
    
    return False


def create_activity(client, database_id, activity):
    """Create new activity in Notion."""
    garmin_id = activity.get('activityId')
    activity_date_gmt = activity.get('startTimeGMT')
    activity_name = format_entertainment(activity.get("activityName"))
    activity_type, activity_subtype = format_activity_type(
        activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    activity_date_local = convert_gmt_to_local(activity_date_gmt)
    icon_url = ACTIVITY_ICONS.get(activity_subtype if activity_subtype != activity_type else activity_type)
    
    props = {
        "Activity Name": {"title": [{"text": {"content": activity_name}}]},
        "Date": {"date": {"start": activity_date_local}},
        "Activity Type": {"select": {"name": activity_type}},
        "Subactivity Type": {"select": {"name": activity_subtype}},
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
    
    page = {"parent": {"database_id": database_id}, "properties": props}
    
    if icon_url:
        page["icon"] = {"type": "external", "external": {"url": icon_url}}
    
    try:
        client.pages.create(**page)
        return True
    except Exception as e:
        error_msg = str(e).lower()
        if any(x in error_msg for x in ["select", "is not a valid", "does not exist"]):
            props["Activity Type"] = {"select": {"name": "Unknown"}}
            props["Subactivity Type"] = {"select": {"name": "Unknown"}}
            try:
                client.pages.create(**{"parent": {"database_id": database_id}, "properties": props})
                return True
            except Exception as e2:
                print(f"    ERROR creating {activity_name}: {e2}")
                return False
        print(f"    ERROR creating {activity_name}: {e}")
        return False


def update_activity(client, existing_activity, new_activity):
    """Update existing activity."""
    garmin_id = new_activity.get('activityId')
    activity_date_gmt = new_activity.get('startTimeGMT')
    activity_name = format_entertainment(new_activity.get("activityName"))
    activity_type, activity_subtype = format_activity_type(
        new_activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    activity_date_local = convert_gmt_to_local(activity_date_gmt)
    icon_url = ACTIVITY_ICONS.get(activity_subtype if activity_subtype != activity_type else activity_type)
    
    props = {
        "Date": {"date": {"start": activity_date_local}},
        "Activity Type": {"select": {"name": activity_type}},
        "Subactivity Type": {"select": {"name": activity_subtype}},
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
    
    update = {"page_id": existing_activity['id'], "properties": props}
    
    if icon_url:
        update["icon"] = {"type": "external", "external": {"url": icon_url}}
    
    try:
        client.pages.update(**update)
        return True
    except Exception as e:
        print(f"    ERROR updating {activity_name}: {e}")
        return False


def sync_activities(garmin, notion, database_id, sync_days, sync_all):
    """Sync activities from Garmin to Notion."""
    print("\n" + "=" * 50)
    print("📊 SYNCING ACTIVITIES")
    print("=" * 50)
    
    if sync_all:
        print("Mode: FULL SYNC (all history)")
        activities = get_all_activities(garmin)
    else:
        print(f"Mode: Last {sync_days} days (+ 3 day overlap)")
        activities = get_recent_activities(garmin, days=sync_days)
    
    print(f"Found {len(activities)} activities to process")
    
    created = updated = unchanged = skipped = errors = 0
    
    for activity in activities:
        garmin_id = activity.get('activityId')
        activity_date_gmt = activity.get('startTimeGMT')
        activity_name = format_entertainment(activity.get('activityName', 'Unnamed Activity'))
        activity_type, _ = format_activity_type(
            activity.get('activityType', {}).get('typeKey', 'Unknown'),
            activity_name
        )
        
        existing = activity_exists_by_garmin_id(notion, database_id, garmin_id)
        
        if isinstance(existing, tuple) and existing[0] == MULTIPLE_MATCH:
            skipped += 1
            continue
        
        if not existing:
            existing = activity_exists_by_date_fallback(
                notion, database_id, activity_date_gmt, activity_type, activity_name
            )
            if isinstance(existing, tuple) and existing[0] == MULTIPLE_MATCH:
                skipped += 1
                continue
        
        if existing:
            if activity_needs_update(existing, activity):
                if update_activity(notion, existing, activity):
                    updated += 1
                    print(f"  UPDATED: {activity_name}")
                else:
                    errors += 1
            else:
                unchanged += 1
        else:
            if create_activity(notion, database_id, activity):
                created += 1
                print(f"  CREATED: {activity_name}")
            else:
                errors += 1
    
    print(f"\n✅ Activities: {created} created, {updated} updated, {unchanged} unchanged, {skipped} skipped, {errors} errors")
    return created, updated, errors


# =============================================================================
# PERSONAL RECORDS SYNC
# =============================================================================

def get_icon_for_record(activity_name):
    icon_map = {
        "1K": "🥇", "1mi": "⚡", "5K": "👟", "10K": "⭐",
        "Longest Run": "🏃", "Longest Ride": "🚴", "Total Ascent": "🚵",
        "Max Avg Power (20 min)": "🔋", "Most Steps in a Day": "👣",
        "Most Steps in a Week": "🚶", "Most Steps in a Month": "📅",
        "Longest Goal Streak": "✔️"
    }
    return icon_map.get(activity_name, "🏅")


def replace_activity_name_by_typeId(typeId):
    typeId_name_map = {
        1: "1K", 2: "1mi", 3: "5K", 4: "10K",
        7: "Longest Run", 8: "Longest Ride", 9: "Total Ascent",
        10: "Max Avg Power (20 min)", 12: "Most Steps in a Day",
        13: "Most Steps in a Week", 14: "Most Steps in a Month",
        15: "Longest Goal Streak"
    }
    return typeId_name_map.get(typeId, "Unnamed Activity")


def format_garmin_pr_value(value, typeId):
    """Format PR value based on type."""
    if typeId == 1:  # 1K
        total_seconds = round(value)
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:02d} /km", f"{minutes}:{seconds:02d} /km"
    
    if typeId == 2:  # 1mi
        total_seconds = round(value)
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        pace_seconds = total_seconds / 1.60934
        pmin = int(pace_seconds // 60)
        psec = int(pace_seconds % 60)
        return f"{minutes}:{seconds:02d}", f"{pmin}:{psec:02d} /km"
    
    if typeId == 3:  # 5K
        total_seconds = round(value)
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        pace_seconds = total_seconds // 5
        pmin = pace_seconds // 60
        psec = pace_seconds % 60
        return f"{minutes}:{seconds:02d}", f"{pmin}:{psec:02d} /km"
    
    if typeId == 4:  # 10K
        total_seconds = round(value)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0:
            formatted = f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            formatted = f"{minutes}:{seconds:02d}"
        pace_seconds = total_seconds // 10
        pmin = pace_seconds // 60
        psec = pace_seconds % 60
        return formatted, f"{pmin}:{psec:02d} /km"
    
    if typeId in [7, 8]:  # Longest Run/Ride
        return f"{value / 1000:.2f} km", ""
    
    if typeId == 9:  # Total Ascent
        return f"{int(value):,} m", ""
    
    if typeId == 10:  # Max Avg Power
        return f"{round(value)} W", ""
    
    if typeId in [12, 13, 14]:  # Steps
        return f"{round(value):,}", ""
    
    if typeId == 15:  # Goal Streak
        return f"{round(value)} days", ""
    
    return str(value), ""


def sync_personal_records(garmin, notion, database_id):
    """Sync personal records from Garmin to Notion."""
    print("\n" + "=" * 50)
    print("🏆 SYNCING PERSONAL RECORDS")
    print("=" * 50)
    
    if not database_id:
        print("⚠️ NOTION_PR_DB_ID not set, skipping")
        return 0, 0, 0
    
    records = garmin.get_personal_record()
    filtered_records = [r for r in records if r.get('typeId') != 16]
    
    print(f"Found {len(filtered_records)} personal records")
    
    created = updated = errors = 0
    
    for record in filtered_records:
        activity_date = record.get('prStartTimeGmtFormatted')
        activity_type = (record.get('activityType') or 'Walking').replace('_', ' ').title()
        activity_name = replace_activity_name_by_typeId(record.get('typeId'))
        typeId = record.get('typeId', 0)
        value, pace = format_garmin_pr_value(record.get('value', 0), typeId)
        
        # Check if exists
        query = notion.databases.query(
            database_id=database_id,
            filter={
                "and": [
                    {"property": "Record", "title": {"equals": activity_name}},
                    {"property": "PR", "checkbox": {"equals": True}}
                ]
            }
        )
        existing = query['results'][0] if query['results'] else None
        
        if existing:
            try:
                existing_date = existing['properties']['Date']['date']['start']
                if activity_date and activity_date > existing_date:
                    # Archive old, create new
                    notion.pages.update(
                        page_id=existing['id'],
                        properties={"PR": {"checkbox": False}}
                    )
                    notion.pages.create(
                        parent={"database_id": database_id},
                        properties={
                            "Date": {"date": {"start": activity_date}},
                            "Activity Type": {"select": {"name": activity_type}},
                            "Record": {"title": [{"text": {"content": activity_name}}]},
                            "typeId": {"number": typeId},
                            "PR": {"checkbox": True},
                            "Value": {"rich_text": [{"text": {"content": value}}]},
                            "Pace": {"rich_text": [{"text": {"content": pace}}]}
                        },
                        icon={"emoji": get_icon_for_record(activity_name)}
                    )
                    created += 1
                    print(f"  NEW PR: {activity_name}")
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"  ERROR: {activity_name}: {e}")
        else:
            try:
                notion.pages.create(
                    parent={"database_id": database_id},
                    properties={
                        "Date": {"date": {"start": activity_date}},
                        "Activity Type": {"select": {"name": activity_type}},
                        "Record": {"title": [{"text": {"content": activity_name}}]},
                        "typeId": {"number": typeId},
                        "PR": {"checkbox": True},
                        "Value": {"rich_text": [{"text": {"content": value}}]},
                        "Pace": {"rich_text": [{"text": {"content": pace}}]}
                    },
                    icon={"emoji": get_icon_for_record(activity_name)}
                )
                created += 1
                print(f"  CREATED: {activity_name}")
            except Exception as e:
                errors += 1
                print(f"  ERROR: {activity_name}: {e}")
    
    print(f"\n✅ Personal Records: {created} created/updated, {errors} errors")
    return created, updated, errors


# =============================================================================
# DAILY STEPS SYNC
# =============================================================================

def sync_daily_steps(garmin, notion, database_id, sync_days, sync_all):
    """Sync daily steps from Garmin to Notion."""
    print("\n" + "=" * 50)
    print("👣 SYNCING DAILY STEPS")
    print("=" * 50)
    
    if not database_id:
        print("⚠️ NOTION_STEPS_DB_ID not set, skipping")
        return 0, 0, 0
    
    days_to_fetch = 730 if sync_all else sync_days
    print(f"Checking last {days_to_fetch} days")
    
    today = datetime.now().date()
    created = skipped = errors = 0
    
    for i in range(days_to_fetch):
        current_date = today - timedelta(days=i)
        date_str = current_date.isoformat()
        
        try:
            # Check if exists
            query = notion.databases.query(
                database_id=database_id,
                filter={"property": "Date", "date": {"equals": date_str}}
            )
            if query["results"]:
                skipped += 1
                continue
            
            steps_data = garmin.get_user_summary(date_str)
            
            if steps_data and steps_data.get("totalSteps", 0) > 0:
                notion.pages.create(
                    parent={"database_id": database_id},
                    properties={
                        "Activity Type": {"title": [{"text": {"content": "Daily Steps"}}]},
                        "Date": {"date": {"start": date_str}},
                        "Total Steps": {"number": steps_data.get("totalSteps", 0)},
                        "Total Distance (km)": {"number": round(steps_data.get("totalDistanceMeters", 0) / 1000, 2)},
                        "Step Goal": {"number": steps_data.get("dailyStepGoal", 0)}
                    }
                )
                created += 1
                print(f"  CREATED: {date_str} ({steps_data.get('totalSteps', 0)} steps)")
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            if "404" not in str(e):
                print(f"  ERROR {date_str}: {e}")
    
    print(f"\n✅ Daily Steps: {created} created, {skipped} skipped, {errors} errors")
    return created, skipped, errors


# =============================================================================
# SLEEP DATA SYNC
# =============================================================================

def sync_sleep_data(garmin, notion, database_id, sync_days, sync_all):
    """Sync sleep data from Garmin to Notion."""
    print("\n" + "=" * 50)
    print("😴 SYNCING SLEEP DATA")
    print("=" * 50)
    
    if not database_id:
        print("⚠️ NOTION_SLEEP_DB_ID not set, skipping")
        return 0, 0, 0
    
    days_to_fetch = 730 if sync_all else sync_days
    print(f"Checking last {days_to_fetch} days")
    
    today = datetime.now().date()
    created = skipped = errors = 0
    
    for i in range(days_to_fetch):
        current_date = today - timedelta(days=i)
        date_str = current_date.isoformat()
        
        try:
            # Check if exists
            query = notion.databases.query(
                database_id=database_id,
                filter={"property": "Long Date", "date": {"equals": date_str}}
            )
            if query["results"]:
                skipped += 1
                continue
            
            sleep_data = garmin.get_sleep_data(date_str)
            
            if sleep_data and sleep_data.get("dailySleepDTO"):
                daily = sleep_data.get("dailySleepDTO", {})
                deep = daily.get("deepSleepSeconds", 0) or 0
                light = daily.get("lightSleepSeconds", 0) or 0
                rem = daily.get("remSleepSeconds", 0) or 0
                awake = daily.get("awakeSleepSeconds", 0) or 0
                total = deep + light + rem
                
                if total > 0:
                    # Format times
                    times_str = ""
                    start = daily.get("sleepStartTimestampLocal")
                    end = daily.get("sleepEndTimestampLocal")
                    if start and end:
                        try:
                            start_t = datetime.fromisoformat(start.replace("Z", "")).strftime("%H:%M")
                            end_t = datetime.fromisoformat(end.replace("Z", "")).strftime("%H:%M")
                            times_str = f"{start_t} - {end_t}"
                        except:
                            pass
                    
                    notion.pages.create(
                        parent={"database_id": database_id},
                        properties={
                            "Date": {"title": [{"text": {"content": date_str}}]},
                            "Long Date": {"date": {"start": date_str}},
                            "Times": {"rich_text": [{"text": {"content": times_str}}]},
                            "Total Sleep": {"rich_text": [{"text": {"content": format_duration(total)}}]},
                            "Total Sleep (h)": {"number": seconds_to_hours(total)},
                            "Deep Sleep": {"rich_text": [{"text": {"content": format_duration(deep)}}]},
                            "Deep Sleep (h)": {"number": seconds_to_hours(deep)},
                            "Light Sleep": {"rich_text": [{"text": {"content": format_duration(light)}}]},
                            "Light Sleep (h)": {"number": seconds_to_hours(light)},
                            "REM Sleep": {"rich_text": [{"text": {"content": format_duration(rem)}}]},
                            "REM Sleep (h)": {"number": seconds_to_hours(rem)},
                            "Awake Time": {"rich_text": [{"text": {"content": format_duration(awake)}}]},
                            "Awake Time (h)": {"number": seconds_to_hours(awake)},
                            "Resting HR": {"number": daily.get("restingHeartRate", 0) or 0},
                            "Sleep Goal": {"checkbox": total >= 25200}  # 7 hours
                        }
                    )
                    created += 1
                    print(f"  CREATED: {date_str} ({seconds_to_hours(total)}h)")
                else:
                    skipped += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            if "404" not in str(e):
                print(f"  ERROR {date_str}: {e}")
    
    print(f"\n✅ Sleep Data: {created} created, {skipped} skipped, {errors} errors")
    return created, skipped, errors


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("🚀 GARMIN TO NOTION UNIFIED SYNC")
    print("=" * 60)
    
    # Configuration
    sync_days = int(os.getenv("SYNC_DAYS", "7"))
    sync_all = os.getenv("SYNC_ALL", "false").lower() == "true"
    
    print(f"\n📋 Configuration:")
    print(f"   SYNC_DAYS: {sync_days}")
    print(f"   SYNC_ALL: {sync_all}")
    
    # Initialize clients
    print("\n🔌 Initializing clients...")
    
    try:
        garmin = init_garmin_client()
    except Exception as e:
        print(f"❌ Garmin login failed: {e}")
        sys.exit(1)
    
    notion_token = os.getenv("NOTION_TOKEN")
    if not notion_token:
        print("❌ NOTION_TOKEN not set")
        sys.exit(1)
    
    notion = Client(auth=notion_token)
    print("✅ Notion client initialized")
    
    # Get database IDs
    activities_db = os.getenv("NOTION_DB_ID")
    pr_db = os.getenv("NOTION_PR_DB_ID")
    steps_db = os.getenv("NOTION_STEPS_DB_ID")
    sleep_db = os.getenv("NOTION_SLEEP_DB_ID")
    
    if not activities_db:
        print("❌ NOTION_DB_ID not set")
        sys.exit(1)
    
    # Run all syncs with the SAME Garmin session
    total_created = 0
    total_errors = 0
    
    # 1. Activities (main)
    c, u, e = sync_activities(garmin, notion, activities_db, sync_days, sync_all)
    total_created += c
    total_errors += e
    
    # 2. Personal Records
    c, u, e = sync_personal_records(garmin, notion, pr_db)
    total_created += c
    total_errors += e
    
    # 3. Daily Steps
    c, s, e = sync_daily_steps(garmin, notion, steps_db, sync_days, sync_all)
    total_created += c
    total_errors += e
    
    # 4. Sleep Data
    c, s, e = sync_sleep_data(garmin, notion, sleep_db, sync_days, sync_all)
    total_created += c
    total_errors += e
    
    # Final summary
    print("\n" + "=" * 60)
    print("🏁 SYNC COMPLETE")
    print("=" * 60)
    print(f"   Total created/updated: {total_created}")
    print(f"   Total errors: {total_errors}")
    
    if total_errors > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
