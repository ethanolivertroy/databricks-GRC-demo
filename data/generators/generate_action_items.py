"""Generate mock action item data for remediation tracking."""
import csv
import random
from datetime import datetime, timedelta
import uuid

STATUSES = ["Open", "In Progress", "Blocked", "Closed", "Cancelled"]
PRIORITIES = ["Critical", "High", "Medium", "Low"]
ACTION_TYPES = ["Remediation", "Implementation", "Documentation", "Training", "Review"]
FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery", "Cameron", "Drew"]
LAST_NAMES = ["Chen", "Patel", "Williams", "Garcia", "Kim", "Johnson", "Brown", "Davis", "Rodriguez", "Martinez"]

def random_date(start_days_ago, end_days_from_now):
    """Generate random date. Positive = days ago, negative = days in future."""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() + timedelta(days=abs(end_days_from_now)) if end_days_from_now < 0 else datetime.now() - timedelta(days=end_days_from_now)
    if start > end:
        start, end = end, start
    delta = end - start
    random_days = random.randint(0, max(1, delta.days))
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

def generate_email(first, last):
    return f"{first.lower()}.{last.lower()}@company.com"

def generate_action_items(num_items=100):
    items = []
    control_families = ["AC", "AT", "AU", "CA", "CM", "CP", "IA", "IR", "MA", "MP", "PE", "PL", "PM", "PS", "RA", "SA", "SC", "SI", "SR"]

    action_descriptions = [
        "Implement multi-factor authentication",
        "Update access control policy",
        "Configure audit logging",
        "Document incident response procedure",
        "Review user access permissions",
        "Update firewall rules",
        "Implement encryption at rest",
        "Configure backup retention",
        "Update security training materials",
        "Review vendor contracts",
        "Implement data classification",
        "Update password policy",
        "Configure SIEM alerts",
        "Document recovery procedures",
        "Review network segmentation",
    ]

    for i in range(num_items):
        status = random.choices(STATUSES, weights=[30, 25, 10, 30, 5])[0]
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        created_date = random_date(180, 30)
        due_date = random_date(-30, -90) if status in ["Closed", "Cancelled"] else random_date(-60, 60)

        closed_date = ""
        if status == "Closed":
            closed_date = random_date(30, 0)

        items.append({
            "action_id": f"ACT-{uuid.uuid4().hex[:8].upper()}",
            "title": random.choice(action_descriptions),
            "description": f"Action item for {random.choice(control_families)} control remediation",
            "control_id": f"{random.choice(control_families)}-{random.randint(1, 20)}",
            "system_id": f"SYS-{random.randint(1, 50):03d}",
            "action_type": random.choice(ACTION_TYPES),
            "priority": random.choice(PRIORITIES),
            "status": status,
            "owner_name": f"{first} {last}",
            "owner_email": generate_email(first, last),
            "created_date": created_date,
            "due_date": due_date,
            "closed_date": closed_date,
            "estimated_hours": random.randint(4, 80),
            "actual_hours": random.randint(4, 100) if status == "Closed" else "",
            "blocked_by": f"ACT-{uuid.uuid4().hex[:8].upper()}" if status == "Blocked" else "",
            "notes": ""
        })

    return items

if __name__ == "__main__":
    items = generate_action_items(100)

    with open("data/mock/action_items.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=items[0].keys())
        writer.writeheader()
        writer.writerows(items)

    print(f"Generated {len(items)} action items")
