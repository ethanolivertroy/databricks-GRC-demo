"""Generate mock control exception/waiver data."""
import csv
import random
from datetime import datetime, timedelta
import uuid

EXCEPTION_STATUSES = ["Active", "Expired", "Pending Approval", "Revoked"]
EXCEPTION_REASONS = [
    "Technical limitation - system cannot support control",
    "Business requirement - control conflicts with operations",
    "Legacy system - scheduled for decommission",
    "Cost prohibitive - alternative compensating control in place",
    "Vendor limitation - third-party system constraint",
    "Temporary exception during migration",
]
FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery", "Cameron", "Drew"]
LAST_NAMES = ["Chen", "Patel", "Williams", "Garcia", "Kim", "Johnson", "Brown", "Davis", "Rodriguez", "Martinez"]
APPROVERS = ["CISO", "VP Security", "Director Compliance", "CTO", "VP Engineering"]

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

def generate_exceptions(num_exceptions=30):
    exceptions = []
    control_families = ["AC", "AT", "AU", "CA", "CM", "CP", "IA", "IR", "MA", "MP", "PE", "SC", "SI"]

    for i in range(num_exceptions):
        status = random.choices(EXCEPTION_STATUSES, weights=[50, 20, 20, 10])[0]
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        approver_first = random.choice(FIRST_NAMES)
        approver_last = random.choice(LAST_NAMES)

        request_date = random_date(365, 30)
        approval_date = random_date(30, 0) if status != "Pending Approval" else ""
        expiry_date = random_date(-180, 180) if status in ["Active", "Expired"] else ""

        exceptions.append({
            "exception_id": f"EXC-{uuid.uuid4().hex[:8].upper()}",
            "control_id": f"{random.choice(control_families)}-{random.randint(1, 15)}",
            "system_id": f"SYS-{random.randint(1, 50):03d}",
            "exception_reason": random.choice(EXCEPTION_REASONS),
            "business_justification": f"Business justification for exception to control requirements.",
            "compensating_control": f"Compensating control: {random.choice(['Manual review process', 'Enhanced monitoring', 'Physical security control', 'Alternative technical control', 'Increased audit frequency'])}",
            "risk_acceptance": random.choice(["Accepted", "Accepted with conditions", "Under review"]),
            "requested_by": f"{first} {last}",
            "requested_by_email": generate_email(first, last),
            "request_date": request_date,
            "approved_by": f"{approver_first} {approver_last}" if approval_date else "",
            "approved_by_title": random.choice(APPROVERS) if approval_date else "",
            "approval_date": approval_date,
            "expiry_date": expiry_date,
            "status": status,
            "review_frequency": random.choice(["Quarterly", "Semi-annually", "Annually"]),
            "last_review_date": random_date(90, 0) if status == "Active" else "",
            "notes": ""
        })

    return exceptions

if __name__ == "__main__":
    exceptions = generate_exceptions(30)

    with open("data/mock/control_exceptions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=exceptions[0].keys())
        writer.writeheader()
        writer.writerows(exceptions)

    print(f"Generated {len(exceptions)} control exceptions")
