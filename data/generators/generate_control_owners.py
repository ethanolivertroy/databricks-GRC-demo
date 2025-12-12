"""Generate mock control owner data."""
import csv
import random

CONTROL_FAMILIES = ["AC", "AT", "AU", "CA", "CM", "CP", "IA", "IR", "MA", "MP", "PE", "PL", "PM", "PS", "PT", "RA", "SA", "SC", "SI", "SR"]
BUSINESS_UNITS = ["Security", "IT Operations", "Compliance", "Engineering", "Infrastructure", "Data Science", "Legal", "Finance", "Human Resources"]
FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery", "Cameron", "Drew"]
LAST_NAMES = ["Chen", "Patel", "Williams", "Garcia", "Kim", "Johnson", "Brown", "Davis", "Rodriguez", "Martinez"]

def generate_email(first, last):
    return f"{first.lower()}.{last.lower()}@company.com"

def generate_control_owners():
    owners = []

    for family in CONTROL_FAMILIES:
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        backup_first = random.choice(FIRST_NAMES)
        backup_last = random.choice(LAST_NAMES)

        owners.append({
            "control_family_code": family,
            "owner_name": f"{first} {last}",
            "owner_email": generate_email(first, last),
            "backup_owner_name": f"{backup_first} {backup_last}",
            "backup_owner_email": generate_email(backup_first, backup_last),
            "business_unit": random.choice(BUSINESS_UNITS),
            "escalation_contact": generate_email(random.choice(FIRST_NAMES), random.choice(LAST_NAMES)),
            "last_review_date": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        })

    return owners

if __name__ == "__main__":
    owners = generate_control_owners()

    with open("data/mock/control_owners.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=owners[0].keys())
        writer.writeheader()
        writer.writerows(owners)

    print(f"Generated {len(owners)} control owner records")
