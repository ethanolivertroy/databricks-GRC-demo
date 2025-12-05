"""
Generate mock systems inventory data for GRC Lakehouse demo.
Simulates a ServiceNow/CMDB export of systems in scope for compliance.
"""
import csv
import uuid
import random
from datetime import datetime, timedelta
import os

# Configuration
NUM_SYSTEMS = 50
OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Realistic system name patterns
SYSTEM_PREFIXES = [
    "Customer", "Employee", "Partner", "Vendor", "Financial",
    "HR", "Engineering", "Sales", "Marketing", "Legal",
    "Supply Chain", "Inventory", "Analytics", "Reporting", "Security"
]

SYSTEM_TYPES = [
    "Data Platform", "Portal", "API Gateway", "Database", "Service",
    "Application", "Dashboard", "Integration Hub", "Warehouse", "Lake",
    "CRM", "ERP", "HRIS", "SCM", "Analytics Engine"
]

BUSINESS_UNITS = [
    "Engineering", "Finance", "Human Resources", "IT Operations",
    "Sales", "Legal", "Marketing", "Customer Success", "Security",
    "Product", "Data Science", "Infrastructure", "Compliance"
]

ENVIRONMENTS = ["Production", "Staging", "Development", "DR"]

HOSTING_TYPES = [
    "Cloud-AWS", "Cloud-Azure", "Cloud-GCP", "On-Premises", "Hybrid"
]

DATA_CLASSIFICATIONS = ["Public", "Internal", "Confidential", "Restricted"]

CRITICALITY_LEVELS = ["Critical", "High", "Medium", "Low"]

CERTIFICATION_SCOPES = ["SOC2", "ISO27001", "Both", "None"]

# Owner name generation
FIRST_NAMES = [
    "James", "Sarah", "Michael", "Emily", "David", "Jessica", "Robert", "Ashley",
    "William", "Amanda", "Richard", "Jennifer", "Joseph", "Elizabeth", "Thomas", "Nicole",
    "Christopher", "Stephanie", "Daniel", "Melissa", "Matthew", "Michelle", "Andrew", "Kimberly"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin",
    "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson", "Walker"
]

def generate_system_name():
    """Generate a realistic system name."""
    prefix = random.choice(SYSTEM_PREFIXES)
    system_type = random.choice(SYSTEM_TYPES)
    return f"{prefix} {system_type}"

def generate_owner():
    """Generate a realistic owner name."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"

def generate_email(owner_name):
    """Generate email from owner name."""
    parts = owner_name.lower().split()
    return f"{parts[0]}.{parts[1]}@nvidia-demo.com"

def generate_last_assessment_date():
    """Generate a random last assessment date within the past 18 months."""
    days_ago = random.randint(1, 540)  # Up to 18 months ago
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

def generate_systems():
    """Generate mock systems inventory."""
    systems = []
    used_names = set()

    for _ in range(NUM_SYSTEMS):
        # Generate unique system name
        name = generate_system_name()
        while name in used_names:
            name = generate_system_name()
        used_names.add(name)

        owner = generate_owner()
        business_unit = random.choice(BUSINESS_UNITS)

        # Critical and Confidential/Restricted systems more likely to be in scope
        data_class = random.choices(
            DATA_CLASSIFICATIONS,
            weights=[5, 30, 40, 25]  # Weighted toward Confidential
        )[0]

        criticality = random.choices(
            CRITICALITY_LEVELS,
            weights=[15, 30, 35, 20]  # Weighted toward High/Medium
        )[0]

        # Systems with higher classification more likely in certification scope
        if data_class in ["Confidential", "Restricted"]:
            cert_scope = random.choices(
                CERTIFICATION_SCOPES,
                weights=[30, 20, 40, 10]
            )[0]
        else:
            cert_scope = random.choices(
                CERTIFICATION_SCOPES,
                weights=[15, 10, 15, 60]
            )[0]

        system = {
            "system_id": str(uuid.uuid4()),
            "system_name": name,
            "system_owner": owner,
            "owner_email": generate_email(owner),
            "business_unit": business_unit,
            "data_classification": data_class,
            "environment": random.choices(
                ENVIRONMENTS,
                weights=[60, 20, 15, 5]  # Mostly Production
            )[0],
            "hosting_type": random.choice(HOSTING_TYPES),
            "criticality": criticality,
            "certification_scope": cert_scope,
            "last_assessment_date": generate_last_assessment_date(),
            "created_date": (datetime.now() - timedelta(days=random.randint(365, 1825))).strftime("%Y-%m-%d"),
            "description": f"{name} supporting {business_unit} operations"
        }
        systems.append(system)

    return systems

def write_csv(systems, filename):
    """Write systems to CSV file."""
    filepath = os.path.join(OUTPUT_DIR, "mock", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='') as f:
        if systems:
            writer = csv.DictWriter(f, fieldnames=systems[0].keys())
            writer.writeheader()
            writer.writerows(systems)

    print(f"Generated {len(systems)} systems -> {filepath}")
    return filepath

if __name__ == "__main__":
    print("Generating mock systems inventory...")
    systems = generate_systems()
    output_file = write_csv(systems, "systems_inventory.csv")

    # Print sample
    print("\nSample systems:")
    for system in systems[:3]:
        print(f"  - {system['system_name']} ({system['criticality']}, {system['certification_scope']})")

    print(f"\nTotal systems generated: {len(systems)}")
    print(f"In SOC2 scope: {len([s for s in systems if 'SOC2' in s['certification_scope'] or s['certification_scope'] == 'Both'])}")
    print(f"In ISO27001 scope: {len([s for s in systems if 'ISO27001' in s['certification_scope'] or s['certification_scope'] == 'Both'])}")
