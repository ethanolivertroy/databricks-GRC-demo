"""
Generate mock evidence records for GRC Lakehouse demo.
Simulates evidence uploads for compliance controls.
"""
import json
import uuid
import random
from datetime import datetime, timedelta
import os
import csv

# Configuration
NUM_EVIDENCE_RECORDS = 200
OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Evidence types with descriptions
EVIDENCE_TYPES = {
    "Screenshot": "Screen capture showing control implementation",
    "Configuration Export": "System configuration file or export",
    "Policy Document": "Formal policy or procedure document",
    "Audit Log": "System audit log or access log",
    "Access Review Report": "Periodic access review documentation",
    "Vulnerability Scan": "Vulnerability assessment report",
    "Penetration Test Report": "Security testing results",
    "Training Records": "Employee training completion records",
    "Change Management Ticket": "Change request and approval documentation",
    "Incident Report": "Security incident documentation",
    "Risk Assessment": "Risk assessment documentation",
    "Business Continuity Plan": "BCP/DR documentation",
    "Vendor Assessment": "Third-party vendor security assessment",
    "Encryption Certificate": "Certificate or key management documentation"
}

# Evidence statuses
EVIDENCE_STATUSES = ["Pending Review", "Accepted", "Rejected", "Expired"]

# Uploaders (system owners and compliance team)
UPLOADERS = [
    "compliance.team@nvidia-demo.com",
    "security.ops@nvidia-demo.com",
    "audit.team@nvidia-demo.com",
    "it.admin@nvidia-demo.com",
    "risk.mgmt@nvidia-demo.com",
    "grc.team@nvidia-demo.com"
]

# Reviewers
REVIEWERS = [
    "senior.auditor@nvidia-demo.com",
    "compliance.lead@nvidia-demo.com",
    "security.manager@nvidia-demo.com",
    "audit.manager@nvidia-demo.com",
    "grc.director@nvidia-demo.com"
]

# Review notes for different statuses
REVIEW_NOTES = {
    "Accepted": [
        "Evidence meets control requirements",
        "Documentation complete and current",
        "Evidence clearly demonstrates control effectiveness",
        "All required artifacts present",
        "Evidence validated against control objectives"
    ],
    "Rejected": [
        "Evidence does not cover full assessment period",
        "Missing required details or timestamps",
        "Evidence is for incorrect system",
        "Documentation is outdated",
        "Unable to verify control implementation from evidence",
        "Evidence quality insufficient for audit purposes"
    ],
    "Pending Review": [None],
    "Expired": [
        "Evidence validity period has passed",
        "Requires refresh for current assessment period"
    ]
}

def load_controls():
    """Load a subset of NIST controls for evidence mapping."""
    controls_file = os.path.join(OUTPUT_DIR, "frameworks", "nist_800_53_rev5_controls.csv")
    controls = []
    with open(controls_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Focus on controls that typically have evidence
            if row['baseline_moderate'] == "TRUE":
                controls.append(row['control_id'])
    return controls

def load_systems():
    """Load systems in certification scope."""
    systems_file = os.path.join(OUTPUT_DIR, "mock", "systems_inventory.csv")
    if not os.path.exists(systems_file):
        return []
    systems = []
    with open(systems_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['certification_scope'] != "None":
                systems.append({
                    "system_id": row['system_id'],
                    "system_name": row['system_name'],
                    "owner_email": row['owner_email']
                })
    return systems

def generate_evidence_path(evidence_type, control_id, system_name):
    """Generate a realistic file path for evidence."""
    safe_system = system_name.replace(" ", "_").lower()
    timestamp = datetime.now().strftime("%Y%m%d")

    extensions = {
        "Screenshot": "png",
        "Configuration Export": "json",
        "Policy Document": "pdf",
        "Audit Log": "csv",
        "Access Review Report": "xlsx",
        "Vulnerability Scan": "pdf",
        "Penetration Test Report": "pdf",
        "Training Records": "xlsx",
        "Change Management Ticket": "pdf",
        "Incident Report": "pdf",
        "Risk Assessment": "xlsx",
        "Business Continuity Plan": "pdf",
        "Vendor Assessment": "pdf",
        "Encryption Certificate": "pem"
    }

    ext = extensions.get(evidence_type, "pdf")
    return f"/evidence/{safe_system}/{control_id}_{evidence_type.replace(' ', '_').lower()}_{timestamp}.{ext}"

def generate_validity_period(status):
    """Generate assessment period dates."""
    today = datetime.now()

    if status == "Expired":
        # Expired evidence from previous period
        end_date = today - timedelta(days=random.randint(30, 180))
        start_date = end_date - timedelta(days=365)
    else:
        # Current evidence
        start_date = today - timedelta(days=random.randint(0, 180))
        end_date = start_date + timedelta(days=365)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def generate_upload_timestamp(start_date_str):
    """Generate upload timestamp within the validity period."""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    days_after = random.randint(0, 30)
    upload_time = start_date + timedelta(days=days_after, hours=random.randint(8, 18), minutes=random.randint(0, 59))
    return upload_time.strftime("%Y-%m-%dT%H:%M:%S")

def generate_review_timestamp(upload_timestamp_str, status):
    """Generate review timestamp after upload."""
    if status == "Pending Review":
        return None

    upload_time = datetime.strptime(upload_timestamp_str, "%Y-%m-%dT%H:%M:%S")
    days_after = random.randint(1, 14)
    review_time = upload_time + timedelta(days=days_after, hours=random.randint(8, 18))
    return review_time.strftime("%Y-%m-%dT%H:%M:%S")

def generate_evidence_records(controls, systems):
    """Generate evidence records."""
    evidence_records = []

    for _ in range(NUM_EVIDENCE_RECORDS):
        control_id = random.choice(controls)
        system = random.choice(systems)
        evidence_type = random.choice(list(EVIDENCE_TYPES.keys()))

        # Determine status with realistic distribution
        status = random.choices(
            EVIDENCE_STATUSES,
            weights=[25, 60, 10, 5]  # Most evidence is accepted
        )[0]

        start_date, end_date = generate_validity_period(status)
        upload_timestamp = generate_upload_timestamp(start_date)
        review_timestamp = generate_review_timestamp(upload_timestamp, status)

        # Uploader is usually system owner or compliance team
        uploader = system['owner_email'] if random.random() > 0.3 else random.choice(UPLOADERS)

        reviewer = random.choice(REVIEWERS) if status != "Pending Review" else None
        review_note = random.choice(REVIEW_NOTES[status])

        record = {
            "evidence_id": str(uuid.uuid4()),
            "control_id": control_id,
            "system_id": system['system_id'],
            "system_name": system['system_name'],
            "evidence_type": evidence_type,
            "evidence_description": EVIDENCE_TYPES[evidence_type],
            "evidence_path": generate_evidence_path(evidence_type, control_id, system['system_name']),
            "uploaded_by": uploader,
            "upload_timestamp": upload_timestamp,
            "assessment_period_start": start_date,
            "assessment_period_end": end_date,
            "status": status,
            "reviewer": reviewer,
            "review_timestamp": review_timestamp,
            "review_notes": review_note
        }
        evidence_records.append(record)

    return evidence_records

def write_json(records, filename):
    """Write evidence records to JSON file (simulating streaming input)."""
    filepath = os.path.join(OUTPUT_DIR, "mock", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"Generated {len(records)} evidence records -> {filepath}")
    return filepath

def write_csv(records, filename):
    """Write evidence records to CSV file."""
    filepath = os.path.join(OUTPUT_DIR, "mock", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='') as f:
        if records:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

    print(f"Generated {len(records)} evidence records (CSV) -> {filepath}")
    return filepath

def print_summary(records):
    """Print summary statistics."""
    print("\n=== Evidence Summary ===")

    # Status distribution
    status_counts = {}
    for r in records:
        status = r['status']
        status_counts[status] = status_counts.get(status, 0) + 1

    print("\nEvidence Status Distribution:")
    for status, count in sorted(status_counts.items()):
        pct = (count / len(records)) * 100
        print(f"  {status}: {count} ({pct:.1f}%)")

    # Evidence type distribution
    type_counts = {}
    for r in records:
        etype = r['evidence_type']
        type_counts[etype] = type_counts.get(etype, 0) + 1

    print("\nTop Evidence Types:")
    for etype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {etype}: {count}")

    # Coverage
    unique_controls = len(set(r['control_id'] for r in records))
    unique_systems = len(set(r['system_id'] for r in records))
    print(f"\nUnique controls covered: {unique_controls}")
    print(f"Unique systems covered: {unique_systems}")

if __name__ == "__main__":
    print("Loading control catalog...")
    controls = load_controls()
    print(f"Loaded {len(controls)} NIST controls (moderate baseline)")

    print("\nLoading systems in certification scope...")
    systems = load_systems()
    if not systems:
        print("No systems found. Please run generate_systems.py first.")
        exit(1)
    print(f"Loaded {len(systems)} systems in scope")

    print("\nGenerating evidence records...")
    evidence = generate_evidence_records(controls, systems)

    # Write both JSON (for streaming simulation) and CSV formats
    write_json(evidence, "evidence_records.json")
    write_csv(evidence, "evidence_records.csv")

    print_summary(evidence)
