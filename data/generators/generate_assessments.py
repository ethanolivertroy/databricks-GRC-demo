"""
Generate mock control assessment data for GRC Lakehouse demo.
Simulates assessment results from internal audits and self-assessments.
"""
import csv
import uuid
import random
import json
from datetime import datetime, timedelta
import os

# Configuration
OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Assessment types
ASSESSMENT_TYPES = ["Self-Assessment", "Internal Audit", "External Audit", "Continuous Monitoring"]

# Implementation statuses with realistic distribution
STATUS_WEIGHTS = {
    "Implemented": 65,
    "Partially Implemented": 20,
    "Not Implemented": 10,
    "Not Applicable": 5
}

# Effectiveness ratings
EFFECTIVENESS_RATINGS = ["Effective", "Partially Effective", "Not Effective"]

# Gap descriptions for non-implemented controls
GAP_DESCRIPTIONS = [
    "Missing formal documentation of the control procedure",
    "Process exists but not consistently followed across all teams",
    "Control not yet implemented pending resource allocation",
    "Legacy system limitation prevents full implementation",
    "Pending vendor support for required functionality",
    "Manual process needs automation for compliance",
    "Policy documented but evidence collection incomplete",
    "Control partially automated, manual steps remain",
    "Third-party dependency blocking full implementation",
    "Awaiting management approval for implementation",
    "Training not yet completed for all relevant personnel",
    "Technical debt impacting control effectiveness",
    "Integration with monitoring tools incomplete",
    "Configuration drift detected, remediation in progress"
]

# Remediation plans
REMEDIATION_PLANS = [
    "Implement automated control monitoring by Q1 2025",
    "Document and formalize existing procedures",
    "Deploy configuration management solution",
    "Complete staff training program",
    "Upgrade legacy system to support required controls",
    "Engage vendor for implementation support",
    "Establish evidence collection workflow",
    "Implement automated compliance checks",
    "Deploy centralized logging solution",
    "Create exception handling process"
]

# Assessor names
ASSESSORS = [
    "Internal Audit Team",
    "Security Operations",
    "Compliance Team",
    "Risk Management",
    "External Auditor - Deloitte",
    "External Auditor - PwC",
    "External Auditor - KPMG",
    "GRC Team",
    "IT Security Team"
]

def load_controls():
    """Load NIST controls from the framework file."""
    controls_file = os.path.join(OUTPUT_DIR, "frameworks", "nist_800_53_rev5_controls.csv")
    controls = []
    with open(controls_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            controls.append(row)
    return controls

def load_systems():
    """Load systems from the mock data file."""
    systems_file = os.path.join(OUTPUT_DIR, "mock", "systems_inventory.csv")
    if not os.path.exists(systems_file):
        print("Systems file not found. Please run generate_systems.py first.")
        return []
    systems = []
    with open(systems_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            systems.append(row)
    return systems

def generate_assessment_date(system_last_assessment):
    """Generate assessment date based on system's last assessment."""
    base_date = datetime.strptime(system_last_assessment, "%Y-%m-%d")
    # Add some variance around the last assessment date
    variance = random.randint(-30, 30)
    assessment_date = base_date + timedelta(days=variance)
    return assessment_date.strftime("%Y-%m-%d")

def generate_remediation_due_date(assessment_date):
    """Generate remediation due date 30-180 days after assessment."""
    base_date = datetime.strptime(assessment_date, "%Y-%m-%d")
    days_ahead = random.randint(30, 180)
    due_date = base_date + timedelta(days=days_ahead)
    return due_date.strftime("%Y-%m-%d")

def should_assess_control(control, system):
    """Determine if a control should be assessed for a system based on baseline and scope."""
    # Skip if system not in certification scope
    if system['certification_scope'] == "None":
        return random.random() < 0.1  # Only 10% chance for out-of-scope systems

    # Check baseline relevance based on system criticality
    criticality = system['criticality']
    if criticality == "Critical":
        return control['baseline_high'] == "TRUE"
    elif criticality == "High":
        return control['baseline_moderate'] == "TRUE"
    else:
        return control['baseline_low'] == "TRUE"

def generate_assessments(controls, systems):
    """Generate assessment records for controls across systems."""
    assessments = []

    for system in systems:
        # Skip some out-of-scope systems entirely
        if system['certification_scope'] == "None" and random.random() > 0.2:
            continue

        # Sample controls for this system (not all controls for all systems)
        relevant_controls = [c for c in controls if should_assess_control(c, system)]

        # Only assess a subset (simulating real assessment scope)
        sample_size = min(len(relevant_controls), random.randint(20, 50))
        sampled_controls = random.sample(relevant_controls, sample_size)

        for control in sampled_controls:
            assessment_date = generate_assessment_date(system['last_assessment_date'])

            # Determine implementation status
            status = random.choices(
                list(STATUS_WEIGHTS.keys()),
                weights=list(STATUS_WEIGHTS.values())
            )[0]

            # Determine effectiveness based on status
            if status == "Implemented":
                effectiveness = random.choices(
                    EFFECTIVENESS_RATINGS,
                    weights=[80, 18, 2]
                )[0]
            elif status == "Partially Implemented":
                effectiveness = random.choices(
                    EFFECTIVENESS_RATINGS,
                    weights=[20, 70, 10]
                )[0]
            else:
                effectiveness = "Not Effective" if status == "Not Implemented" else None

            # Generate gap info for non-implemented controls
            gap_description = None
            remediation_plan = None
            remediation_due_date = None
            remediation_owner = None

            if status in ["Not Implemented", "Partially Implemented"]:
                gap_description = random.choice(GAP_DESCRIPTIONS)
                remediation_plan = random.choice(REMEDIATION_PLANS)
                remediation_due_date = generate_remediation_due_date(assessment_date)
                remediation_owner = system['system_owner']

            assessment = {
                "assessment_id": str(uuid.uuid4()),
                "control_id": control['control_id'],
                "control_family": control['control_family'],
                "system_id": system['system_id'],
                "system_name": system['system_name'],
                "assessment_date": assessment_date,
                "assessment_type": random.choices(
                    ASSESSMENT_TYPES,
                    weights=[40, 35, 15, 10]
                )[0],
                "assessor": random.choice(ASSESSORS),
                "implementation_status": status,
                "effectiveness_rating": effectiveness,
                "gap_description": gap_description,
                "remediation_plan": remediation_plan,
                "remediation_due_date": remediation_due_date,
                "remediation_owner": remediation_owner,
                "findings": f"Assessment of {control['control_id']} for {system['system_name']}"
            }
            assessments.append(assessment)

    return assessments

def write_csv(assessments, filename):
    """Write assessments to CSV file."""
    filepath = os.path.join(OUTPUT_DIR, "mock", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, 'w', newline='') as f:
        if assessments:
            writer = csv.DictWriter(f, fieldnames=assessments[0].keys())
            writer.writeheader()
            writer.writerows(assessments)

    print(f"Generated {len(assessments)} assessments -> {filepath}")
    return filepath

def print_summary(assessments):
    """Print summary statistics."""
    print("\n=== Assessment Summary ===")

    # Status distribution
    status_counts = {}
    for a in assessments:
        status = a['implementation_status']
        status_counts[status] = status_counts.get(status, 0) + 1

    print("\nImplementation Status Distribution:")
    for status, count in sorted(status_counts.items()):
        pct = (count / len(assessments)) * 100
        print(f"  {status}: {count} ({pct:.1f}%)")

    # Calculate overall compliance
    implemented = status_counts.get("Implemented", 0)
    partial = status_counts.get("Partially Implemented", 0)
    total_assessed = len([a for a in assessments if a['implementation_status'] != "Not Applicable"])

    compliance_pct = ((implemented + partial * 0.5) / total_assessed) * 100 if total_assessed > 0 else 0
    print(f"\nOverall Compliance: {compliance_pct:.1f}%")

    # Controls with gaps
    gaps = [a for a in assessments if a['gap_description']]
    print(f"Controls with gaps: {len(gaps)}")

    # Overdue remediations
    today = datetime.now().strftime("%Y-%m-%d")
    overdue = [a for a in assessments if a['remediation_due_date'] and a['remediation_due_date'] < today]
    print(f"Overdue remediations: {len(overdue)}")

if __name__ == "__main__":
    print("Loading control catalog...")
    controls = load_controls()
    print(f"Loaded {len(controls)} NIST controls")

    print("\nLoading systems inventory...")
    systems = load_systems()
    if not systems:
        print("No systems found. Please run generate_systems.py first.")
        exit(1)
    print(f"Loaded {len(systems)} systems")

    print("\nGenerating assessment records...")
    assessments = generate_assessments(controls, systems)
    output_file = write_csv(assessments, "control_assessments.csv")

    print_summary(assessments)
