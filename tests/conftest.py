"""Pytest fixtures for GRC Compliance Lakehouse tests."""
import pytest
from datetime import datetime, timedelta


@pytest.fixture
def status_scores():
    """Standard implementation status scores."""
    return {
        "Implemented": 100,
        "Partially Implemented": 50,
        "Not Implemented": 0,
        "Not Applicable": None,
    }


@pytest.fixture
def compliance_thresholds():
    """Compliance percentage thresholds."""
    return {
        "compliant": 90,
        "partially_compliant": 70,
    }


@pytest.fixture
def risk_thresholds():
    """Risk score thresholds."""
    return {
        "high": 0.7,
        "medium": 0.4,
    }


@pytest.fixture
def sample_nist_control():
    """Sample NIST 800-53 control record."""
    return {
        "control_id": "AC-1",
        "control_family": "AC",
        "control_name": "Policy and Procedures",
        "control_description": "Access control policy and procedures",
        "baseline_impact": "Low",
        "priority": "P1",
    }


@pytest.fixture
def sample_assessment():
    """Sample control assessment record."""
    return {
        "assessment_id": "ASMT-001",
        "control_id": "AC-1",
        "system_id": "SYS-001",
        "implementation_status": "Implemented",
        "assessment_date": datetime.now().strftime("%Y-%m-%d"),
        "assessor": "Test User",
        "evidence_reference": "EVD-001",
    }


@pytest.fixture
def sample_system():
    """Sample system inventory record."""
    return {
        "system_id": "SYS-001",
        "system_name": "Test System",
        "criticality": "High",
        "data_classification": "Confidential",
        "owner": "System Owner",
        "business_unit": "IT",
    }


@pytest.fixture
def sample_evidence():
    """Sample evidence record."""
    return {
        "evidence_id": "EVD-001",
        "control_id": "AC-1",
        "system_id": "SYS-001",
        "evidence_type": "Documentation",
        "description": "Access control policy document",
        "upload_date": datetime.now().strftime("%Y-%m-%d"),
        "expiry_date": (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d"),
        "status": "Accepted",
    }


@pytest.fixture
def sample_action_item():
    """Sample action item record."""
    return {
        "action_id": "ACT-001",
        "title": "Implement MFA",
        "control_id": "IA-2",
        "system_id": "SYS-001",
        "priority": "High",
        "status": "Open",
        "owner_name": "Test Owner",
        "due_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
    }


@pytest.fixture
def sample_exception():
    """Sample control exception record."""
    return {
        "exception_id": "EXC-001",
        "control_id": "SC-7",
        "system_id": "SYS-001",
        "exception_reason": "Legacy system limitation",
        "compensating_control": "Enhanced monitoring",
        "status": "Active",
        "expiry_date": (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d"),
    }


@pytest.fixture
def valid_control_families():
    """Valid NIST 800-53 control family codes."""
    return [
        "AC", "AT", "AU", "CA", "CM", "CP", "IA", "IR",
        "MA", "MP", "PE", "PL", "PM", "PS", "PT", "RA",
        "SA", "SC", "SI", "SR"
    ]


@pytest.fixture
def valid_criticality_levels():
    """Valid system criticality levels."""
    return ["Critical", "High", "Medium", "Low"]


@pytest.fixture
def valid_data_classifications():
    """Valid data classification levels."""
    return ["Public", "Internal", "Confidential", "Restricted"]


@pytest.fixture
def valid_implementation_statuses():
    """Valid control implementation statuses."""
    return ["Implemented", "Partially Implemented", "Not Implemented", "Not Applicable"]


@pytest.fixture
def valid_evidence_statuses():
    """Valid evidence statuses."""
    return ["Pending Review", "Accepted", "Rejected", "Expired"]
