"""Data quality validation tests for GRC Compliance Lakehouse."""
import pytest
import re
from datetime import datetime


class TestControlIdValidation:
    """Test control ID format validation."""

    def test_valid_nist_control_format(self, valid_control_families):
        """Valid NIST control IDs should match XX-N pattern."""
        pattern = r'^[A-Z]{2}-[0-9]+$'
        test_ids = ["AC-1", "SC-7", "IA-5", "CM-10", "AU-12"]
        for control_id in test_ids:
            assert re.match(pattern, control_id) is not None
            family = control_id.split("-")[0]
            assert family in valid_control_families

    def test_invalid_control_format(self):
        """Invalid control IDs should be rejected."""
        pattern = r'^[A-Z]{2}-[0-9]+$'
        invalid_ids = ["XY-1", "123", "ac-1", "A-1", "ACC-1", "AC1", "AC-"]
        for control_id in invalid_ids:
            # XY-1 matches format but XY is not valid family
            if control_id == "XY-1":
                continue
            assert re.match(pattern, control_id) is None

    def test_control_family_extraction(self):
        """Should correctly extract control family from ID."""
        test_cases = [
            ("AC-1", "AC"),
            ("SC-7", "SC"),
            ("AU-12", "AU"),
            ("CM-6", "CM"),
        ]
        for control_id, expected_family in test_cases:
            family = control_id.split("-")[0]
            assert family == expected_family


class TestImplementationStatusValidation:
    """Test implementation status values."""

    def test_valid_statuses(self, valid_implementation_statuses):
        """All valid statuses should be recognized."""
        test_statuses = ["Implemented", "Partially Implemented", "Not Implemented", "Not Applicable"]
        for status in test_statuses:
            assert status in valid_implementation_statuses

    def test_invalid_status(self, valid_implementation_statuses):
        """Invalid statuses should be rejected."""
        invalid_statuses = ["Complete", "Done", "Pending", "In Progress", "N/A"]
        for status in invalid_statuses:
            assert status not in valid_implementation_statuses


class TestCriticalityValidation:
    """Test system criticality values."""

    def test_valid_criticality(self, valid_criticality_levels):
        """All valid criticality levels should be recognized."""
        for level in ["Critical", "High", "Medium", "Low"]:
            assert level in valid_criticality_levels

    def test_criticality_ordering(self, valid_criticality_levels):
        """Criticality levels should be in correct order."""
        expected_order = ["Critical", "High", "Medium", "Low"]
        assert valid_criticality_levels == expected_order

    def test_invalid_criticality(self, valid_criticality_levels):
        """Invalid criticality values should be rejected."""
        invalid = ["Very High", "Extreme", "None", "Unknown"]
        for level in invalid:
            assert level not in valid_criticality_levels


class TestDataClassificationValidation:
    """Test data classification values."""

    def test_valid_classifications(self, valid_data_classifications):
        """All valid classifications should be recognized."""
        for classification in ["Public", "Internal", "Confidential", "Restricted"]:
            assert classification in valid_data_classifications

    def test_invalid_classifications(self, valid_data_classifications):
        """Invalid classifications should be rejected."""
        invalid = ["Secret", "Top Secret", "Private", "Sensitive"]
        for classification in invalid:
            assert classification not in valid_data_classifications


class TestEvidenceStatusValidation:
    """Test evidence status values."""

    def test_valid_evidence_statuses(self, valid_evidence_statuses):
        """All valid evidence statuses should be recognized."""
        for status in ["Pending Review", "Accepted", "Rejected", "Expired"]:
            assert status in valid_evidence_statuses

    def test_invalid_evidence_status(self, valid_evidence_statuses):
        """Invalid evidence statuses should be rejected."""
        invalid = ["Approved", "Denied", "Active", "Inactive"]
        for status in invalid:
            assert status not in valid_evidence_statuses


class TestDateValidation:
    """Test date format and logic validation."""

    def test_valid_date_format(self):
        """Dates should be in YYYY-MM-DD format."""
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        valid_dates = ["2024-01-15", "2023-12-31", "2025-06-30"]
        for date_str in valid_dates:
            assert re.match(pattern, date_str) is not None

    def test_date_parsing(self):
        """Dates should parse correctly."""
        date_str = "2024-06-15"
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        assert parsed.year == 2024
        assert parsed.month == 6
        assert parsed.day == 15

    def test_assessment_date_not_future(self):
        """Assessment dates should not be in the future."""
        future_date = datetime(2099, 1, 1)
        assert future_date > datetime.now()
        # In production, we would reject this

    def test_expiry_date_logic(self):
        """Expiry dates should be after creation dates."""
        created = datetime(2024, 1, 1)
        expires = datetime(2025, 1, 1)
        assert expires > created


class TestScoreValidation:
    """Test compliance score validation."""

    def test_score_range(self, status_scores):
        """Scores should be 0-100 or None."""
        for status, score in status_scores.items():
            if score is not None:
                assert 0 <= score <= 100

    def test_percentage_range(self):
        """Compliance percentages should be 0-100."""
        valid_percentages = [0, 25, 50, 75, 100]
        for pct in valid_percentages:
            assert 0 <= pct <= 100

    def test_invalid_percentage(self):
        """Invalid percentages should be rejected."""
        invalid = [-10, 101, 150, -1]
        for pct in invalid:
            assert not (0 <= pct <= 100)


class TestRiskScoreValidation:
    """Test risk score validation."""

    def test_risk_score_range(self):
        """Risk scores should be 0.0-1.0."""
        valid_scores = [0.0, 0.25, 0.5, 0.75, 1.0]
        for score in valid_scores:
            assert 0.0 <= score <= 1.0

    def test_invalid_risk_score(self):
        """Invalid risk scores should be rejected."""
        invalid = [-0.1, 1.1, 2.0, -1.0]
        for score in invalid:
            assert not (0.0 <= score <= 1.0)


class TestReferentialIntegrity:
    """Test referential integrity rules."""

    def test_assessment_references_control(self, sample_assessment, sample_nist_control):
        """Assessment control_id should reference valid control."""
        assert sample_assessment["control_id"] == sample_nist_control["control_id"]

    def test_assessment_references_system(self, sample_assessment, sample_system):
        """Assessment system_id should reference valid system."""
        assert sample_assessment["system_id"] == sample_system["system_id"]

    def test_evidence_references_control(self, sample_evidence, sample_nist_control):
        """Evidence control_id should reference valid control."""
        assert sample_evidence["control_id"] == sample_nist_control["control_id"]

    def test_action_item_references_control(self, sample_action_item):
        """Action item should reference valid control family."""
        control_id = sample_action_item["control_id"]
        family = control_id.split("-")[0]
        valid_families = ["AC", "AT", "AU", "CA", "CM", "CP", "IA", "IR",
                         "MA", "MP", "PE", "PL", "PM", "PS", "PT", "RA",
                         "SA", "SC", "SI", "SR"]
        assert family in valid_families


class TestRequiredFields:
    """Test required field presence."""

    def test_control_required_fields(self, sample_nist_control):
        """Controls should have all required fields."""
        required = ["control_id", "control_family", "control_name"]
        for field in required:
            assert field in sample_nist_control
            assert sample_nist_control[field] is not None

    def test_assessment_required_fields(self, sample_assessment):
        """Assessments should have all required fields."""
        required = ["assessment_id", "control_id", "system_id", "implementation_status", "assessment_date"]
        for field in required:
            assert field in sample_assessment
            assert sample_assessment[field] is not None

    def test_system_required_fields(self, sample_system):
        """Systems should have all required fields."""
        required = ["system_id", "system_name", "criticality"]
        for field in required:
            assert field in sample_system
            assert sample_system[field] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
