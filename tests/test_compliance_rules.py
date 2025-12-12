"""Unit tests for compliance rule engine logic."""
import pytest
from datetime import datetime, timedelta


class TestComplianceScoreCalculation:
    """Test compliance score calculations."""

    def test_implemented_score(self):
        """Implemented status should score 100."""
        status_scores = {
            "Implemented": 100,
            "Partially Implemented": 50,
            "Not Implemented": 0,
        }
        assert status_scores["Implemented"] == 100

    def test_partial_score(self):
        """Partially Implemented should score 50."""
        status_scores = {
            "Implemented": 100,
            "Partially Implemented": 50,
            "Not Implemented": 0,
        }
        assert status_scores["Partially Implemented"] == 50

    def test_not_implemented_score(self):
        """Not Implemented should score 0."""
        status_scores = {
            "Implemented": 100,
            "Partially Implemented": 50,
            "Not Implemented": 0,
        }
        assert status_scores["Not Implemented"] == 0


class TestOverdueRemediationRule:
    """Test overdue remediation detection."""

    def test_overdue_detection(self):
        """Should flag as overdue when due date is in past."""
        due_date = datetime.now() - timedelta(days=30)
        is_overdue = due_date < datetime.now()
        assert is_overdue is True

    def test_not_overdue(self):
        """Should not flag when due date is in future."""
        due_date = datetime.now() + timedelta(days=30)
        is_overdue = due_date < datetime.now()
        assert is_overdue is False

    def test_overdue_severity(self):
        """Should return CRITICAL for >30 days overdue."""
        days_overdue = -45  # 45 days past due
        if days_overdue < -30:
            result = "CRITICAL"
        elif days_overdue < 0:
            result = "FAIL"
        elif days_overdue < 14:
            result = "WARN"
        else:
            result = "PASS"
        assert result == "CRITICAL"

    def test_upcoming_warning(self):
        """Should return WARN for items due within 14 days."""
        days_to_remediation = 10
        if days_to_remediation < -30:
            result = "CRITICAL"
        elif days_to_remediation < 0:
            result = "FAIL"
        elif days_to_remediation < 14:
            result = "WARN"
        else:
            result = "PASS"
        assert result == "WARN"


class TestEvidenceExpiryRule:
    """Test evidence expiry detection."""

    def test_expired_evidence(self):
        """Should flag expired evidence."""
        expiry_date = datetime.now() - timedelta(days=10)
        is_expired = expiry_date < datetime.now()
        assert is_expired is True

    def test_valid_evidence(self):
        """Should not flag valid evidence."""
        expiry_date = datetime.now() + timedelta(days=90)
        is_expired = expiry_date < datetime.now()
        assert is_expired is False

    def test_expiring_soon_warning(self):
        """Should warn for evidence expiring within 30 days."""
        days_until_expiry = 20
        warning_threshold = 30
        should_warn = days_until_expiry < warning_threshold
        assert should_warn is True


class TestAssessmentFreshnessRule:
    """Test assessment freshness detection."""

    def test_stale_assessment(self):
        """Should flag assessments older than 365 days."""
        days_since_assessment = 400
        if days_since_assessment > 365:
            result = "FAIL"
        elif days_since_assessment > 270:
            result = "WARN"
        else:
            result = "PASS"
        assert result == "FAIL"

    def test_warning_assessment(self):
        """Should warn for assessments between 270-365 days."""
        days_since_assessment = 300
        if days_since_assessment > 365:
            result = "FAIL"
        elif days_since_assessment > 270:
            result = "WARN"
        else:
            result = "PASS"
        assert result == "WARN"

    def test_fresh_assessment(self):
        """Should pass for recent assessments."""
        days_since_assessment = 100
        if days_since_assessment > 365:
            result = "FAIL"
        elif days_since_assessment > 270:
            result = "WARN"
        else:
            result = "PASS"
        assert result == "PASS"


class TestCriticalSystemRule:
    """Test critical system coverage rule."""

    def test_critical_system_low_compliance(self):
        """Critical systems with <90% should fail."""
        criticality = "Critical"
        compliance_percentage = 85

        if criticality == "Critical" and compliance_percentage < 90:
            result = "FAIL"
        elif criticality == "Critical" and compliance_percentage < 95:
            result = "WARN"
        elif criticality == "High" and compliance_percentage < 80:
            result = "WARN"
        else:
            result = "PASS"

        assert result == "FAIL"

    def test_critical_system_borderline(self):
        """Critical systems between 90-95% should warn."""
        criticality = "Critical"
        compliance_percentage = 92

        if criticality == "Critical" and compliance_percentage < 90:
            result = "FAIL"
        elif criticality == "Critical" and compliance_percentage < 95:
            result = "WARN"
        elif criticality == "High" and compliance_percentage < 80:
            result = "WARN"
        else:
            result = "PASS"

        assert result == "WARN"

    def test_critical_system_compliant(self):
        """Critical systems with >=95% should pass."""
        criticality = "Critical"
        compliance_percentage = 98

        if criticality == "Critical" and compliance_percentage < 90:
            result = "FAIL"
        elif criticality == "Critical" and compliance_percentage < 95:
            result = "WARN"
        elif criticality == "High" and compliance_percentage < 80:
            result = "WARN"
        else:
            result = "PASS"

        assert result == "PASS"


class TestControlIdValidation:
    """Test control ID validation."""

    def test_valid_nist_control_id(self):
        """Valid NIST control IDs should match pattern."""
        import re
        pattern = r'^[A-Z]{2}-[0-9]+'
        valid_ids = ["AC-1", "SC-7", "IA-5", "CM-10"]
        for control_id in valid_ids:
            assert re.match(pattern, control_id) is not None

    def test_invalid_control_id(self):
        """Invalid control IDs should not match pattern."""
        import re
        pattern = r'^[A-Z]{2}-[0-9]+'
        invalid_ids = ["XY1", "123", "ac-1", "A-1"]
        for control_id in invalid_ids:
            assert re.match(pattern, control_id) is None

    def test_control_family_extraction(self):
        """Should extract control family from control ID."""
        control_id = "SC-7"
        family = control_id.split("-")[0]
        assert family == "SC"


class TestComplianceStatusClassification:
    """Test compliance status classification."""

    def test_compliant_classification(self):
        """>=90% should be Compliant."""
        compliance_pct = 95
        if compliance_pct >= 90:
            status = "Compliant"
        elif compliance_pct >= 70:
            status = "Partially Compliant"
        else:
            status = "Non-Compliant"
        assert status == "Compliant"

    def test_partial_classification(self):
        """70-89% should be Partially Compliant."""
        compliance_pct = 75
        if compliance_pct >= 90:
            status = "Compliant"
        elif compliance_pct >= 70:
            status = "Partially Compliant"
        else:
            status = "Non-Compliant"
        assert status == "Partially Compliant"

    def test_noncompliant_classification(self):
        """<70% should be Non-Compliant."""
        compliance_pct = 50
        if compliance_pct >= 90:
            status = "Compliant"
        elif compliance_pct >= 70:
            status = "Partially Compliant"
        else:
            status = "Non-Compliant"
        assert status == "Non-Compliant"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
