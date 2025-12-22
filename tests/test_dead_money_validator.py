"""
Pytest test suite for Dead Money Validator.

Runs cross-validation tests on processed compensation data.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dead_money_validator import DeadMoneyValidator


class TestDeadMoneyValidation:
    """Test suite for dead money cross-validation."""

    @pytest.fixture
    def validator(self):
        """Create validator instance."""
        return DeadMoneyValidator(processed_dir="data/processed/compensation")

    def test_synthetic_players(self, validator):
        """Test synthetic player detection (should pass with informational reporting)."""
        result = validator.test_synthetic_players()
        
        assert result["status"] == "PASS", f"Synthetic test failed: {result}"
        assert "synthetic_records" in result
        assert "synthetic_pct" in result
        assert result["synthetic_pct"] > 0  # Should detect synthetic players
        assert "All data (synthetic + real) retained" in result.get("note", "")

    def test_team_player_reconciliation(self, validator):
        """Test team vs player dead money reconciliation."""
        result = validator.test_team_player_reconciliation_csv()
        
        # Should be PASS or WARN, not FAIL
        assert result["status"] in ["PASS", "WARN"], \
            f"Reconciliation test failed: {result['status']}"
        
        # Should have metrics
        assert "rows_compared" in result
        assert "mismatch_count" in result
        assert "tolerance_pct" in result

    def test_year_over_year_consistency(self, validator):
        """Test year-over-year consistency checks."""
        result = validator.test_year_over_year_consistency()
        
        # Should be PASS or WARN, not FAIL
        assert result["status"] in ["PASS", "WARN"], \
            f"YoY consistency test failed: {result['status']}"
        
        # Should have metrics
        assert "years_analyzed" in result
        assert "max_increase_pct" in result
        assert "max_decrease_pct" in result

    def test_all_tests_pass_or_warn(self, validator):
        """Test that all validator tests complete without failures."""
        validator.run_all_tests()
        results = validator.test_results
        
        # Count results by status
        statuses = [r["status"] for r in results.values()]
        
        # Should have no FAIL status
        assert "FAIL" not in statuses, \
            f"Validator returned FAIL: {[r for r in results.values() if r['status'] == 'FAIL']}"
        
        # Should have at least one PASS
        assert "PASS" in statuses, "No tests passed"

    def test_validator_exit_code_zero(self, validator):
        """Test that validator returns exit code 0."""
        validator.run_all_tests()
        exit_code = validator.print_summary()
        
        # Should exit with 0 (success) since tests are non-blocking
        assert exit_code == 0, f"Validator exited with code {exit_code}"


class TestDataAvailability:
    """Test that required data files exist."""

    def test_player_dead_money_csv_exists(self):
        """Test that player dead money CSV is available."""
        csv_path = Path("data/processed/compensation/player_dead_money.csv")
        assert csv_path.exists(), f"Missing: {csv_path}"

    def test_team_dead_money_csv_exists(self):
        """Test that team dead money CSV is available."""
        csv_path = Path("data/processed/compensation/team_dead_money_by_year.csv")
        assert csv_path.exists(), f"Missing: {csv_path}"

    def test_compensation_dir_exists(self):
        """Test that compensation directory exists."""
        comp_dir = Path("data/processed/compensation")
        assert comp_dir.exists(), f"Missing: {comp_dir}"
        assert comp_dir.is_dir(), f"Not a directory: {comp_dir}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
