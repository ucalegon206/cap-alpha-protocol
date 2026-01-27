#!/usr/bin/env python3
"""
Tests for core_models.py - Data Models & Idempotency Framework

Covers:
- ChecksumGenerator (dedup detection)
- DeduplicationEngine (identify & remove duplicates)
- ValidationGate (schema, nulls, types, ranges, uniqueness)
- IdempotentProcessor (full processing pipeline)
"""

import pytest
import pandas as pd
from pathlib import Path

from src.core_models import (
    ChecksumGenerator,
    DeduplicationEngine,
    ValidationGate,
    ValidationStatus,
    ProcessingStatus,
    IdempotentProcessor,
)


class TestChecksumGenerator:
    """Test checksum generation for dedup detection"""
    
    def test_generate_record_checksum_exact_match(self):
        """Checksums match for identical records"""
        record = {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024}
        key_fields = ['player_name', 'team', 'year']
        
        checksum1 = ChecksumGenerator.generate_record_checksum(record, key_fields)
        checksum2 = ChecksumGenerator.generate_record_checksum(record, key_fields)
        
        assert checksum1 == checksum2
        assert len(checksum1) == 32  # MD5 hex digest length
    
    def test_generate_record_checksum_case_insensitive(self):
        """Checksums match regardless of case"""
        record1 = {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024}
        record2 = {'player_name': 'patrick mahomes', 'team': 'kc', 'year': 2024}
        key_fields = ['player_name', 'team', 'year']
        
        checksum1 = ChecksumGenerator.generate_record_checksum(record1, key_fields)
        checksum2 = ChecksumGenerator.generate_record_checksum(record2, key_fields)
        
        assert checksum1 == checksum2, "Checksums should be case-insensitive"
    
    def test_generate_record_checksum_handles_nulls(self):
        """Checksums handle None/NaN values consistently"""
        record1 = {'player_name': 'Josh Allen', 'team': 'BUF', 'year': None}
        record2 = {'player_name': 'Josh Allen', 'team': 'BUF', 'year': pd.NA}
        key_fields = ['player_name', 'team', 'year']
        
        checksum1 = ChecksumGenerator.generate_record_checksum(record1, key_fields)
        checksum2 = ChecksumGenerator.generate_record_checksum(record2, key_fields)
        
        assert checksum1 == checksum2, "Null handling should be consistent"
    
    def test_generate_dataframe_checksum(self):
        """DataFrame checksum changes when data changes"""
        df1 = pd.DataFrame([
            {'player': 'Mahomes', 'team': 'KC', 'cap': 58.0},
            {'player': 'Allen', 'team': 'BUF', 'cap': 65.0},
        ])
        
        df2 = pd.DataFrame([
            {'player': 'Mahomes', 'team': 'KC', 'cap': 58.0},
            {'player': 'Allen', 'team': 'BUF', 'cap': 65.0},
        ])
        
        df3 = pd.DataFrame([
            {'player': 'Mahomes', 'team': 'KC', 'cap': 60.0},  # Changed
            {'player': 'Allen', 'team': 'BUF', 'cap': 65.0},
        ])
        
        checksum1 = ChecksumGenerator.generate_dataframe_checksum(df1)
        checksum2 = ChecksumGenerator.generate_dataframe_checksum(df2)
        checksum3 = ChecksumGenerator.generate_dataframe_checksum(df3)
        
        assert checksum1 == checksum2, "Same data should have same checksum"
        assert checksum1 != checksum3, "Different data should have different checksum"


class TestDeduplicationEngine:
    """Test duplicate detection and removal"""
    
    def test_detect_duplicates_with_exact_duplicates(self):
        """Detects exact duplicate records"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024, 'cap_hit': 58.0},
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024, 'cap_hit': 58.0},  # Duplicate
            {'player_name': 'Josh Allen', 'team': 'BUF', 'year': 2024, 'cap_hit': 65.0},
        ])
        
        dedup = DeduplicationEngine(['player_name', 'team', 'year'])
        clean, duplicates = dedup.detect_duplicates(df)
        
        assert len(clean) == 2, "Should have 2 unique records"
        assert len(duplicates) == 1, "Should have 1 duplicate"
        assert clean.iloc[0]['player_name'] == 'Patrick Mahomes'
        assert duplicates.iloc[0]['player_name'] == 'Patrick Mahomes'
    
    def test_detect_duplicates_case_insensitive(self):
        """Detects duplicates with case differences"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024},
            {'player_name': 'patrick mahomes', 'team': 'kc', 'year': 2024},  # Duplicate (different case)
        ])
        
        dedup = DeduplicationEngine(['player_name', 'team', 'year'])
        clean, duplicates = dedup.detect_duplicates(df)
        
        assert len(clean) == 1, "Should have 1 unique record (case-insensitive)"
        assert len(duplicates) == 1, "Should have 1 duplicate"
    
    def test_detect_duplicates_no_duplicates(self):
        """Returns empty duplicates DataFrame when no duplicates"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024},
            {'player_name': 'Josh Allen', 'team': 'BUF', 'year': 2024},
            {'player_name': 'Jalen Hurts', 'team': 'PHI', 'year': 2024},
        ])
        
        dedup = DeduplicationEngine(['player_name', 'team', 'year'])
        clean, duplicates = dedup.detect_duplicates(df)
        
        assert len(clean) == 3
        assert len(duplicates) == 0
    
    def test_mark_duplicates(self):
        """Marks duplicates instead of removing them"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024},
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024},  # Duplicate
            {'player_name': 'Josh Allen', 'team': 'BUF', 'year': 2024},
        ])
        
        dedup = DeduplicationEngine(['player_name', 'team', 'year'])
        marked = dedup.mark_duplicates(df)
        
        assert 'is_duplicate' in marked.columns
        assert marked['is_duplicate'].sum() == 1, "Should mark 1 duplicate"
        assert marked.loc[0, 'is_duplicate'] == False
        assert marked.loc[1, 'is_duplicate'] == True


class TestValidationGate:
    """Test validation gates"""
    
    def test_validate_required_columns_pass(self):
        """Validation passes when all columns present"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', 'Allen'],
            'team': ['KC', 'BUF'],
            'year': [2024, 2024],
        })
        
        result = ValidationGate.validate_required_columns(df, ['player_name', 'team', 'year'])
        
        assert result.status == ValidationStatus.PASS
        assert 'All 3 required columns present' in result.message
    
    def test_validate_required_columns_fail(self):
        """Validation fails when columns missing"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', 'Allen'],
            'team': ['KC', 'BUF'],
        })
        
        result = ValidationGate.validate_required_columns(df, ['player_name', 'team', 'year', 'cap_hit'])
        
        assert result.status == ValidationStatus.FAIL
        assert 'Missing required columns' in result.message
        assert 'year' in result.details['missing_columns']
    
    def test_validate_no_nulls_pass(self):
        """Validation passes when no nulls in critical columns"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', 'Allen'],
            'team': ['KC', 'BUF'],
        })
        
        result = ValidationGate.validate_no_nulls(df, ['player_name', 'team'])
        
        assert result.status == ValidationStatus.PASS
    
    def test_validate_no_nulls_fail(self):
        """Validation fails when nulls in critical columns"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', None],
            'team': ['KC', 'BUF'],
        })
        
        result = ValidationGate.validate_no_nulls(df, ['player_name', 'team'])
        
        assert result.status == ValidationStatus.FAIL
        assert 'player_name' in result.details['null_columns']
    
    def test_validate_uniqueness_pass(self):
        """Validation passes when records are unique"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', 'Allen', 'Hurts'],
            'team': ['KC', 'BUF', 'PHI'],
            'year': [2024, 2024, 2024],
        })
        
        result = ValidationGate.validate_uniqueness(df, ['player_name', 'team', 'year'])
        
        assert result.status == ValidationStatus.PASS
    
    def test_validate_uniqueness_fail(self):
        """Validation fails when duplicates exist"""
        df = pd.DataFrame({
            'player_name': ['Mahomes', 'Mahomes', 'Allen'],
            'team': ['KC', 'KC', 'BUF'],
            'year': [2024, 2024, 2024],
        })
        
        result = ValidationGate.validate_uniqueness(df, ['player_name', 'team', 'year'])
        
        assert result.status == ValidationStatus.FAIL
        assert result.details['duplicate_count'] == 2


class TestIdempotentProcessor:
    """Test full idempotent processing pipeline"""
    
    def test_process_successful(self):
        """Full pipeline processes data successfully"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024, 'cap_hit': 58.0},
            {'player_name': 'Josh Allen', 'team': 'BUF', 'year': 2024, 'cap_hit': 65.0},
        ])
        
        processor = IdempotentProcessor('test', ['player_name', 'team', 'year'])
        result_df, state = processor.process(
            df,
            required_cols=['player_name', 'team', 'year', 'cap_hit'],
            critical_null_cols=['player_name', 'team'],
            unique_on=['player_name', 'team', 'year']
        )
        
        assert state.status == ProcessingStatus.COMPLETED
        assert state.row_count == 2
        assert state.duplicate_count == 0
        assert len(result_df) == 2
    
    def test_process_removes_duplicates(self):
        """Processor removes duplicates"""
        df = pd.DataFrame([
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024, 'cap_hit': 58.0},
            {'player_name': 'Patrick Mahomes', 'team': 'KC', 'year': 2024, 'cap_hit': 58.0},  # Duplicate
            {'player_name': 'Josh Allen', 'team': 'BUF', 'year': 2024, 'cap_hit': 65.0},
        ])
        
        processor = IdempotentProcessor('test', ['player_name', 'team', 'year'])
        result_df, state = processor.process(
            df,
            required_cols=['player_name', 'team', 'year', 'cap_hit'],
            unique_on=['player_name', 'team', 'year']
        )
        
        assert state.status == ProcessingStatus.COMPLETED
        assert state.duplicate_count == 1
        assert len(result_df) == 2
    
    def test_process_fails_on_missing_columns(self):
        """Processor fails when required columns missing"""
        df = pd.DataFrame([
            {'player_name': 'Mahomes', 'team': 'KC'},
        ])
        
        processor = IdempotentProcessor('test', ['player_name', 'team', 'year'])
        result_df, state = processor.process(
            df,
            required_cols=['player_name', 'team', 'year', 'cap_hit']
        )
        
        assert state.status == ProcessingStatus.FAILED
        assert 'Missing required columns' in state.error_message
    
    def test_process_fails_on_duplicates_after_dedup(self):
        """Processor fails if duplicates remain after dedup"""
        df = pd.DataFrame([
            {'player_name': 'Mahomes', 'team': 'KC', 'year': 2024},
            {'player_name': 'mahomes', 'team': 'kc', 'year': 2024},  # Case variation - should dedup
            {'player_name': 'Allen', 'team': 'BUF', 'year': 2024},
        ])
        
        processor = IdempotentProcessor('test', ['player_name', 'team', 'year'])
        result_df, state = processor.process(
            df,
            required_cols=['player_name', 'team', 'year'],
            unique_on=['player_name', 'team', 'year']
        )
        
        # After dedup, should be 2 unique records
        assert state.duplicate_count == 1
        assert len(result_df) == 2
        assert state.status == ProcessingStatus.COMPLETED


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
