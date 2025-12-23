#!/usr/bin/env python3
"""
Data Load Validation

Validates a data load before promotion to production marts.
Runs configurable validation rules and returns pass/fail with details.

Usage:
    python validate_load.py --load-id abc123
    python validate_load.py --file data/licenses/texas/2025-12-23/dentist.csv --source-type tx_license
    python validate_load.py --file data.csv --source-type tx_license --previous data_prev.csv
"""

import argparse
import json
import hashlib
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Callable
import pandas as pd


@dataclass
class ValidationResult:
    """Result of a single validation rule."""
    rule_name: str
    passed: bool
    severity: str  # 'error' or 'warning'
    message: str
    details: Optional[Dict] = None


@dataclass
class LoadValidationResult:
    """Overall validation result for a load."""
    load_id: str
    source_type: str
    source_file: str
    validated_at: str
    passed: bool
    row_count: int
    row_count_previous: Optional[int]
    row_count_delta_pct: Optional[float]
    errors: List[ValidationResult] = field(default_factory=list)
    warnings: List[ValidationResult] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            **asdict(self),
            'errors': [asdict(e) for e in self.errors],
            'warnings': [asdict(w) for w in self.warnings],
        }


# =============================================================================
# VALIDATION RULES
# =============================================================================

class ValidationRules:
    """Collection of validation rules."""

    @staticmethod
    def row_count_min(df: pd.DataFrame, min_rows: int) -> ValidationResult:
        """Check minimum row count."""
        count = len(df)
        passed = count >= min_rows
        return ValidationResult(
            rule_name='row_count_min',
            passed=passed,
            severity='error',
            message=f"Row count {count} {'meets' if passed else 'below'} minimum {min_rows}",
            details={'row_count': count, 'min_required': min_rows}
        )

    @staticmethod
    def row_count_delta(df: pd.DataFrame, prev_df: pd.DataFrame, max_delta_pct: float) -> ValidationResult:
        """Check row count hasn't changed too dramatically."""
        if prev_df is None or len(prev_df) == 0:
            return ValidationResult(
                rule_name='row_count_delta',
                passed=True,
                severity='warning',
                message="No previous data to compare - skipping delta check",
                details={'skipped': True}
            )

        current = len(df)
        previous = len(prev_df)
        delta_pct = abs(current - previous) / previous if previous > 0 else 0
        passed = delta_pct <= max_delta_pct

        return ValidationResult(
            rule_name='row_count_delta',
            passed=passed,
            severity='error',
            message=f"Row count delta {delta_pct:.1%} {'within' if passed else 'exceeds'} {max_delta_pct:.0%} threshold",
            details={
                'current_count': current,
                'previous_count': previous,
                'delta_pct': round(delta_pct, 4),
                'max_delta_pct': max_delta_pct
            }
        )

    @staticmethod
    def field_populated(df: pd.DataFrame, field: str, min_pct: float) -> ValidationResult:
        """Check that a field is populated above threshold."""
        if field not in df.columns:
            return ValidationResult(
                rule_name=f'field_populated_{field}',
                passed=False,
                severity='error',
                message=f"Field '{field}' not found in data",
                details={'field': field, 'available_columns': list(df.columns)[:20]}
            )

        populated_pct = df[field].notna().mean()
        # Also check for empty strings
        if df[field].dtype == 'object':
            populated_pct = (df[field].notna() & (df[field].str.strip() != '')).mean()

        passed = populated_pct >= min_pct

        return ValidationResult(
            rule_name=f'field_populated_{field}',
            passed=passed,
            severity='error',
            message=f"Field '{field}' {populated_pct:.1%} populated {'meets' if passed else 'below'} {min_pct:.0%} threshold",
            details={
                'field': field,
                'populated_pct': round(populated_pct, 4),
                'min_required': min_pct
            }
        )

    @staticmethod
    def field_format(df: pd.DataFrame, field: str, pattern: str, min_pct: float) -> ValidationResult:
        """Check that a field matches a regex pattern."""
        if field not in df.columns:
            return ValidationResult(
                rule_name=f'field_format_{field}',
                passed=False,
                severity='error',
                message=f"Field '{field}' not found in data",
                details={'field': field}
            )

        # Only check non-null values
        non_null = df[field].dropna().astype(str)
        if len(non_null) == 0:
            return ValidationResult(
                rule_name=f'field_format_{field}',
                passed=False,
                severity='error',
                message=f"Field '{field}' has no non-null values to validate",
                details={'field': field}
            )

        matches = non_null.str.match(pattern)
        match_pct = matches.mean()
        passed = match_pct >= min_pct

        # Get sample of non-matching values
        non_matching = non_null[~matches].head(5).tolist()

        return ValidationResult(
            rule_name=f'field_format_{field}',
            passed=passed,
            severity='error',
            message=f"Field '{field}' {match_pct:.1%} match pattern {'meets' if passed else 'below'} {min_pct:.0%}",
            details={
                'field': field,
                'pattern': pattern,
                'match_pct': round(match_pct, 4),
                'min_required': min_pct,
                'sample_non_matching': non_matching
            }
        )

    @staticmethod
    def no_duplicates(df: pd.DataFrame, key_fields: List[str]) -> ValidationResult:
        """Check for duplicate records on key fields."""
        missing = [f for f in key_fields if f not in df.columns]
        if missing:
            return ValidationResult(
                rule_name='no_duplicates',
                passed=False,
                severity='error',
                message=f"Key fields not found: {missing}",
                details={'missing_fields': missing}
            )

        duplicates = df[df.duplicated(subset=key_fields, keep=False)]
        dup_count = len(duplicates)
        passed = dup_count == 0

        return ValidationResult(
            rule_name='no_duplicates',
            passed=passed,
            severity='error',
            message=f"{'No duplicates found' if passed else f'{dup_count} duplicate records on {key_fields}'}",
            details={
                'key_fields': key_fields,
                'duplicate_count': dup_count,
                'sample_duplicates': duplicates[key_fields].head(5).to_dict('records') if dup_count > 0 else []
            }
        )

    @staticmethod
    def date_range(df: pd.DataFrame, field: str, min_date: Optional[str], max_date: Optional[str]) -> ValidationResult:
        """Check dates are within expected range."""
        if field not in df.columns:
            return ValidationResult(
                rule_name=f'date_range_{field}',
                passed=False,
                severity='error',
                message=f"Field '{field}' not found",
                details={'field': field}
            )

        # Parse dates
        dates = pd.to_datetime(df[field], errors='coerce')
        valid_dates = dates.dropna()

        if len(valid_dates) == 0:
            return ValidationResult(
                rule_name=f'date_range_{field}',
                passed=True,
                severity='warning',
                message=f"No valid dates in '{field}' to validate",
                details={'field': field}
            )

        issues = []

        if min_date:
            min_dt = pd.to_datetime(min_date)
            too_old = (valid_dates < min_dt).sum()
            if too_old > 0:
                issues.append(f"{too_old} dates before {min_date}")

        if max_date:
            if max_date == 'CURRENT_DATE()':
                max_dt = pd.Timestamp.now()
            else:
                max_dt = pd.to_datetime(max_date)
            too_new = (valid_dates > max_dt).sum()
            if too_new > 0:
                issues.append(f"{too_new} dates after {max_date}")

        passed = len(issues) == 0

        return ValidationResult(
            rule_name=f'date_range_{field}',
            passed=passed,
            severity='error',
            message=f"Date range check: {'; '.join(issues) if issues else 'all dates in range'}",
            details={
                'field': field,
                'min_date': str(valid_dates.min()),
                'max_date': str(valid_dates.max()),
                'issues': issues
            }
        )

    @staticmethod
    def value_distribution(df: pd.DataFrame, field: str, value: any, min_pct: float) -> ValidationResult:
        """Check that a specific value appears at least min_pct of the time."""
        if field not in df.columns:
            return ValidationResult(
                rule_name=f'value_distribution_{field}',
                passed=False,
                severity='error',
                message=f"Field '{field}' not found",
                details={'field': field}
            )

        value_pct = (df[field] == value).mean()
        passed = value_pct >= min_pct

        return ValidationResult(
            rule_name=f'value_distribution_{field}_{value}',
            passed=passed,
            severity='error',
            message=f"Value '{value}' in '{field}' at {value_pct:.1%} {'meets' if passed else 'below'} {min_pct:.0%}",
            details={
                'field': field,
                'target_value': value,
                'actual_pct': round(value_pct, 4),
                'min_required': min_pct,
                'value_counts': df[field].value_counts().head(10).to_dict()
            }
        )


# =============================================================================
# SOURCE TYPE CONFIGURATIONS
# =============================================================================

SOURCE_CONFIGS = {
    'tx_license': {
        'key_fields': ['LIC_ID'],  # LIC_ID is unique; LIC_NBR can be reused for cancelled licenses
        'license_number_field': 'LIC_NBR',
        'name_fields': ['LAST_NME', 'FIRST_NME'],
        'date_fields': {
            'LIC_ORIG_DTE': {'format': '%m/%d/%Y'},
            'LIC_EXPR_DTE': {'format': '%m/%d/%Y'},
        },
        'status_field': 'LIC_STA_CDE',
        'active_statuses': [20, 46, 70],
        'rules': [
            ('row_count_min', {'min_rows': 1000}),
            ('row_count_delta', {'max_delta_pct': 0.20}),
            ('field_populated', {'field': 'LIC_NBR', 'min_pct': 0.99}),
            ('field_populated', {'field': 'LAST_NME', 'min_pct': 0.99}),
            ('field_populated', {'field': 'FIRST_NME', 'min_pct': 0.98}),
            ('field_populated', {'field': 'CITY', 'min_pct': 0.90}),
            ('no_duplicates', {'key_fields': ['LIC_ID']}),
            ('date_range', {'field': 'LIC_ORIG_DTE', 'min_date': '1900-01-01', 'max_date': 'CURRENT_DATE()'}),
            ('value_distribution', {'field': 'LIC_STA_CDE', 'value': 20, 'min_pct': 0.50}),
        ],
    },
    'wa_license': {
        'key_fields': ['credential_number'],
        'rules': [
            ('row_count_min', {'min_rows': 1000}),
            ('row_count_delta', {'max_delta_pct': 0.20}),
            ('field_populated', {'field': 'credential_number', 'min_pct': 0.99}),
            ('field_populated', {'field': 'last_name', 'min_pct': 0.99}),
            ('no_duplicates', {'key_fields': ['credential_number']}),
        ],
    },
    'co_license': {
        'key_fields': ['license_number'],
        'rules': [
            ('row_count_min', {'min_rows': 500}),
            ('row_count_delta', {'max_delta_pct': 0.20}),
            ('field_populated', {'field': 'license_number', 'min_pct': 0.99}),
            ('field_populated', {'field': 'last_name', 'min_pct': 0.99}),
            ('no_duplicates', {'key_fields': ['license_number']}),
        ],
    },
    'npi': {
        'key_fields': ['NPI'],
        'rules': [
            ('row_count_min', {'min_rows': 100000}),
            ('row_count_delta', {'max_delta_pct': 0.10}),
            ('field_populated', {'field': 'NPI', 'min_pct': 0.9999}),
            ('field_format', {'field': 'NPI', 'pattern': r'^\d{10}$', 'min_pct': 0.99}),
            ('no_duplicates', {'key_fields': ['NPI']}),
        ],
    },
}


def generate_load_id(file_path: str, timestamp: datetime) -> str:
    """Generate deterministic load ID from file path and timestamp."""
    data = f"{file_path}-{timestamp.isoformat()}"
    return hashlib.md5(data.encode()).hexdigest()[:12]


def validate_load(
    df: pd.DataFrame,
    source_type: str,
    prev_df: Optional[pd.DataFrame] = None,
    load_id: Optional[str] = None,
    source_file: Optional[str] = None,
) -> LoadValidationResult:
    """
    Validate a data load against configured rules.

    Args:
        df: Current data to validate
        source_type: Type of source (e.g., 'tx_license')
        prev_df: Previous load for comparison (optional)
        load_id: Load identifier (generated if not provided)
        source_file: Source file path

    Returns:
        LoadValidationResult with pass/fail and details
    """
    if source_type not in SOURCE_CONFIGS:
        raise ValueError(f"Unknown source type: {source_type}. Available: {list(SOURCE_CONFIGS.keys())}")

    config = SOURCE_CONFIGS[source_type]
    rules = config.get('rules', [])

    # Generate load ID if not provided
    if not load_id:
        load_id = generate_load_id(source_file or 'unknown', datetime.now())

    # Calculate row count delta
    row_count = len(df)
    row_count_prev = len(prev_df) if prev_df is not None else None
    row_count_delta = None
    if row_count_prev and row_count_prev > 0:
        row_count_delta = (row_count - row_count_prev) / row_count_prev

    # Run validation rules
    errors = []
    warnings = []

    for rule_name, rule_params in rules:
        rule_fn = getattr(ValidationRules, rule_name, None)
        if not rule_fn:
            warnings.append(ValidationResult(
                rule_name=rule_name,
                passed=False,
                severity='warning',
                message=f"Unknown rule: {rule_name}"
            ))
            continue

        try:
            # Add prev_df for delta rules
            if rule_name == 'row_count_delta':
                result = rule_fn(df, prev_df, **rule_params)
            else:
                result = rule_fn(df, **rule_params)

            if not result.passed:
                if result.severity == 'error':
                    errors.append(result)
                else:
                    warnings.append(result)
            elif result.severity == 'warning':
                warnings.append(result)

        except Exception as e:
            errors.append(ValidationResult(
                rule_name=rule_name,
                passed=False,
                severity='error',
                message=f"Rule execution failed: {str(e)}"
            ))

    # Overall pass/fail (errors fail, warnings don't)
    passed = len(errors) == 0

    return LoadValidationResult(
        load_id=load_id,
        source_type=source_type,
        source_file=source_file or 'unknown',
        validated_at=datetime.now().isoformat(),
        passed=passed,
        row_count=row_count,
        row_count_previous=row_count_prev,
        row_count_delta_pct=round(row_count_delta, 4) if row_count_delta else None,
        errors=errors,
        warnings=warnings,
    )


def main():
    parser = argparse.ArgumentParser(description="Validate a data load")
    parser.add_argument('--file', required=True, help="Path to CSV file to validate")
    parser.add_argument('--source-type', required=True, choices=list(SOURCE_CONFIGS.keys()),
                        help="Type of data source")
    parser.add_argument('--previous', help="Path to previous CSV file for comparison")
    parser.add_argument('--load-id', help="Load ID (generated if not provided)")
    parser.add_argument('--output', help="Output JSON file for results")
    parser.add_argument('--quiet', action='store_true', help="Only output errors")

    args = parser.parse_args()

    # Load data
    print(f"Loading {args.file}...")
    df = pd.read_csv(args.file, low_memory=False)

    prev_df = None
    if args.previous:
        print(f"Loading previous: {args.previous}...")
        prev_df = pd.read_csv(args.previous, low_memory=False)

    # Validate
    print(f"Validating as {args.source_type}...")
    result = validate_load(
        df=df,
        source_type=args.source_type,
        prev_df=prev_df,
        load_id=args.load_id,
        source_file=args.file,
    )

    # Output
    if not args.quiet:
        print()
        print("=" * 60)
        print(f"Validation Results: {'PASSED' if result.passed else 'FAILED'}")
        print("=" * 60)
        print(f"Load ID: {result.load_id}")
        print(f"Source: {result.source_type}")
        print(f"File: {result.source_file}")
        print(f"Row count: {result.row_count:,}")
        if result.row_count_previous:
            print(f"Previous: {result.row_count_previous:,} ({result.row_count_delta_pct:+.1%})")
        print()

        if result.errors:
            print("ERRORS:")
            for err in result.errors:
                print(f"  [FAIL] {err.rule_name}: {err.message}")
            print()

        if result.warnings:
            print("WARNINGS:")
            for warn in result.warnings:
                print(f"  [WARN] {warn.rule_name}: {warn.message}")
            print()

    # Save output
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        print(f"Results saved to: {args.output}")

    # Exit code
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
