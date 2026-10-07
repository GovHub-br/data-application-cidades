#!/usr/bin/env python3
"""
Quality analysis of treated tables in PostgreSQL (dados_historicos_formatados schema).

Usage:
    uv run python scripts/quality_analysis_db.py
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import urllib.parse
from collections import defaultdict

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ── Configuration ──────────────────────────────────────────────────────────

DB_HOST = "10.0.0.50"
DB_PORT = "5432"
DB_NAME = "cidades"
DB_USER = "cidades"
DB_PASSWORD = "hRWdIE)rfKZPDLUs9Zsd57+qP"
DB_SCHEMA_SOURCE = "dados_historicos"
DB_SCHEMA_TARGET = "dados_historicos_formatados"

# ── Connect ─────────────────────────────────────────────────────────────────

password_encoded = urllib.parse.quote_plus(DB_PASSWORD)
connection_string = (
    f"postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(
    connection_string,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 30},
)
print(f"Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")

# ── 1. List ALL tables ───────────────────────────────────────────────────────

METADATA_EXCLUDE = {"_classificacao", "_dedup_map", "_quality_report"}

with engine.connect() as conn:
    result = conn.execute(
        text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        ORDER BY table_name
    """),
        {"schema": DB_SCHEMA_TARGET},
    )

    all_tables = [row[0] for row in result.fetchall()]

print(f"\n{'=' * 80}")
print(f"1. ALL TABLES IN {DB_SCHEMA_TARGET}")
print(f"{'=' * 80}")
print(f"Total tables: {len(all_tables)}\n")

# Separate metadata vs data tables
data_tables = [t for t in all_tables if t not in METADATA_EXCLUDE]
metadata_tables = [t for t in all_tables if t in METADATA_EXCLUDE]

print(f"Data tables: {len(data_tables)}")
print(f"Metadata tables: {metadata_tables}")
print("\nFirst 30 data tables:")
for i, t in enumerate(data_tables[:30], 1):
    print(f"  {i:3d}. {t}")

# ── 2. Get row counts for all tables ───────────────────────────────────────

print(f"\n{'=' * 80}")
print("2. TABLE SIZE ANALYSIS")
print(f"{'=' * 80}")


def get_table_info(schema: str, table_name: str) -> dict:
    """Get row count and size info for a table using a fresh connection."""
    result = {"table_name": table_name, "row_count": None, "error": None}
    try:
        # Use a new connection each time to avoid transaction issues
        with engine.connect() as conn:
            # Row count
            row_res = conn.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            )
            result["row_count"] = row_res.scalar()

            # Column info
            col_res = conn.execute(
                text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """),
                {"schema": schema, "table": table_name},
            )
            result["columns"] = [
                {"name": r[0], "type": r[1]} for r in col_res.fetchall()
            ]
            result["column_count"] = len(result["columns"])

    except Exception as e:
        result["error"] = str(e)

    return result


# Get info for all tables (this may take a while)
print("\nFetching row counts for all tables...")
all_table_info = {}
for i, table in enumerate(data_tables, 1):
    if i % 50 == 0:
        print(f"  Processed {i}/{len(data_tables)}...")
    info = get_table_info(DB_SCHEMA_TARGET, table)
    all_table_info[table] = info

# Print summary
table_sizes = []
for t, info in all_table_info.items():
    if info.get("row_count") is not None:
        table_sizes.append((t, info["row_count"]))

table_sizes.sort(key=lambda x: x[1], reverse=True)

print(f"\nTop 20 tables by row count:")
print(f"{'Table Name':<50} {'Rows':>12}")
print("-" * 64)
for t, rows in table_sizes[:20]:
    print(f"{t:<50} {rows:>12,}")

# Categorize
small = [(t, r) for t, r in table_sizes if r < 1000]
medium = [(t, r) for t, r in table_sizes if 1000 <= r < 50000]
large = [(t, r) for t, r in table_sizes if r >= 50000]

print(f"\n\nTable size distribution:")
print(f"  Small (<1,000 rows):    {len(small):>5} tables")
print(f"  Medium (1K-50K rows):   {len(medium):>5} tables")
print(f"  Large (>=50K rows):     {len(large):>5} tables")

# ── 3. Select SAMPLE tables ──────────────────────────────────────────────────

print(f"\n{'=' * 80}")
print("3. SAMPLE SELECTION (15 tables: mix of sizes and institutions)")
print(f"{'=' * 80}")

bb_tables = [t for t, _ in table_sizes if t.startswith("bb_")]
caixa_tables = [t for t, _ in table_sizes if t.startswith("caixa_")]

print(f"\nBB tables with known counts: {len(bb_tables)}")
print(f"Caixa tables with known counts: {len(caixa_tables)}")

sample_tables = []

# Small: 2 bb_, 3 caixa_
small_bb = [t for t, _ in small if t.startswith("bb_")][:2]
small_caixa = [t for t, _ in small if t.startswith("caixa_")][:3]
sample_tables.extend(small_bb)
sample_tables.extend(small_caixa)

# Medium: 3 bb_, 3 caixa_
medium_bb = [t for t, _ in medium if t.startswith("bb_")][:3]
medium_caixa = [t for t, _ in medium if t.startswith("caixa_")][:3]
sample_tables.extend(medium_bb)
sample_tables.extend(medium_caixa)

# Large: 2 bb_, 2 caixa_
large_bb = [t for t, _ in large if t.startswith("bb_")][:2]
large_caixa = [t for t, _ in large if t.startswith("caixa_")][:2]
sample_tables.extend(large_bb)
sample_tables.extend(large_caixa)

print(f"\nSelected {len(sample_tables)} sample tables:")
for i, t in enumerate(sample_tables, 1):
    rows = all_table_info[t].get("row_count", "N/A")
    inst = "BB" if t.startswith("bb_") else "Caixa"
    rows_str = f"{rows:,}" if isinstance(rows, int) else str(rows)
    print(f"  {i:2d}. [{inst}] {t:<50} ({rows_str} rows)")

# ── 4. Detailed analysis of each sampled table ──────────────────────────────

print(f"\n{'=' * 80}")
print("4. DETAILED QUALITY ANALYSIS PER TABLE")
print(f"{'=' * 80}")


def analyze_table_detailed(schema: str, table_name: str) -> dict:
    """Perform detailed quality analysis on a single table."""
    result = {
        "table_name": table_name,
        "row_count": 0,
        "column_count": 0,
        "columns": [],
        "null_heavy_cols": [],
        "metadata_issues": {},
        "type_issues": [],
        "all_null_rows": 0,
        "error": None,
    }

    try:
        # Use a fresh connection for each table
        with engine.connect() as conn:
            # Row count
            row_res = conn.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            )
            result["row_count"] = row_res.scalar()

            if result["row_count"] == 0:
                result["error"] = "Empty table"
                return result

            # Get column info
            col_res = conn.execute(
                text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """),
                {"schema": schema, "table": table_name},
            )

            columns = col_res.fetchall()
            result["column_count"] = len(columns)
            result["columns"] = [{"name": c[0], "type": c[1]} for c in columns]

            # Analyze each column
            for col in result["columns"]:
                col_name = col["name"]

                try:
                    # NULL count
                    null_res = conn.execute(
                        text(f'''
                        SELECT COUNT(*) FROM "{schema}"."{table_name}"
                        WHERE "{col_name}" IS NULL
                    ''')
                    )
                    null_count = null_res.scalar()

                    # Empty string count
                    empty_res = conn.execute(
                        text(f'''
                        SELECT COUNT(*) FROM "{schema}"."{table_name}"
                        WHERE "{col_name}" = ''
                    ''')
                    )
                    empty_count = empty_res.scalar()

                    # 'nan' string count (safe cast)
                    nan_res = conn.execute(
                        text(f'''
                        SELECT COUNT(*) FROM "{schema}"."{table_name}"
                        WHERE "{col_name}"::text = 'nan'
                    ''')
                    )
                    nan_count = nan_res.scalar()

                    col["null_count"] = null_count
                    col["empty_count"] = empty_count
                    col["nan_count"] = nan_count

                    total = result["row_count"]
                    bad_pct = (
                        (null_count + empty_count + nan_count) / total * 100
                        if total > 0
                        else 0
                    )
                    col["bad_pct"] = round(bad_pct, 2)

                    if bad_pct > 90:
                        result["null_heavy_cols"].append(
                            {"column": col_name, "bad_pct": round(bad_pct, 2)}
                        )

                except Exception as e:
                    col["analysis_error"] = str(e)

            # Check metadata columns
            metadata_cols = ["institution", "profile", "report_date", "source_table"]
            for meta_col in metadata_cols:
                if any(c["name"] == meta_col for c in result["columns"]):
                    try:
                        null_res = conn.execute(
                            text(f'''
                            SELECT COUNT(*) FROM "{schema}"."{table_name}"
                            WHERE "{meta_col}" IS NULL OR "{meta_col}" = ''
                        ''')
                        )
                        null_or_empty = null_res.scalar()
                        fill_rate = round(
                            (result["row_count"] - null_or_empty)
                            / result["row_count"]
                            * 100,
                            2,
                        )
                        result["metadata_issues"][meta_col] = {
                            "null_or_empty": null_or_empty,
                            "fill_rate": fill_rate,
                        }
                    except Exception as e:
                        result["metadata_issues"][f"{meta_col}_error"] = str(e)

            # Check for numeric type inconsistencies
            numeric_types = {
                "integer",
                "bigint",
                "smallint",
                "numeric",
                "real",
                "double precision",
            }
            for col in result["columns"]:
                if col["type"] in numeric_types:
                    try:
                        # Find values that are NOT numeric
                        bad_res = conn.execute(
                            text(f'''
                            SELECT COUNT(*) FROM "{schema}"."{table_name}"
                            WHERE "{col["name"]}" IS NOT NULL
                              AND "{col["name"]}" != ''
                              AND NOT (::text ~ '^[0-9.eE+-]+$')
                        ''')
                        )
                        bad_count = bad_res.scalar()
                        if bad_count > 0:
                            # Get sample bad values
                            samples_res = conn.execute(
                                text(f'''
                                SELECT DISTINCT LEFT("{col["name"]}"::text, 50)
                                FROM "{schema}"."{table_name}"
                                WHERE "{col["name"]}" IS NOT NULL
                                  AND "{col["name"]}" != ''
                                  AND NOT (::text ~ '^[0-9.eE+-]+$')
                                LIMIT 3
                            ''')
                            )
                            samples = [r[0] for r in samples_res.fetchall()]
                            result["type_issues"].append(
                                {
                                    "column": col["name"],
                                    "expected_type": col["type"],
                                    "bad_count": bad_count,
                                    "samples": samples,
                                }
                            )
                    except Exception as e:
                        result["type_issues"].append(
                            {"column": col["name"], "error": str(e)}
                        )

            # Check for all-null rows
            if result["row_count"] <= 50000:
                try:
                    conditions = " AND ".join(
                        [
                            f'("{c["name"]}" IS NULL OR "{c["name"]}" = \'\')'
                            for c in result["columns"]
                        ]
                    )
                    all_null_res = conn.execute(
                        text(f'''
                        SELECT COUNT(*) FROM "{schema}"."{table_name}"
                        WHERE {conditions}
                    ''')
                    )
                    result["all_null_rows"] = all_null_res.scalar()
                except Exception as e:
                    result["all_null_rows_error"] = str(e)
            else:
                result["all_null_rows"] = "Skipped (table too large)"

    except Exception as e:
        result["error"] = str(e)

    return result


# Run analysis on all sample tables
all_results = {}
for i, table in enumerate(sample_tables, 1):
    print(f"\nAnalyzing {i}/{len(sample_tables)}: {table}...")
    all_results[table] = analyze_table_detailed(DB_SCHEMA_TARGET, table)

# ── 5. Print detailed results ───────────────────────────────────────────────

for table in sample_tables:
    r = all_results[table]
    inst = "BB" if table.startswith("bb_") else "Caixa"
    print(f"\n{'─' * 80}")
    print(f"TABLE: [{inst}] {table}")
    print(f"{'─' * 80}")
    print(f"  Rows: {r.get('row_count', 'N/A'):,}")
    print(f"  Columns: {r.get('column_count', 'N/A')}")

    if "error" in r and r["error"]:
        print(f"  ERROR: {r['error']}")
        continue

    # Null-heavy columns
    if r.get("null_heavy_cols"):
        print(f"\n  ⚠️  NULL-HEAVY COLUMNS (>90% NULL/empty):")
        for c in r["null_heavy_cols"]:
            print(f"     - {c['column']}: {c['bad_pct']}% bad")

    # Metadata issues
    if r.get("metadata_issues"):
        print(f"\n  ⚠️  METADATA COLUMN STATUS:")
        for col, info in r["metadata_issues"].items():
            if isinstance(info, dict):
                filled = info["fill_rate"]
                nulls = info["null_or_empty"]
                print(f"     - {col}: {nulls} null/empty ({filled}% filled)")
            else:
                print(f"     - {col}: ERROR - {info}")

    # Type issues
    if r.get("type_issues"):
        print(f"\n  ⚠️  TYPE INCONSISTENCIES:")
        for issue in r["type_issues"]:
            if "error" in issue:
                print(f"     - {issue['column']}: ERROR - {issue['error']}")
            else:
                samples = issue.get("samples", [])
                print(f"     - {issue['column']} (expected {issue['expected_type']}):")
                print(
                    f"       {issue['bad_count']} non-numeric values, samples: {samples}"
                )

    # All-null rows
    all_null = r.get("all_null_rows", 0)
    if all_null == "Skipped (table too large)":
        pass  # Skip printing for large tables
    elif all_null and all_null > 0:
        print(f"\n  ⚠️  ROWS WITH ALL NULL/EMPTY: {all_null:,}")

    # Column details
    print(f"\n  COLUMN DETAILS:")
    print(
        f"  {'Column Name':<35} {'Type':<22} {'NULL':>8} {'Empty':>8} {'nan':>6} {'Bad%':>8}"
    )
    print(f"  {'-' * 89}")
    for c in r.get("columns", []):
        null = c.get("null_count", "-")
        empty = c.get("empty_count", "-")
        nan = c.get("nan_count", "-")
        bad = c.get("bad_pct", "-")
        null_str = f"{null:,}" if isinstance(null, int) else str(null)
        empty_str = f"{empty:,}" if isinstance(empty, int) else str(empty)
        nan_str = f"{nan:,}" if isinstance(nan, int) else str(nan)
        print(
            f"  {c['name']:<35} {c['type']:<22} {null_str:>8} {empty_str:>8} {nan_str:>6} {bad:>8}"
        )

# ── 6. Summary statistics ───────────────────────────────────────────────────

print(f"\n\n{'=' * 80}")
print("5. SUMMARY STATISTICS ACROSS SAMPLED TABLES")
print(f"{'=' * 80}")

total_rows = sum(
    r.get("row_count", 0)
    for r in all_results.values()
    if isinstance(r.get("row_count"), int)
)
total_cols = sum(r.get("column_count", 0) for r in all_results.values())

null_heavy_count = sum(len(r.get("null_heavy_cols", [])) for r in all_results.values())
tables_with_null_heavy = sum(
    1 for r in all_results.values() if r.get("null_heavy_cols")
)

metadata_issue_count = sum(
    len(r.get("metadata_issues", [])) for r in all_results.values()
)
tables_with_metadata_issues = sum(
    1 for r in all_results.values() if r.get("metadata_issues")
)

type_issue_count = sum(len(r.get("type_issues", [])) for r in all_results.values())
tables_with_type_issues = sum(1 for r in all_results.values() if r.get("type_issues"))

print(f"\nTables analyzed: {len(sample_tables)}")
print(f"Total rows (sampled): {total_rows:,}")
print(f"Total columns (sampled): {total_cols}")

print(f"\n── NULL-HEAVY COLUMNS (>90% NULL/empty) ──")
print(f"  Tables affected: {tables_with_null_heavy}/{len(sample_tables)}")
print(f"  Total null-heavy columns: {null_heavy_count}")

print(f"\n── METADATA COLUMN STATUS ──")
print(
    f"  Tables with metadata issues: {tables_with_metadata_issues}/{len(sample_tables)}"
)
for meta_col in ["institution", "profile", "report_date", "source_table"]:
    issues_in_col = sum(
        1
        for r in all_results.values()
        if r.get("metadata_issues", {}).get(meta_col, {}).get("null_or_empty", 0) > 0
    )
    print(f"    - {meta_col}: {issues_in_col} tables with nulls/empty")

print(f"\n── TYPE INCONSISTENCIES ──")
print(f"  Tables affected: {tables_with_type_issues}/{len(sample_tables)}")
print(f"  Total type issues: {type_issue_count}")

# ── 7. Specific problematic examples ─────────────────────────────────────────

print(f"\n{'=' * 80}")
print("6. SPECIFIC PROBLEMATIC EXAMPLES")
print(f"{'=' * 80}")

# Most null-heavy
worst_null = max(all_results.values(), key=lambda r: len(r.get("null_heavy_cols", [])))
if worst_null.get("null_heavy_cols"):
    print(f"\n📊 Table with MOST null-heavy columns:")
    print(
        f"   {worst_null['table_name']}: {len(worst_null['null_heavy_cols'])} columns >90% bad"
    )
    for c in worst_null["null_heavy_cols"][:5]:
        print(f"     - {c['column']}: {c['bad_pct']}%")

# Metadata issues - find most problematic
problematic_meta = []
for t, r in all_results.items():
    for col, info in r.get("metadata_issues", {}).items():
        if isinstance(info, dict) and info.get("null_or_empty", 0) > 0:
            problematic_meta.append((t, col, info["null_or_empty"], info["fill_rate"]))

if problematic_meta:
    print(f"\n📋 Tables with METADATA nulls/empty (top 10):")
    problematic_meta.sort(key=lambda x: x[2], reverse=True)
    for t, col, nulls, fill_rate in problematic_meta[:10]:
        print(f"   [{t}] {col}: {nulls} null/empty ({fill_rate}% filled)")

# Type issues
type_problems = []
for t, r in all_results.items():
    for issue in r.get("type_issues", []):
        if "error" not in issue:
            type_problems.append(
                (t, issue["column"], issue["expected_type"], issue["bad_count"])
            )

if type_problems:
    print(f"\n🔢 Tables with TYPE inconsistencies (top 10):")
    type_problems.sort(key=lambda x: x[3], reverse=True)
    for t, col, exp_type, bad_count in type_problems[:10]:
        print(f"   [{t}] {col} (expected {exp_type}): {bad_count} bad values")

# ── 8. Complete table list ──────────────────────────────────────────────────

print(f"\n{'=' * 80}")
print("7. COMPLETE TABLE LIST WITH ROW COUNTS")
print(f"{'=' * 80}")

for t, rows in table_sizes:
    inst = "BB" if t.startswith("bb_") else "Caixa"
    rows_str = f"{rows:,}" if isinstance(rows, int) else str(rows)
    print(f"  [{inst}] {t:<50} {rows_str:>12}")

print(f"\n{'=' * 80}")
print("ANALYSIS COMPLETE")
print(f"{'=' * 80}")
