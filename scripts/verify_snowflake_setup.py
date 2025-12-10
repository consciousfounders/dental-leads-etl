#!/usr/bin/env python3
"""
Verify Snowflake Setup for Dental Leads ETL

Checks:
1. Stage exists (npi_stage)
2. GCS storage integration
3. Stage contents
4. NPI data loaded
5. Table schema
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_client import SnowflakeClient


def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def run_query(client: SnowflakeClient, description: str, sql: str, show_results: bool = True):
    """Run a query and display results"""
    print(f"📋 {description}")
    print(f"   SQL: {sql[:80]}{'...' if len(sql) > 80 else ''}")
    print()
    
    try:
        results = client.execute(sql)
        
        if results and show_results:
            for row in results[:10]:  # Limit to 10 rows for display
                print(f"   {row}")
            if len(results) > 10:
                print(f"   ... and {len(results) - 10} more rows")
        elif results:
            print(f"   ✅ Query returned {len(results)} rows")
        else:
            print("   ✅ Query executed (no results returned)")
        
        return results
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def main():
    print_section("SNOWFLAKE SETUP VERIFICATION")
    print("Connecting to Snowflake...")
    
    try:
        with SnowflakeClient() as client:
            print("✅ Connected successfully!\n")
            
            # 1. Check current context
            print_section("1. CURRENT CONTEXT")
            run_query(client, "Current database/schema/warehouse", 
                     "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
            
            # 2. Check if stage exists
            print_section("2. STAGE CHECK")
            run_query(client, "Looking for npi_stage", 
                     "SHOW STAGES LIKE 'npi_stage' IN SCHEMA RAW")
            
            # 3. Check storage integrations
            print_section("3. STORAGE INTEGRATIONS")
            run_query(client, "Available storage integrations", 
                     "SHOW STORAGE INTEGRATIONS")
            
            # 4. Try to list stage contents (if stage exists)
            print_section("4. STAGE CONTENTS")
            try:
                run_query(client, "Files in @npi_stage", 
                         "LIST @RAW.npi_stage")
            except Exception as e:
                print(f"   ⚠️  Could not list stage: {e}")
            
            # 5. Check available schemas
            print_section("5. SCHEMAS IN DATABASE")
            run_query(client, "Available schemas", 
                     "SHOW SCHEMAS IN DATABASE DENTAL_LEADS")
            
            # 6. Check tables in RAW schema
            print_section("6. TABLES IN RAW SCHEMA")
            run_query(client, "Tables in RAW schema", 
                     "SHOW TABLES IN SCHEMA RAW")
            
            # 7. Check if NPI data exists
            print_section("7. NPI DATA CHECK")
            
            # First check what tables exist that might have NPI data
            tables_result = client.execute("SHOW TABLES IN SCHEMA RAW")
            npi_tables = [t for t in (tables_result or []) if 'NPI' in str(t).upper()]
            
            if npi_tables:
                print(f"   Found NPI-related tables: {[t[1] for t in npi_tables]}")
                
                for table in npi_tables:
                    table_name = table[1]
                    print(f"\n   📊 Table: {table_name}")
                    
                    # Get row count
                    count_result = run_query(client, f"Row count for {table_name}", 
                                            f"SELECT COUNT(*) FROM RAW.{table_name}", 
                                            show_results=True)
                    
                    # Get schema
                    print(f"\n   📋 Schema for {table_name}:")
                    schema_result = client.execute(f"DESCRIBE TABLE RAW.{table_name}")
                    if schema_result:
                        for col in schema_result[:20]:
                            print(f"      - {col[0]}: {col[1]}")
                        if len(schema_result) > 20:
                            print(f"      ... and {len(schema_result) - 20} more columns")
                    
                    # Sample data
                    print(f"\n   📋 Sample data from {table_name}:")
                    run_query(client, "First 3 rows", 
                             f"SELECT * FROM RAW.{table_name} LIMIT 3")
            else:
                print("   ⚠️  No NPI tables found in RAW schema")
                print("   You may need to run the NPI ingestion pipeline first.")
            
            # 8. Check CLEAN schema
            print_section("8. CLEAN SCHEMA CHECK")
            try:
                run_query(client, "Tables in CLEAN schema", 
                         "SHOW TABLES IN SCHEMA CLEAN")
            except Exception as e:
                print(f"   ⚠️  CLEAN schema may not exist: {e}")
                print("   We'll create it when building RAW → CLEAN transformation")
            
            print_section("VERIFICATION COMPLETE")
            print("Review the output above to confirm your setup.")
            print("\nNext steps:")
            print("  1. If stage/integration missing → Create them in Snowflake")
            print("  2. If NPI data missing → Run NPI ingestion pipeline")
            print("  3. If all good → Proceed to RAW → CLEAN transformation")
            
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check your secrets are configured (snowflake-account, snowflake-user, etc.)")
        print("  2. Verify your private key is valid")
        print("  3. Check network connectivity to Snowflake")
        sys.exit(1)


if __name__ == "__main__":
    main()

