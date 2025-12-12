# Architecture Assessment: Security & Swappability

## Executive Summary

**Architecture Flow:**
```
GCP VM (Orchestration) 
  → GCS Bucket (Data Lake)
    → Snowflake & BigQuery (Data Warehouses)
      → Streamlit / Looker / Hex (Visualization Tools)
```

**Overall Assessment:**
- ✅ **Warehouse Swappability**: **GOOD** - Well abstracted via `db_adapter.py`
- ⚠️ **Visualization Tool Swappability**: **PARTIAL** - Tools connect directly, not through adapter
- ⚠️ **Security Architecture**: **MIXED** - Good patterns but inconsistent implementation

---

## 1. Data Warehouse Swappability ✅

### Current Implementation

**Strengths:**
- ✅ **Unified Interface**: `utils/db_adapter.py` provides a single `get_db()` function
- ✅ **Environment-Driven**: Swaps via `WAREHOUSE_ENGINE` env var (`snowflake` | `bigquery`)
- ✅ **Consistent API**: Both clients implement same methods:
  - `execute(sql)` → Returns list of tuples
  - `fetch_df(sql)` → Returns pandas DataFrame
  - Context manager support (`with get_db() as db:`)
- ✅ **ETL Uses Adapter**: All ETL pipelines (`npi_ingestion.py`, `enrichment_pipeline.py`, `validation_pipeline.py`) use `get_db()`

**Code Pattern:**
```python
# etl/npi_ingestion.py
from utils.db_adapter import get_db

with get_db() as db:
    db.execute(sql)  # Works with Snowflake OR BigQuery
```

**SQL Compatibility:**
- ✅ Separate SQL files: `load_npi.snowflake.sql` and `load_npi.bigquery.sql`
- ✅ Handles dialect differences (e.g., `VARIANT` vs `JSON`, `STRING` vs `VARCHAR`)

### Assessment: **EXCELLENT** ✅

**To Swap Warehouses:**
1. Set `WAREHOUSE_ENGINE=bigquery` (or `snowflake`)
2. Ensure credentials configured in Secret Manager
3. Run ETL - no code changes needed

**Recommendation:** Keep this pattern. It's well-designed.

---

## 2. Visualization Tool Swappability ⚠️

### Current State

| Tool | Connection Method | Uses Adapter? | Swappable? |
|------|------------------|---------------|------------|
| **Streamlit** | Direct Snowflake connector | ❌ No | ❌ Hardcoded to Snowflake |
| **Looker** | Direct Snowflake connection | ❌ No | ❌ Hardcoded to Snowflake |
| **Hex** | Direct connections (BQ + SF) | ❌ No | ⚠️ Manual config per tool |

### Issues Identified

#### 1. Streamlit (`dashboards/client_dashboard.py`)
```python
# Line 178-240: Hardcoded Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(...)  # ❌ Direct Snowflake only
```

**Problems:**
- Hardcoded to Snowflake
- Doesn't use `db_adapter.py`
- Would require code changes to support BigQuery
- SQL queries are Snowflake-specific (e.g., `CLEAN.V_STATE_INSIGHTS`)

#### 2. Looker (`looker/dental_leads.model.lkml`)
```lkml
connection: "snowflake_dental_leads"  # ❌ Hardcoded connection name
```

**Problems:**
- LookML model references specific Snowflake connection
- Would need separate model for BigQuery
- Connection setup script (`setup_looker_connection.py`) is Snowflake-specific

#### 3. Hex
- ✅ **Good**: Supports both BigQuery and Snowflake natively
- ⚠️ **Issue**: Each connection configured manually in Hex UI
- ⚠️ **Issue**: No shared configuration with other tools

### Assessment: **NEEDS IMPROVEMENT** ⚠️

**Impact:**
- To switch warehouses, you'd need to:
  1. Update Streamlit code (hardcoded queries)
  2. Create new Looker connection + model
  3. Reconfigure Hex connections
  4. Test all three tools separately

**Recommendation:** Create visualization adapter layer (see below).

---

## 3. Security Architecture Assessment ⚠️

### Strengths ✅

#### 1. Secrets Management
- ✅ **GCP Secret Manager** integration (`utils/secrets_manager.py`)
- ✅ **Environment Variable Fallback** for local dev
- ✅ **SOC 2 Compliance**: All secret accesses logged
- ✅ **No Plaintext Secrets**: Secrets never in code

#### 2. Authentication Methods
- ✅ **RSA Key-Pair Auth** for Snowflake (production)
- ✅ **Service Account Keys** for BigQuery (JSON keys)
- ✅ **Password Auth** as fallback (local dev)

#### 3. Access Control
- ✅ **Least Privilege**: Read-only access for visualization tools
- ✅ **Dedicated Users**: Hex setup creates `HEX_USER` with `HEX_ROLE`
- ✅ **Role-Based Access**: Different roles per use case

### Weaknesses ⚠️

#### 1. Inconsistent Secret Storage

| Component | Secret Storage | Issue |
|-----------|---------------|-------|
| **ETL Pipelines** | GCP Secret Manager / Env Vars | ✅ Good |
| **Streamlit** | `.streamlit/secrets.toml` | ⚠️ Separate system |
| **Looker** | `config/looker_snowflake.json` + `config/looker_rsa_key.p8` | ⚠️ File-based |
| **Hex** | Hex UI (manual entry) | ⚠️ No automation |

**Problem:** Secrets scattered across multiple systems, harder to rotate/audit.

#### 2. Credential Rotation Risk

**Current State:**
- Snowflake RSA keys stored in:
  - GCP Secret Manager (for ETL)
  - `.streamlit/secrets.toml` (for Streamlit)
  - `config/looker_rsa_key.p8` (for Looker)
  - Hex UI (manual)

**Risk:** Rotating credentials requires updating 4+ places.

#### 3. No Centralized Audit Trail

**Current:**
- ETL: Logs via `secrets_manager.py` ✅
- Streamlit: No secret access logging ❌
- Looker: No secret access logging ❌
- Hex: Managed by Hex platform (unknown) ❓

**Gap:** Can't see unified audit trail of who accessed what.

#### 4. VM Security Posture

**GCP VM (Orchestration Hub):**
- ✅ Runs ETL pipelines
- ✅ Connects to enrichment APIs
- ⚠️ **Unknown**: VM access controls, firewall rules, IAM roles
- ⚠️ **Unknown**: How secrets are injected into VM

**Recommendation:** Document VM security setup.

### Security Scorecard

| Category | Score | Notes |
|----------|-------|-------|
| **Secrets Management** | 7/10 | Good patterns, but fragmented |
| **Authentication** | 9/10 | RSA keys + service accounts |
| **Access Control** | 8/10 | Least privilege applied |
| **Audit Trail** | 6/10 | Partial logging |
| **Credential Rotation** | 5/10 | Manual, multi-step process |
| **Overall** | **7/10** | Good foundation, needs consolidation |

---

## 4. Recommendations

### Priority 1: Visualization Adapter Layer

**Create:** `utils/viz_adapter.py`

```python
"""
Unified visualization adapter - abstracts warehouse choice
"""
from utils.db_adapter import get_db
import pandas as pd

class VizAdapter:
    """Adapter for visualization tools to query any warehouse"""
    
    def __init__(self):
        self.db = get_db()
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return DataFrame"""
        return self.db.fetch_df(sql)
    
    def get_connection(self):
        """Get native connection (for tools that need it)"""
        # Returns Snowflake connector or BigQuery client
        return self.db.conn if hasattr(self.db, 'conn') else self.db.client
```

**Update Streamlit:**
```python
# dashboards/client_dashboard.py
from utils.viz_adapter import VizAdapter

@st.cache_data(ttl=300)
def load_data():
    viz = VizAdapter()
    state_insights = viz.query("SELECT * FROM CLEAN.V_STATE_INSIGHTS...")
    return state_insights, county_density, ...
```

**Benefits:**
- ✅ Streamlit works with Snowflake OR BigQuery
- ✅ Single codebase for both warehouses
- ✅ Consistent with ETL pattern

### Priority 2: Centralized Secret Management

**Option A: GCP Secret Manager for All**
- Store all secrets in GCP Secret Manager
- Tools fetch via API/CLI
- Single source of truth

**Option B: Terraform/Infrastructure as Code**
- Define all connections in Terraform
- Auto-provision Looker connections
- Auto-configure Hex via API (if available)

**Recommendation:** Option A (simpler, leverages existing Secret Manager).

### Priority 3: Looker Multi-Warehouse Support

**Create:** `looker/dental_leads_bigquery.model.lkml`

```lkml
connection: "bigquery_dental_leads"  # Separate connection
# ... same explores, adapted for BigQuery SQL
```

**Or:** Use Looker's connection parameters to swap dynamically (if supported).

### Priority 4: Documentation

**Create:** `docs/SECURITY.md`
- VM security configuration
- Firewall rules
- IAM roles
- Secret rotation procedures
- Incident response plan

---

## 5. Architecture Diagram (Current vs Recommended)

### Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GCP VM (Orchestration)                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ETL Pipelines                                        │   │
│  │  ✅ Uses db_adapter.py (swappable)                    │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │   GCS Bucket     │
              │  (Data Lake)     │
              └────────┬──────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
┌───────────────┐           ┌───────────────┐
│   Snowflake   │           │   BigQuery    │
│  (DENTAL_     │           │  (dental_     │
│   LEADS)      │           │   leads)      │
└───────┬───────┘           └───────┬───────┘
        │                           │
        │                           │
    ┌───┴────┬──────────┬───────────┴────┐
    │        │          │                 │
    ▼        ▼          ▼                 ▼
┌────────┐ ┌──────┐ ┌────────┐      ┌────────┐
│Streamlit│ │Looker│ │  Hex   │      │  Hex   │
│(SF only)│ │(SF)  │ │ (SF)   │      │ (BQ)   │
└────────┘ └──────┘ └────────┘      └────────┘
```

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GCP VM (Orchestration)                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ETL Pipelines                                        │   │
│  │  ✅ Uses db_adapter.py                               │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │   GCS Bucket     │
              │  (Data Lake)     │
              └────────┬──────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
┌───────────────┐           ┌───────────────┐
│   Snowflake   │           │   BigQuery    │
└───────┬───────┘           └───────┬───────┘
        │                           │
        └───────────┬───────────────┘
                    │
            ┌───────▼────────┐
            │  viz_adapter.py│  ← NEW: Unified interface
            └───────┬────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
        ▼           ▼           ▼
┌────────┐    ┌──────┐    ┌────────┐
│Streamlit│    │Looker│    │  Hex   │
│(Any WH)│    │(Any) │    │ (Any)  │
└────────┘    └──────┘    └────────┘
```

---

## 6. Action Items

### Immediate (This Week)
- [ ] Create `utils/viz_adapter.py` for visualization abstraction
- [ ] Update Streamlit to use `viz_adapter.py`
- [ ] Document VM security configuration

### Short Term (This Month)
- [ ] Migrate all secrets to GCP Secret Manager
- [ ] Create credential rotation script
- [ ] Add unified audit logging

### Long Term (Next Quarter)
- [ ] Create BigQuery Looker model
- [ ] Automate Hex connection setup (if API available)
- [ ] Implement infrastructure as code (Terraform)

---

## 7. Conclusion

**Warehouse Swappability:** ✅ **EXCELLENT**
- Well-architected adapter pattern
- Easy to swap between Snowflake and BigQuery

**Visualization Swappability:** ⚠️ **NEEDS WORK**
- Tools hardcoded to Snowflake
- Need adapter layer for consistency

**Security:** ⚠️ **GOOD FOUNDATION, NEEDS CONSOLIDATION**
- Strong authentication (RSA keys)
- Good secrets management patterns
- But fragmented across tools
- Needs centralized audit trail

**Overall:** Your architecture is **well-designed for warehouse swapping**, but **visualization tools need abstraction** to match. Security is solid but could benefit from consolidation.

---

*Assessment Date: December 2024*
*Assessed By: AI Architecture Review*


