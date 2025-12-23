# State Dental License Data - Priority Matrix

## Quick Reference: Automated vs Manual Access

### Fully Automated (No Human Intervention)

| State | Est. Dentists | Access Method | Format | Update Freq | Direct URL |
|-------|---------------|---------------|--------|-------------|------------|
| **TX** | 20,000+ | Direct CSV | CSV | Daily | [Download](https://ls.tsbde.texas.gov/lib/csv/Dentist.csv) |
| **WA** | 8,000+ | Open Data Portal | CSV/API | Daily | [data.wa.gov](https://data.wa.gov/health/Health-Care-Provider-Credential-Data/qxh8-f4bd) |
| **CO** | 6,000+ | Open Data Portal | CSV/API | Nightly | [data.colorado.gov](https://data.colorado.gov/Regulations/Professional-and-Occupational-Licenses-in-Colorado/7s5z-vewr) |
| **FL** | 15,000+ | Public Records Portal | ASCII/CSV | Weekly | [DBPR Portal](https://www2.myfloridalicense.com/instant-public-records/) |

### Requires One-Time Setup (API Key / Account)

| State | Est. Dentists | Access Method | Setup Required | Format |
|-------|---------------|---------------|----------------|--------|
| **CA** | 40,000+ | DCA Search API | API access request form | JSON/API |
| **NY** | 20,000+ | Open Data Portal | May need account | CSV/API |

### Requires Payment (But Automated After)

| State | Est. Dentists | Access Method | Cost | Format |
|-------|---------------|---------------|------|--------|
| **VA** | 7,000+ | Virginia Interactive | $100 + $20/1000 records | CSV/Excel |
| **AZ** | 5,000+ | Board Request | $100 for mailing list | Digital |

### Requires FOIA / Public Records Request

| State | Est. Dentists | Contact | Notes |
|-------|---------------|---------|-------|
| **PA** | 12,000+ | st-dentistry@pa.gov | Right-to-Know request |
| **OH** | 9,000+ | dental.board@den.ohio.gov | Public records request |
| **IL** | 11,000+ | (217) 782-0458 | Bulk lookup exists but not export |
| **MI** | 8,000+ | LARA FOIA | FOIA request needed |
| **GA** | 8,000+ | gbd.georgia.gov | Contact board |
| **NC** | 7,000+ | info@ncdentalboard.org | Contact board |
| **NJ** | 9,000+ | askconsumeraffairs@dca.lps.state.nj.us | Contact DCA |
| **MA** | 7,000+ | (800) 414-0168 | Public records request |

---

## Priority Tiers by Effort/Value

### 🟢 TIER 1: Automate Now (Week 1)
*No human intervention required - can script immediately*

| Priority | State | Dentists | Why Priority | Action |
|----------|-------|----------|--------------|--------|
| 1 | **Texas** ✅ | 20,345 | Already done, daily CSV | ✅ Complete |
| 2 | **Washington** | ~8,000 | Free open data, CSV/API | `curl` the API |
| 3 | **Colorado** | ~6,000 | Free open data, nightly updates | `curl` the API |
| 4 | **Florida** | ~15,000 | 3rd largest state, free download | Navigate portal once, then automate |

**Tier 1 Total: ~50,000 dentists**

### 🟡 TIER 2: Quick Setup (Week 2)
*Requires 15-30 min human action, then automated*

| Priority | State | Dentists | Setup Required | Action |
|----------|-------|----------|----------------|--------|
| 5 | **California** | ~40,000 | Submit API access form | [Request API](https://search.dca.ca.gov/api) |
| 6 | **New York** | ~20,000 | Check data.ny.gov | Search for license dataset |
| 7 | **Virginia** | ~7,000 | Pay $100-200, get download | [Virginia Interactive](https://dhp.virginiainteractive.org/Home/SDownloadInfo) |
| 8 | **Arizona** | ~5,000 | Pay $100 for mailing list | Contact jill.barenbaum@dentalboard.az.gov |

**Tier 2 Total: ~72,000 dentists**

### 🔴 TIER 3: FOIA Required (Week 3-4)
*Requires formal request, 1-4 week response time*

| Priority | State | Dentists | Request Method |
|----------|-------|----------|----------------|
| 9 | **Pennsylvania** | ~12,000 | Right-to-Know Law request |
| 10 | **Illinois** | ~11,000 | FOIA to IDFPR |
| 11 | **Ohio** | ~9,000 | Public records request |
| 12 | **New Jersey** | ~9,000 | OPRA request |
| 13 | **Michigan** | ~8,000 | FOIA to LARA |
| 14 | **Georgia** | ~8,000 | Open Records Act |
| 15 | **North Carolina** | ~7,000 | Public records request |
| 16 | **Massachusetts** | ~7,000 | Public records request |

**Tier 3 Total: ~71,000 dentists**

---

## Cumulative Coverage

| After Tier | States | Est. Active Dentists | % of US Dentists (~200K) |
|------------|--------|----------------------|--------------------------|
| Tier 1 | 4 | ~50,000 | 25% |
| Tier 2 | 8 | ~122,000 | 61% |
| Tier 3 | 16 | ~193,000 | 97% |

---

## Immediate Action Items for You

### This Week (Tier 1 - Automated)
Nothing needed from you! I can create scripts to:
- ✅ Texas - already downloading
- [ ] Washington - data.wa.gov API
- [ ] Colorado - data.colorado.gov API
- [ ] Florida - find exact download URL in portal

### Next Week (Tier 2 - Quick Setup)

| Task | Time | Link |
|------|------|------|
| CA: Submit API access form | 5 min | [DCA API Request](https://search.dca.ca.gov/api) |
| NY: Check data.ny.gov for "My License" | 10 min | [data.ny.gov](https://data.ny.gov/Government-Finance/My-License/xkwa-k8qn) |
| VA: Pay $100-200 for download access | 10 min | [Virginia Interactive](https://dhp.virginiainteractive.org/Home/SDownloadInfo) |
| AZ: Email for mailing list ($100) | 5 min | jill.barenbaum@dentalboard.az.gov |

### Following Weeks (Tier 3 - FOIA)
I can draft a template FOIA letter for you to send to the remaining states.

---

## Data Availability Summary Table

| State | Rank by Dentists | Auto Download | API | Open Data | Paid | FOIA |
|-------|------------------|---------------|-----|-----------|------|------|
| CA | 1 | | ✅ | | | |
| TX | 2 | ✅ | | | | |
| NY | 3 | | ✅? | ✅? | | |
| FL | 4 | ✅ | | | | |
| PA | 5 | | | | | ✅ |
| IL | 6 | | | | | ✅ |
| OH | 7 | | | | | ✅ |
| NJ | 8 | | | | | ✅ |
| MI | 9 | | | | | ✅ |
| GA | 10 | | | | | ✅ |
| WA | 11 | ✅ | ✅ | ✅ | | |
| NC | 12 | | | | | ✅ |
| VA | 13 | | | | ✅ | |
| MA | 14 | | | | | ✅ |
| CO | 15 | ✅ | ✅ | ✅ | | |
| AZ | 16 | | | | ✅ | |

---

## Scripts I Can Create Now

1. **fetch_washington.py** - Pull from data.wa.gov API
2. **fetch_colorado.py** - Pull from data.colorado.gov API
3. **fetch_florida.py** - Download from DBPR portal (need to verify exact URL)
4. **foia_template.md** - Standard public records request letter

Want me to create these?
