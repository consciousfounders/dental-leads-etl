# State Dental License Data Sources

## Summary by Access Type

| Access Type | States | Notes |
|-------------|--------|-------|
| **Direct CSV Download** | TX | Best case - daily updates, full database |
| **Bulk API** | CA | Requires access request |
| **Bulk Lookup Tool** | IL | Batch verification, not full export |
| **Weekly File Downloads** | FL (DBPR) | ASCII text format |
| **Open Data Portal** | NY | May have license datasets |
| **Individual Lookup Only** | PA, OH, MA, VA, SC | Requires scraping or FOIA |

---

## Tier 1: Direct Bulk Download (Self-Service)

### Texas ⭐ BEST
- **Source**: [Texas State Board of Dental Examiners](https://tsbde.texas.gov/resources/licensee-lists/)
- **Format**: CSV (comma-separated text)
- **Update Frequency**: Every 24 hours
- **Data Available**:
  - Dentists
  - Dental Hygienists
  - Registered Dental Assistants
  - Dental Laboratories
  - Exception Tracking Numbers (ETN)
- **Direct URLs**:
  - https://ls.tsbde.texas.gov/lib/csv/Dentist.csv
  - https://ls.tsbde.texas.gov/lib/csv/Hygienist.csv
  - https://ls.tsbde.texas.gov/lib/csv/DentalAssistant.csv
  - https://ls.tsbde.texas.gov/lib/csv/Labs.csv
  - https://ls.tsbde.texas.gov/lib/csv/ETN.csv
- **Record Counts** (Dec 2024):
  - Dentists: ~40,000
  - Hygienists: ~28,000
  - Assistants: ~128,000
  - Labs: ~4,100
  - ETN: ~1,300

---

## Tier 2: Bulk Download via Portal/Request

### Florida
- **Source**: [Florida DBPR](https://www2.myfloridalicense.com/instant-public-records/)
- **Format**: ASCII text (quote/comma delimited)
- **Update Frequency**: Weekly
- **Access**: Free download from public records portal
- **Notes**:
  - Must navigate to Instant Public Records section
  - Dental licenses under Department of Health (DOH), not DBPR
  - May need to check [DOH MQA Portal](https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders)
- **Public Records Request**: [Open Government Portal](https://www2.myfloridalicense.com/open-government/)

### California
- **Source**: [California DCA](https://www.dca.ca.gov/data/index.shtml)
- **Format**: Standard file format (CSV available)
- **API**: [DCA Search API](https://search.dca.ca.gov/api) - requires access request
- **Notes**:
  - Comprehensive lists of professional licensees available
  - API requires "known list" of license numbers
  - Must request API access through online form
  - BreEZe system for individual lookups
- **Contact**: DentalBoard@dca.ca.gov, 877-729-7789

### New York
- **Source**: [NY Open Data Portal](https://data.ny.gov/)
- **Dataset**: ["My License"](https://data.ny.gov/Government-Finance/My-License/xkwa-k8qn)
- **Format**: CSV, JSON, API
- **Notes**:
  - Open data portal may have professional license datasets
  - NYSED Office of Professions handles dental licensing
  - Need to verify if dental specifically included
- **Contact**: dentbd@nysed.gov, 518-474-3817

---

## Tier 3: Bulk Lookup (Not Full Export)

### Illinois
- **Source**: [IDFPR Bulk License Look Up](https://idfpr.illinois.gov/licenselookup/bulklookup.html)
- **Format**: Web form results
- **Notes**:
  - Approved by Joint Commission and NCQA for verification
  - Requires list of known license numbers (can't export full database)
  - Good for validation, not initial discovery
  - New CORE system launched Oct 2024
- **Contact**: 217-782-0458

---

## Tier 4: Individual Lookup Only (Requires FOIA/Scraping)

### Pennsylvania
- **Source**: [PALS - Pennsylvania Licensing System](https://www.pals.pa.gov/)
- **Lookup**: Individual searches only
- **For Bulk Data**: Submit Right-to-Know request
- **Contact**: st-dentistry@pa.gov, 717-783-7162

### Ohio
- **Source**: [eLicense Ohio](https://elicense.ohio.gov/oh_verifylicense?board=Dental+Board)
- **Lookup**: Individual searches only
- **For Bulk Data**: Contact board or submit public records request
- **Contact**: dental.board@den.ohio.gov, 614-466-2580

### Virginia
- **Source**: [Virginia Board of Dentistry](https://www.dhp.virginia.gov/Boards/Dentistry/)
- **Lookup**: License lookup and verification available
- **For Bulk Data**: Public records request likely needed

### Massachusetts
- **Source**: [Board of Registration in Dentistry](https://www.mass.gov/orgs/board-of-registration-in-dentistry)
- **Lookup**: Individual verification
- **For Bulk Data**: Public records request

### South Carolina
- **Source**: [SC Board of Dentistry](https://llr.sc.gov/bod/)
- **Lookup**: Individual verification
- **For Bulk Data**: FOIA request

---

## Federal/National Sources

### Data.gov
- **URL**: [data.gov dental datasets](https://catalog.data.gov/dataset/?tags=dentist&res_format=CSV)
- **Available Datasets**:
  - NOHSS Adult Oral Health Indicators (CDC/HHS) - 2012-2020
  - Washington State Insurers Dental Loss Ratios
  - Maryland School Children Dental Studies (historical)
  - Chicago Oral Health Metrics
- **Note**: These are aggregate/statistical, NOT individual license records

### NPI Registry
- **URL**: [NPPES NPI Registry](https://npiregistry.cms.hhs.gov/)
- **Format**: Downloadable files available
- **Notes**:
  - Already integrated (369K dentist records)
  - Can cross-reference with state license data via name/address matching

---

## Data Acquisition Strategy by Priority

### Phase 1: Direct Download (Immediate)
1. **Texas** ✅ (Done) - Full CSV download, daily updates
2. **Florida** - Check DBPR/DOH public records portal
3. **California** - Request DCA API access + bulk file download

### Phase 2: Open Data Portals
4. **New York** - Check data.ny.gov for license datasets
5. Other states with open data initiatives

### Phase 3: Bulk Lookup Enhancement
6. **Illinois** - Build license number list from NPI, then bulk verify

### Phase 4: FOIA/Public Records Requests
7. Large states without self-service: PA, OH, GA, NC, NJ, MI
8. Template public records request letter
9. Track response times and costs

### Phase 5: Web Scraping (Last Resort)
- Only for states with no other option
- Respect rate limits and robots.txt
- Consider legal review first

---

## Recommended Next Steps

1. **Florida Deep Dive**: Navigate the DBPR/DOH portals to find the actual bulk download location for dental licenses

2. **California API Request**: Submit access request to DCA Search API

3. **New York Data Portal**: Search data.ny.gov for "professional license" or "dental" datasets

4. **FOIA Template**: Create a standardized public records request for states without self-service:
   - Ohio
   - Pennsylvania
   - Michigan
   - Georgia
   - North Carolina
   - New Jersey

5. **State Board Contact List**: Build a spreadsheet with:
   - State
   - Board name
   - Phone
   - Email
   - Website
   - Data format (if known)
   - Request method
   - Response time
   - Cost (if any)

---

## Schema Considerations for Multi-State

Based on Texas schema analysis, key variations to expect:

| Field | Texas | Other States (Expected) |
|-------|-------|------------------------|
| Status codes | 20=Active, 60=Cancelled, etc. | Will vary |
| Specialty codes | GEN, ORTH, OMS, etc. | Different abbreviations |
| Date format | MM/DD/YYYY | May vary |
| Certification fields | NOX_PERMIT_DTE, LEVEL_1-4_DTE | Different cert types |
| County | Included | May or may not have |

The staging layer design accommodates these variations with state-specific mapping in each `stg_{state}_{type}.sql` model.
