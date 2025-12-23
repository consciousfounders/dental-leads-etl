# Texas Dental License Data - Schema Analysis

## Data Summary

| File | Records | Size | Record Type |
|------|---------|------|-------------|
| Dentist.csv | 39,853 | 13.4 MB | Licensed Dentist |
| Hygienist.csv | 27,970 | 7.4 MB | Registered Dental Hygienist |
| DentalAssistant.csv | 127,872 | 28.7 MB | Registered Dental Assistant |
| Labs.csv | 4,099 | 897 KB | Dental Laboratory |
| ETN.csv | 1,266 | 177 KB | Exception Tracking Number |

**Total: ~201,000 records across 5 professional types**

---

## Schema Comparison

### Common Fields (All Professional Types)

| Field | Dentist | Hygienist | Assistant | Labs | ETN | Notes |
|-------|---------|-----------|-----------|------|-----|-------|
| REC_TYPE | ✓ | ✓ | ✓ | ✓ | ✓ | Professional type identifier |
| LIC_ID | ✓ | ✓ | ✓ | ✓ | - | Internal ID (unique per file) |
| LIC_NBR | ✓ | ✓ | ✓ | ✓ | ✓ | License number |
| LIC_STA_CDE | ✓ | ✓ | ✓ | ✓ | ✓ | Status code (20, 45, 60, etc.) |
| LIC_STA_DESC | ✓ | ✓ | ✓ | ✓ | ✓ | Status description |
| LIC_ORIG_DTE | ✓ | ✓ | ✓ | ✓ | - | Original issue date |
| LIC_EXPR_DTE | ✓ | ✓ | ✓ | ✓ | ✓ | Expiration date |
| ENTITY_NBR | ✓ | ✓ | ✓ | ✓ | ✓ | Entity reference number |
| REMEDIAL_PLNS | ✓ | ✓ | ✓ | ✓ | - | Remedial plans flag |

### Address Fields

| Field | Dentist | Hygienist | Assistant | Labs | ETN |
|-------|---------|-----------|-----------|------|-----|
| ADDRESS1 | ✓ | ✓ | ✓ | ✓ | - |
| ADDRESS2 | ✓ | ✓ | ✓ | ✓ | - |
| CITY | ✓ | ✓ | ✓ | ✓ | ✓ |
| STATE | ✓ | ✓ | ✓ | ✓ | ✓ |
| ZIP | ✓ | ✓ | ✓ | ✓ | ✓ |
| COUNTY | ✓ | ✓ | ✓ | ✓ | ✓ |
| COUNTRY | ✓ | ✓ | ✓ | ✓ | ✓ |
| PHONE | ✓ | ✓ | ✓ | ✓ | - |

### Personal Info Fields

| Field | Dentist | Hygienist | Assistant | Labs | ETN |
|-------|---------|-----------|-----------|------|-----|
| FIRST_NME | ✓ | ✓ | ✓ | - | ✓ |
| MIDDLE_NME | ✓ | ✓ | ✓ | - | ✓ |
| LAST_NME | ✓ | ✓* | ✓ | - | ✓ |
| FORMER_LAST_NME | ✓ | ✓ | - | - | - |
| GENDER | ✓ | ✓ | ✓ | - | - |
| BIRTH_YEAR | ✓ | ✓ | ✓ | - | - |
| GRAD_YR | ✓ | ✓ | - | - | - |
| SCHOOL | ✓ | ✓ | - | - | - |

*Note: Hygienist has typo `LAST_MNE` instead of `LAST_NME`

### Professional-Specific Fields

#### Dentist Only (38 columns)
| Field | Description | Sample Values |
|-------|-------------|---------------|
| NOX_PERMIT_DTE | Nitrous oxide permit date | Date or "No Permit" |
| LEVEL_1_DTE | Anesthesia Level 1 permit date | Date or "No Permit" |
| LEVEL_2_DTE | Anesthesia Level 2 permit date | Date or "No Permit" |
| LEVEL_3_DTE | Anesthesia Level 3 permit date | Date or "No Permit" |
| LEVEL_4_DTE | Anesthesia Level 4 permit date | Date or "No Permit" |
| PORTABILITY | License portability flag | Yes/No |
| PRAC_DESC | Practice description | Private, Military, Government, Faculty, Retired, Other |
| PRAC_TYPES | Specialty/Practice type | GEN, ORTH, OMS, PEDO, PERI, END, PROS, DPH, etc. |
| SHRP_MOD | SHRP modifier | No, Yes Level 2, Yes Levels 2-3, Yes Levels 2-4 |
| SPP_MOD | SPP modifier | No, Yes Level 2, Yes Levels 2-3, Yes Levels 2-4 |
| ERX_WAIVER | E-prescribing waiver | Yes/No/blank |
| LEVEL_EXEMPT | Level exemption flag | Yes/No |

#### Hygienist Only (29 columns)
| Field | Description | Sample Values |
|-------|-------------|---------------|
| SEALANT | Sealant certification | Yes/No |
| NOM_MOD | Nitrous oxide monitoring modifier | Yes/No |
| LIA_MOD | Local infiltration anesthesia modifier | Yes/No |

#### Dental Assistant Only (24 columns)
| Field | Description | Sample Values |
|-------|-------------|---------------|
| NOM_MOD | Nitrous oxide monitoring modifier | Yes/No |

#### Labs Only (23 columns)
| Field | Description | Sample Values |
|-------|-------------|---------------|
| LAB_NME | Laboratory name | Business name |
| LAB_TYPE | Laboratory type | Commercial, Licensed TX DDS, Gov/Educational, etc. |
| LAB_OWNER | Lab owner name | Person name |
| LAB_MANAGER | Lab manager name | Person name |
| LAB_CDT | Certified Dental Technician | Person name |

#### ETN Only (16 columns)
| Field | Description | Sample Values |
|-------|-------------|---------------|
| RANK_EFCT_DTE | Rank effective date | Date |
| ERX_WAIVER | E-prescribing waiver | Yes/No/blank |

---

## License Status Codes

| Code | Description | Dentist | Hygienist | Assistant | Labs |
|------|-------------|---------|-----------|-----------|------|
| 20 | Active | 20,345 | 16,187 | 47,549 | 622 |
| 45 | Expired | 780 | 604 | 7,947 | 48 |
| 46 | Active/Probate | 46 | 2 | 57 | 1 |
| 47 | Enf Suspension | 17 | 1 | 2 | - |
| 48 | Expired - NSF | - | 2 | 8 | - |
| 60 | Cancelled | 8,821 | 6,993 | 72,165 | 2,275 |
| 61 | Revoked | 87 | 7 | 57 | 8 |
| 65 | Vol Surrender | 143 | 17 | 33 | 2 |
| 70 | Charity | 46 | - | - | - |
| 71 | Retired | - | - | 36 | - |
| 72 | Retired | 5,824 | 4,029 | - | - |
| 80 | Deceased/Closed | 3,744 | 128 | 15 | 1,143 |

### Status Categories for Marketing

| Category | Codes | Use Case |
|----------|-------|----------|
| **ACTIVE** | 20, 46, 70 | Current prospects/customers |
| **LAPSED** | 45, 48, 60 | Win-back campaigns |
| **INACTIVE** | 61, 65, 71, 72 | Suppress from outreach |
| **DECEASED** | 80 | Remove from all lists |
| **SUSPENDED** | 47 | Monitor for reinstatement |

---

## Specialty/Practice Types (Dentists)

| Code | Description | Count |
|------|-------------|-------|
| GEN | General Dentistry | 18,709 |
| ORTH | Orthodontics | 1,456 |
| OMS | Oral & Maxillofacial Surgery | 1,160 |
| PEDO | Pediatric Dentistry | 1,055 |
| PERI | Periodontics | 765 |
| PROS | Prosthodontics | 673 |
| END | Endodontics | 620 |
| DPH | Dental Public Health | 182 |
| OMP | Oral Medicine/Pathology | 63 |
| OFPN | Orofacial Pain | 36 |
| OMR | Oral & Maxillofacial Radiology | 35 |
| ANE | Dental Anesthesiology | 20 |
| DANE | Dental Anesthesia (legacy?) | 15 |
| OMED | Oral Medicine | 4 |

---

## Certification/Endorsement Fields

### Dentist Certifications
1. **Nitrous Oxide (NOX_PERMIT_DTE)**: ~19,000 with permits
2. **Anesthesia Levels 1-4**: Multi-tier sedation permits
3. **SHRP_MOD**: Sedation permit modifiers
4. **SPP_MOD**: Sedation permit modifiers
5. **Portability**: Interstate practice permit

### Hygienist Certifications
1. **SEALANT**: ~8,224 with certification
2. **NOM_MOD**: Nitrous oxide monitoring
3. **LIA_MOD**: Local infiltration anesthesia (~1,130 with certification)

### Assistant Certifications
1. **NOM_MOD**: Nitrous oxide monitoring (~19,561 with certification)

---

## Data Quality Issues

1. **Column name typo**: Hygienist has `LAST_MNE` instead of `LAST_NME`
2. **Inconsistent specialty encoding**: Some records have practice type in wrong column
3. **Date format**: MM/DD/YYYY - needs parsing
4. **Mixed status codes**: 71 and 72 both mean "Retired" for different types
5. **Empty values**: Many fields have blank values vs "No" vs "No Permit"
6. **County data**: Good for territory mapping

---

## Key Observations for Pipeline Design

1. **Unified schema possible**: Core fields overlap across all types
2. **Type-specific extensions needed**: Certifications vary by professional type
3. **Entity tracking**: ENTITY_NBR may link to NPI data
4. **Historical data included**: Contains cancelled/deceased going back decades
5. **Good change detection candidates**:
   - LIC_STA_CDE changes (status events)
   - LIC_EXPR_DTE changes (renewal events)
   - Address fields (location events)
   - Certification date fields (credential events)
