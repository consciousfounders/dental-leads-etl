# ============================================================
# Practice Decision Makers View
# ============================================================
# 66K matched practice owners - high-value sales targets
# Auth official matched to individual dentist NPI
# ============================================================

view: practice_decision_makers {
  sql_table_name: DENTAL_LEADS.CLEAN.PRACTICE_DECISION_MAKERS ;;

  # ============================================================
  # Keys
  # ============================================================
  
  dimension: organization_npi {
    type: string
    sql: ${TABLE}.ORGANIZATION_NPI ;;
    description: "Organization's NPI"
  }
  
  dimension: individual_npi {
    primary_key: yes
    type: string
    sql: ${TABLE}.INDIVIDUAL_NPI ;;
    description: "Matched dentist's individual NPI"
  }

  # ============================================================
  # Organization Info
  # ============================================================
  
  dimension: organization_name {
    type: string
    sql: ${TABLE}.ORGANIZATION_NAME ;;
  }
  
  dimension: dba_name {
    type: string
    sql: ${TABLE}.DBA_NAME ;;
    description: "Doing Business As name"
  }
  
  dimension: org_city {
    type: string
    sql: ${TABLE}.ORG_CITY ;;
  }
  
  dimension: org_state {
    type: string
    sql: ${TABLE}.ORG_STATE ;;
    map_layer_name: us_states
  }
  
  dimension: org_phone {
    type: string
    sql: ${TABLE}.ORG_PHONE ;;
  }

  # ============================================================
  # Decision Maker (Dentist Owner)
  # ============================================================
  
  dimension: dentist_name {
    type: string
    sql: ${TABLE}.DENTIST_NAME ;;
    description: "Practice owner's name"
  }
  
  dimension: first_name {
    type: string
    sql: ${TABLE}.FIRST_NAME ;;
  }
  
  dimension: last_name {
    type: string
    sql: ${TABLE}.LAST_NAME ;;
  }
  
  dimension: gender {
    type: string
    sql: ${TABLE}.GENDER ;;
  }
  
  dimension: dentist_phone {
    type: string
    sql: ${TABLE}.DENTIST_PHONE ;;
    description: "Owner's direct phone"
  }

  # ============================================================
  # Match Quality
  # ============================================================
  
  dimension: match_confidence {
    type: string
    sql: ${TABLE}.MATCH_CONFIDENCE ;;
    description: "HIGH = same city, MEDIUM = same state"
  }
  
  dimension: practice_age_cohort {
    type: string
    sql: ${TABLE}.PRACTICE_AGE_COHORT ;;
  }

  # ============================================================
  # Specialty
  # ============================================================
  
  dimension: dentist_taxonomy {
    type: string
    sql: ${TABLE}.DENTIST_TAXONOMY ;;
  }

  # ============================================================
  # Measures
  # ============================================================
  
  measure: count {
    type: count
    drill_fields: [organization_name, dentist_name, org_city, org_state, dentist_phone]
  }
  
  measure: count_high_confidence {
    type: count
    filters: [match_confidence: "HIGH"]
    description: "Matches where dentist is in same city as practice"
  }
  
  measure: count_male {
    type: count
    filters: [gender: "Male"]
  }
  
  measure: count_female {
    type: count
    filters: [gender: "Female"]
  }
}

