# ============================================================
# Dental Providers View
# ============================================================
# Main provider table - 366K active US dental providers
# ============================================================

view: dental_providers {
  sql_table_name: DENTAL_LEADS.CLEAN.DENTAL_PROVIDERS ;;

  # ============================================================
  # Primary Keys
  # ============================================================
  
  dimension: npi {
    primary_key: yes
    type: string
    sql: ${TABLE}.NPI ;;
    description: "National Provider Identifier (unique)"
  }

  # ============================================================
  # Entity Type
  # ============================================================
  
  dimension: entity_type {
    type: string
    sql: ${TABLE}.ENTITY_TYPE ;;
    description: "Individual or Organization"
  }
  
  dimension: entity_type_code {
    type: string
    sql: ${TABLE}.ENTITY_TYPE_CODE ;;
    hidden: yes
  }

  # ============================================================
  # Provider Name
  # ============================================================
  
  dimension: display_name {
    type: string
    sql: ${TABLE}.DISPLAY_NAME ;;
    description: "Formatted name for display"
  }
  
  dimension: first_name {
    type: string
    sql: ${TABLE}.FIRST_NAME ;;
  }
  
  dimension: last_name {
    type: string
    sql: ${TABLE}.LAST_NAME ;;
  }
  
  dimension: credential {
    type: string
    sql: ${TABLE}.CREDENTIAL ;;
    description: "DDS, DMD, etc."
  }
  
  dimension: organization_name {
    type: string
    sql: ${TABLE}.ORGANIZATION_NAME ;;
  }

  # ============================================================
  # Location
  # ============================================================
  
  dimension: practice_address_full {
    type: string
    sql: ${TABLE}.PRACTICE_ADDRESS_FULL ;;
    description: "Full street address"
  }
  
  dimension: practice_city {
    type: string
    sql: ${TABLE}.PRACTICE_CITY ;;
  }
  
  dimension: practice_state {
    type: string
    sql: ${TABLE}.PRACTICE_STATE ;;
    map_layer_name: us_states
  }
  
  dimension: practice_zip {
    type: zipcode
    sql: ${TABLE}.PRACTICE_ZIP ;;
  }

  # ============================================================
  # Contact Info
  # ============================================================
  
  dimension: practice_phone_clean {
    type: string
    sql: ${TABLE}.PRACTICE_PHONE_CLEAN ;;
    description: "Phone number (digits only)"
  }

  # ============================================================
  # Specialty
  # ============================================================
  
  dimension: primary_taxonomy_code {
    type: string
    sql: ${TABLE}.PRIMARY_TAXONOMY_CODE ;;
    description: "Dental specialty code"
  }

  # ============================================================
  # Demographics
  # ============================================================
  
  dimension: gender {
    type: string
    sql: ${TABLE}.GENDER ;;
  }

  # ============================================================
  # Practice Age (NPI-based, see notes)
  # ============================================================
  
  dimension: practice_age_cohort {
    type: string
    sql: ${TABLE}.PRACTICE_AGE_COHORT ;;
    description: "Based on NPI registration date (NOT true practice age)"
  }
  
  dimension_group: enumeration {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.ENUMERATION_DATE ;;
    description: "NPI registration date"
  }

  # ============================================================
  # Measures
  # ============================================================
  
  measure: count {
    type: count
    drill_fields: [npi, display_name, practice_city, practice_state]
  }
  
  measure: count_individuals {
    type: count
    filters: [entity_type: "Individual"]
  }
  
  measure: count_organizations {
    type: count
    filters: [entity_type: "Organization"]
  }
}

