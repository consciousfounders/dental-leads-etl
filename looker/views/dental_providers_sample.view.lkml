# ============================================================
# Enriched Dental Providers Sample View
# ============================================================
# 84 Wiza-enriched records with email, phone, LinkedIn
# ============================================================

view: dental_providers_sample {
  sql_table_name: DENTAL_LEADS.ENRICHED.DENTAL_PROVIDERS_SAMPLE ;;

  dimension: npi {
    primary_key: yes
    type: string
    sql: ${TABLE}.NPI ;;
  }
  
  dimension: display_name {
    type: string
    sql: ${TABLE}.DISPLAY_NAME ;;
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
  
  dimension: specialty_name {
    type: string
    sql: ${TABLE}.SPECIALTY_NAME ;;
  }

  # Enriched Fields
  dimension: enriched_email {
    type: string
    sql: ${TABLE}.ENRICHED_EMAIL ;;
  }
  
  dimension: email_type {
    type: string
    sql: ${TABLE}.EMAIL_TYPE ;;
    description: "personal or work"
  }
  
  dimension: enriched_phone_1 {
    type: string
    sql: ${TABLE}.ENRICHED_PHONE_1 ;;
  }
  
  dimension: linkedin_profile_url {
    type: string
    sql: ${TABLE}.LINKEDIN_PROFILE_URL ;;
  }
  
  dimension: wiza_company {
    type: string
    sql: ${TABLE}.WIZA_COMPANY ;;
  }
  
  dimension: wiza_title {
    type: string
    sql: ${TABLE}.WIZA_TITLE ;;
  }

  measure: count {
    type: count
    drill_fields: [display_name, enriched_email, practice_city, practice_state]
  }
  
  measure: count_work_email {
    type: count
    filters: [email_type: "work"]
  }
  
  measure: count_personal_email {
    type: count
    filters: [email_type: "personal"]
  }
}

