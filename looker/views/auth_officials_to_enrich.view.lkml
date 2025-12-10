# ============================================================
# Auth Officials to Enrich View
# ============================================================
# 38K auth officials needing Wiza/Apollo enrichment
# ============================================================

view: auth_officials_to_enrich {
  sql_table_name: DENTAL_LEADS.CLEAN.AUTH_OFFICIALS_TO_ENRICH ;;

  dimension: organization_npi {
    primary_key: yes
    type: string
    sql: ${TABLE}.ORGANIZATION_NPI ;;
  }
  
  dimension: organization_name {
    type: string
    sql: ${TABLE}.ORGANIZATION_NAME ;;
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
  
  dimension: auth_official_full_name {
    type: string
    sql: ${TABLE}.AUTH_OFFICIAL_FULL_NAME ;;
    description: "Name to search in Wiza/Apollo"
  }
  
  dimension: auth_official_credential {
    type: string
    sql: ${TABLE}.AUTH_OFFICIAL_CREDENTIAL ;;
  }
  
  dimension: credential_category {
    type: string
    sql: ${TABLE}.CREDENTIAL_CATEGORY ;;
    description: "dentist_no_npi_match, unknown, business_professional, etc."
  }
  
  dimension: enrichment_priority {
    type: number
    sql: ${TABLE}.ENRICHMENT_PRIORITY ;;
    description: "1=highest priority"
  }
  
  dimension: enriched_email {
    type: string
    sql: ${TABLE}.ENRICHED_EMAIL ;;
    description: "Populated after Wiza enrichment"
  }
  
  dimension: enriched_linkedin_url {
    type: string
    sql: ${TABLE}.ENRICHED_LINKEDIN_URL ;;
  }
  
  dimension: is_enriched {
    type: yesno
    sql: ${TABLE}.ENRICHED_EMAIL IS NOT NULL ;;
  }

  measure: count {
    type: count
    drill_fields: [organization_name, auth_official_full_name, credential_category]
  }
  
  measure: count_enriched {
    type: count
    filters: [is_enriched: "yes"]
  }
  
  measure: enrichment_rate {
    type: number
    sql: ${count_enriched} / NULLIF(${count}, 0) ;;
    value_format_name: percent_1
  }
}

