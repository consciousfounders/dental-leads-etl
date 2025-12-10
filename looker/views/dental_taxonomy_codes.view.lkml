# ============================================================
# Dental Taxonomy Codes View
# ============================================================
# Reference table for dental specialties
# ============================================================

view: dental_taxonomy_codes {
  sql_table_name: DENTAL_LEADS.CLEAN.DENTAL_TAXONOMY_CODES ;;

  dimension: taxonomy_code {
    primary_key: yes
    type: string
    sql: ${TABLE}.TAXONOMY_CODE ;;
  }
  
  dimension: specialty_name {
    type: string
    sql: ${TABLE}.SPECIALTY_NAME ;;
    description: "Human-readable specialty name"
  }
  
  dimension: description {
    type: string
    sql: ${TABLE}.DESCRIPTION ;;
  }
}

