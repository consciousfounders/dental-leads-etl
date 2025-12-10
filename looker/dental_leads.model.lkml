# ============================================================
# Dental Leads - Looker Model
# ============================================================
# This file defines the LookML model for the Dental Leads platform
# Import into Looker after connecting to Snowflake
# ============================================================

connection: "snowflake_dental_leads"

# Include all view files
include: "/views/*.view.lkml"

# ============================================================
# Explores (Entry Points for Analysis)
# ============================================================

explore: dental_providers {
  label: "Dental Providers"
  description: "366K active US dental providers from NPI registry"
  
  join: dental_taxonomy_codes {
    type: left_outer
    sql_on: ${dental_providers.primary_taxonomy_code} = ${dental_taxonomy_codes.taxonomy_code} ;;
    relationship: many_to_one
  }
}

explore: practice_decision_makers {
  label: "Practice Decision Makers"
  description: "66K matched practice owners (auth official = individual dentist)"
  
  join: dental_taxonomy_codes {
    type: left_outer
    sql_on: ${practice_decision_makers.dentist_taxonomy} = ${dental_taxonomy_codes.taxonomy_code} ;;
    relationship: many_to_one
  }
}

explore: auth_officials_to_enrich {
  label: "Auth Officials to Enrich"
  description: "38K auth officials needing Wiza/Apollo enrichment"
}

explore: enriched_sample {
  label: "Enriched Sample (Wiza)"
  description: "84 Wiza-matched records with email/LinkedIn"
  from: dental_providers_sample
}

