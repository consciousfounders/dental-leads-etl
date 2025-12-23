-- fct_license_events.sql
-- Fact table that detects and categorizes license change events
-- These events drive downstream marketing automations

{{ config(
    materialized='incremental',
    unique_key='event_id',
    schema='marts',
    on_schema_change='append_new_columns'
) }}

with snapshot_changes as (
    -- Get all snapshot records with their validity periods
    select
        *,
        dbt_valid_from as effective_from,
        coalesce(dbt_valid_to, current_timestamp()) as effective_to,
        dbt_valid_to is null as is_current,
        lag(dbt_scd_id) over (
            partition by professional_id
            order by dbt_valid_from
        ) as previous_record_id
    from {{ ref('snp_dental_professionals') }}
),

current_and_previous as (
    -- Join current record with its previous version
    select
        curr.professional_id,
        curr.state_code,
        curr.professional_type,
        curr.license_number,
        curr.first_name,
        curr.last_name,
        curr.effective_from as event_timestamp,

        -- Current values
        curr.status_code as current_status_code,
        curr.status_category as current_status_category,
        curr.license_expiration_date as current_expiration_date,
        curr.address_line_1 as current_address_1,
        curr.city as current_city,
        curr.address_state as current_state,
        curr.zip_code as current_zip,
        curr.county as current_county,
        curr.specialty_code as current_specialty,

        -- Previous values
        prev.status_code as previous_status_code,
        prev.status_category as previous_status_category,
        prev.license_expiration_date as previous_expiration_date,
        prev.address_line_1 as previous_address_1,
        prev.city as previous_city,
        prev.address_state as previous_state,
        prev.zip_code as previous_zip,
        prev.county as previous_county,
        prev.specialty_code as previous_specialty,

        -- Certification current
        curr.has_nitrous_oxide_permit as current_nitrous,
        curr.has_anesthesia_level_1 as current_anes_1,
        curr.has_anesthesia_level_2 as current_anes_2,
        curr.has_anesthesia_level_3 as current_anes_3,
        curr.has_anesthesia_level_4 as current_anes_4,
        curr.has_sealant_certification as current_sealant,
        curr.has_nitrous_monitoring as current_nom,
        curr.has_local_infiltration_anesthesia as current_lia,

        -- Certification previous
        prev.has_nitrous_oxide_permit as previous_nitrous,
        prev.has_anesthesia_level_1 as previous_anes_1,
        prev.has_anesthesia_level_2 as previous_anes_2,
        prev.has_anesthesia_level_3 as previous_anes_3,
        prev.has_anesthesia_level_4 as previous_anes_4,
        prev.has_sealant_certification as previous_sealant,
        prev.has_nitrous_monitoring as previous_nom,
        prev.has_local_infiltration_anesthesia as previous_lia,

        -- Is this a new record (no previous version)?
        prev.professional_id is null as is_new_record

    from snapshot_changes curr
    left join snapshot_changes prev
        on curr.previous_record_id = prev.dbt_scd_id
    where curr.effective_from > (
        select coalesce(max(event_timestamp), '1900-01-01') from {{ this }}
    )
    {% if is_incremental() %}
    or curr.is_current = true
    {% endif %}
),

events_unpivoted as (
    -- Generate individual event records for each type of change

    -- NEW LICENSE EVENT
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'NEW_LICENSE'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'NEW_LICENSE' as event_type,
        'New licensee detected' as event_description,
        null as previous_value,
        current_status_category as current_value,
        'HIGH' as priority,
        'onboarding' as marketing_action
    from current_and_previous
    where is_new_record = true

    union all

    -- STATUS CHANGE: Active to Lapsed (Cancelled/Expired)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'STATUS_LAPSED'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'STATUS_LAPSED' as event_type,
        'License cancelled or expired' as event_description,
        previous_status_category as previous_value,
        current_status_category as current_value,
        'MEDIUM' as priority,
        'winback_or_suppress' as marketing_action
    from current_and_previous
    where is_new_record = false
      and previous_status_category = 'ACTIVE'
      and current_status_category = 'LAPSED'

    union all

    -- STATUS CHANGE: Lapsed to Active (Reinstated)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'STATUS_REINSTATED'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'STATUS_REINSTATED' as event_type,
        'License reinstated' as event_description,
        previous_status_category as previous_value,
        current_status_category as current_value,
        'HIGH' as priority,
        'reengagement' as marketing_action
    from current_and_previous
    where is_new_record = false
      and previous_status_category in ('LAPSED', 'SUSPENDED')
      and current_status_category = 'ACTIVE'

    union all

    -- STATUS CHANGE: Any to Deceased
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'STATUS_DECEASED'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'STATUS_DECEASED' as event_type,
        'Licensee deceased - remove from all lists' as event_description,
        previous_status_category as previous_value,
        current_status_category as current_value,
        'CRITICAL' as priority,
        'suppress_all' as marketing_action
    from current_and_previous
    where is_new_record = false
      and previous_status_category != 'DECEASED'
      and current_status_category = 'DECEASED'

    union all

    -- ADDRESS CHANGE
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'ADDRESS_CHANGE'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'ADDRESS_CHANGE' as event_type,
        'Practice address changed' as event_description,
        concat(coalesce(previous_city, ''), ', ', coalesce(previous_state, '')) as previous_value,
        concat(coalesce(current_city, ''), ', ', coalesce(current_state, '')) as current_value,
        'LOW' as priority,
        'update_crm_territory' as marketing_action
    from current_and_previous
    where is_new_record = false
      and (
          coalesce(previous_address_1, '') != coalesce(current_address_1, '')
          or coalesce(previous_city, '') != coalesce(current_city, '')
          or coalesce(previous_zip, '') != coalesce(current_zip, '')
      )

    union all

    -- COUNTY CHANGE (territory reassignment)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'TERRITORY_CHANGE'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'TERRITORY_CHANGE' as event_type,
        'County/territory changed' as event_description,
        previous_county as previous_value,
        current_county as current_value,
        'MEDIUM' as priority,
        'territory_reassignment' as marketing_action
    from current_and_previous
    where is_new_record = false
      and coalesce(previous_county, '') != coalesce(current_county, '')
      and current_county is not null

    union all

    -- NEW CERTIFICATION: Nitrous Oxide
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'NEW_CERT_NITROUS'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'NEW_CERTIFICATION' as event_type,
        'Obtained nitrous oxide permit' as event_description,
        'No permit' as previous_value,
        'Nitrous oxide permit' as current_value,
        'HIGH' as priority,
        'upsell_sedation_products' as marketing_action
    from current_and_previous
    where is_new_record = false
      and previous_nitrous = false
      and current_nitrous = true

    union all

    -- NEW CERTIFICATION: Anesthesia Level 4 (highest)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'NEW_CERT_ANES_4'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'NEW_CERTIFICATION' as event_type,
        'Obtained anesthesia level 4 permit' as event_description,
        'No level 4 permit' as previous_value,
        'Anesthesia level 4 permit' as current_value,
        'HIGH' as priority,
        'upsell_advanced_sedation' as marketing_action
    from current_and_previous
    where is_new_record = false
      and previous_anes_4 = false
      and current_anes_4 = true

    union all

    -- NEW CERTIFICATION: Sealant (Hygienist)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'NEW_CERT_SEALANT'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'NEW_CERTIFICATION' as event_type,
        'Obtained sealant certification' as event_description,
        'No certification' as previous_value,
        'Sealant certification' as current_value,
        'MEDIUM' as priority,
        'upsell_preventive_products' as marketing_action
    from current_and_previous
    where is_new_record = false
      and professional_type = 'hygienist'
      and previous_sealant = false
      and current_sealant = true

    union all

    -- EXPIRATION APPROACHING (within 90 days)
    select
        {{ dbt_utils.generate_surrogate_key(['professional_id', 'event_timestamp', "'EXPIRATION_APPROACHING'"]) }} as event_id,
        professional_id,
        state_code,
        professional_type,
        license_number,
        first_name,
        last_name,
        event_timestamp,
        'EXPIRATION_APPROACHING' as event_type,
        'License expiring within 90 days' as event_description,
        cast(previous_expiration_date as varchar) as previous_value,
        cast(current_expiration_date as varchar) as current_value,
        'MEDIUM' as priority,
        'renewal_reminder' as marketing_action
    from current_and_previous
    where current_status_category = 'ACTIVE'
      and current_expiration_date between current_date() and dateadd(day, 90, current_date())
      and coalesce(previous_expiration_date, '1900-01-01') != current_expiration_date
)

select
    event_id,
    professional_id,
    state_code,
    professional_type,
    license_number,
    first_name,
    last_name,
    event_timestamp,
    event_type,
    event_description,
    previous_value,
    current_value,
    priority,
    marketing_action,
    current_timestamp() as created_at,
    false as is_processed,
    null as processed_at,
    null as downstream_system
from events_unpivoted
