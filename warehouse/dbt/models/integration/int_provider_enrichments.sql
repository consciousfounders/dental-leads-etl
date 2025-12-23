{{
    config(
        materialized='incremental',
        unique_key='provider_id',
        schema='integration'
    )
}}

/*
    Provider Enrichments Layer

    Aggregates enrichment data from multiple sources:
    - NPI (baseline contact)
    - Apollo (B2B enrichment - email, company, title)
    - Wiza (LinkedIn-based enrichment)
    - Custom scrapers (practice websites, reviews, etc.)

    Priority for each field type (highest to lowest):
    - Email: Apollo > Wiza > Scraped > NPI
    - Phone: NPI > Apollo > Wiza > Scraped
    - LinkedIn: Wiza > Apollo
    - Practice website: Scraped > Apollo
    - Company info: Apollo > Wiza > Scraped

    This model:
    1. Collects all enrichments per provider
    2. Applies priority waterfall
    3. Tracks source provenance for each field
*/

-- Enrichment source priority (lower = higher priority)
{% set email_priority = {'apollo': 1, 'wiza': 2, 'scraped': 3, 'npi': 4} %}
{% set phone_priority = {'npi': 1, 'apollo': 2, 'wiza': 3, 'scraped': 4} %}

WITH base_providers AS (
    -- Start with license-based provider IDs
    SELECT
        COALESCE(npi_number, state_code || '-' || license_number) AS provider_id,
        npi_number,
        license_state,
        license_number,
        first_name,
        last_name,
        city,
        state
    FROM {{ ref('int_npi_license_matches') }}
),

-- NPI enrichments
npi_enrichments AS (
    SELECT
        npi AS provider_id,
        'npi' AS source,
        provider_business_practice_location_address_telephone_number AS phone,
        NULL AS email,  -- NPI doesn't have email
        NULL AS linkedin_url,
        NULL AS website,
        provider_organization_name_legal_business_name AS company_name,
        NULL AS title,
        NULL AS employee_count,
        last_update_date AS enriched_at
    FROM {{ source('raw_npi', 'npi_providers') }}
    WHERE entity_type_code = '1'
),

-- Apollo enrichments (B2B data)
apollo_enrichments AS (
    SELECT
        provider_id,
        'apollo' AS source,
        phone,
        email,
        linkedin_url,
        website_url AS website,
        company_name,
        title,
        employee_count,
        enriched_at
    FROM {{ source('raw_enrichments', 'apollo_enrichments') }}
    WHERE email IS NOT NULL  -- Only use if we got an email
),

-- Wiza enrichments (LinkedIn-based)
wiza_enrichments AS (
    SELECT
        provider_id,
        'wiza' AS source,
        phone,
        email,
        linkedin_url,
        company_website AS website,
        company_name,
        title,
        NULL AS employee_count,
        enriched_at
    FROM {{ source('raw_enrichments', 'wiza_enrichments') }}
),

-- Custom scraper enrichments
scraped_enrichments AS (
    SELECT
        provider_id,
        'scraped' AS source,
        phone,
        email,
        NULL AS linkedin_url,
        website,
        practice_name AS company_name,
        NULL AS title,
        NULL AS employee_count,
        scraped_at AS enriched_at
    FROM {{ source('raw_enrichments', 'scraped_data') }}
),

-- Stack all enrichments
all_enrichments AS (
    SELECT * FROM npi_enrichments
    UNION ALL
    SELECT * FROM apollo_enrichments
    UNION ALL
    SELECT * FROM wiza_enrichments
    UNION ALL
    SELECT * FROM scraped_enrichments
),

-- Apply priority waterfall for each field
enrichment_ranked AS (
    SELECT
        p.provider_id,

        -- Email (Apollo > Wiza > Scraped > NPI)
        FIRST_VALUE(e.email IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source
                WHEN 'apollo' THEN 1
                WHEN 'wiza' THEN 2
                WHEN 'scraped' THEN 3
                WHEN 'npi' THEN 4
                ELSE 99
            END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS email,

        FIRST_VALUE(CASE WHEN e.email IS NOT NULL THEN e.source END IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source
                WHEN 'apollo' THEN 1 WHEN 'wiza' THEN 2 WHEN 'scraped' THEN 3 WHEN 'npi' THEN 4 ELSE 99
            END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS email_source,

        -- Phone (NPI > Apollo > Wiza > Scraped)
        FIRST_VALUE(e.phone IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source
                WHEN 'npi' THEN 1
                WHEN 'apollo' THEN 2
                WHEN 'wiza' THEN 3
                WHEN 'scraped' THEN 4
                ELSE 99
            END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS phone,

        FIRST_VALUE(CASE WHEN e.phone IS NOT NULL THEN e.source END IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source
                WHEN 'npi' THEN 1 WHEN 'apollo' THEN 2 WHEN 'wiza' THEN 3 WHEN 'scraped' THEN 4 ELSE 99
            END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS phone_source,

        -- LinkedIn (Wiza > Apollo)
        FIRST_VALUE(e.linkedin_url IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'wiza' THEN 1 WHEN 'apollo' THEN 2 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS linkedin_url,

        FIRST_VALUE(CASE WHEN e.linkedin_url IS NOT NULL THEN e.source END IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'wiza' THEN 1 WHEN 'apollo' THEN 2 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS linkedin_source,

        -- Website (Scraped > Apollo > Wiza)
        FIRST_VALUE(e.website IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'scraped' THEN 1 WHEN 'apollo' THEN 2 WHEN 'wiza' THEN 3 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS website,

        FIRST_VALUE(CASE WHEN e.website IS NOT NULL THEN e.source END IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'scraped' THEN 1 WHEN 'apollo' THEN 2 WHEN 'wiza' THEN 3 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS website_source,

        -- Company name (Apollo > Wiza > Scraped > NPI)
        FIRST_VALUE(e.company_name IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'apollo' THEN 1 WHEN 'wiza' THEN 2 WHEN 'scraped' THEN 3 WHEN 'npi' THEN 4 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS company_name,

        FIRST_VALUE(CASE WHEN e.company_name IS NOT NULL THEN e.source END IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'apollo' THEN 1 WHEN 'wiza' THEN 2 WHEN 'scraped' THEN 3 WHEN 'npi' THEN 4 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS company_source,

        -- Title (Apollo > Wiza)
        FIRST_VALUE(e.title IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'apollo' THEN 1 WHEN 'wiza' THEN 2 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS title,

        -- Employee count (Apollo only typically)
        FIRST_VALUE(e.employee_count IGNORE NULLS) OVER (
            PARTITION BY p.provider_id
            ORDER BY CASE e.source WHEN 'apollo' THEN 1 ELSE 99 END
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS employee_count,

        -- Enrichment metadata
        MAX(e.enriched_at) OVER (PARTITION BY p.provider_id) AS last_enriched_at,

        ROW_NUMBER() OVER (PARTITION BY p.provider_id ORDER BY e.enriched_at DESC) AS rn

    FROM base_providers p
    LEFT JOIN all_enrichments e ON p.provider_id = e.provider_id
)

SELECT
    provider_id,

    -- Contact info with provenance
    email,
    email_source,
    phone,
    phone_source,
    linkedin_url,
    linkedin_source,
    website,
    website_source,

    -- Company info
    company_name,
    company_source,
    title,
    employee_count,

    -- Coverage flags
    email IS NOT NULL AS has_email,
    phone IS NOT NULL AS has_phone,
    linkedin_url IS NOT NULL AS has_linkedin,
    website IS NOT NULL AS has_website,

    -- Quality score (0-100)
    (
        CASE WHEN email IS NOT NULL THEN 40 ELSE 0 END +
        CASE WHEN phone IS NOT NULL THEN 20 ELSE 0 END +
        CASE WHEN linkedin_url IS NOT NULL THEN 15 ELSE 0 END +
        CASE WHEN website IS NOT NULL THEN 15 ELSE 0 END +
        CASE WHEN company_name IS NOT NULL THEN 10 ELSE 0 END
    ) AS enrichment_score,

    -- Timestamps
    last_enriched_at,
    CURRENT_TIMESTAMP() AS _updated_at

FROM enrichment_ranked
WHERE rn = 1
