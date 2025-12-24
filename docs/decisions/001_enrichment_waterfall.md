# ADR-001: Enrichment Data Waterfall Priority

**Date:** 2025-12-23
**Status:** Accepted
**Commits:** (pending - current session)

## Context

We have multiple data sources for provider contact information:
- State license files (TX, etc.)
- NPI registry (NPPES)
- Apollo.io (B2B enrichment)
- Wiza (LinkedIn-based)
- Custom web scrapers

Each source has different strengths. Need a deterministic priority for which source "wins" for each field type.

## Decision

Implement field-specific waterfall priority:

| Field | Priority (highest → lowest) |
|-------|---------------------------|
| Email | Apollo > Wiza > Scraped |
| Phone | NPI > Apollo > Wiza > Scraped |
| LinkedIn | Wiza > Apollo |
| Website | Scraped > Apollo > Wiza |
| Company | Apollo > Wiza > Scraped > NPI |
| Address | NPI > License (NPI = practice, License = may be home) |

## Alternatives Considered

1. **Single source of truth** - Pick one enrichment provider
   - Pros: Simpler
   - Cons: Leaves money on table, each source has gaps

2. **Most recent wins** - Latest data overwrites
   - Pros: Fresh data
   - Cons: Could overwrite verified data with unverified

3. **Field-specific priority (chosen)**
   - Pros: Leverages strengths of each source
   - Cons: More complex logic

## Consequences

- Positive: Maximizes data coverage, leverages best source per field
- Positive: Tracks provenance (`*_source` fields) for audit
- Negative: Requires maintaining priority logic in dbt models
- Risk: Priority may need adjustment as we learn source quality

## Implementation Notes

Key files:
- `warehouse/dbt/models/integration/int_provider_enrichments.sql` - Waterfall logic
- `warehouse/dbt/models/integration/int_provider_golden.sql` - Final merged record
- `warehouse/ddl/enrichment_sources.sql` - Source tables

## Session Reference

Claude Code session: `data-pipeline-enrichment-setup`
Key discussion points:
- User has Apollo (2500 credits/month), Wiza, custom scrapers
- NPI is free but lacks email
- Apollo email quality > Wiza for B2B
- NPI phone preferred (official practice number vs personal)
