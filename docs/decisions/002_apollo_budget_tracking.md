# ADR-002: Apollo Credit Budget Tracking

**Date:** 2025-12-23
**Status:** Accepted
**Commits:** (pending - current session)

## Context

Apollo.io charges per-credit for enrichment:
- Email lookup: 1 credit (includes company, LinkedIn, location free)
- Phone reveal: +5 credits
- User has 2500 credits/month allocation

Need guardrails to prevent accidental overspend.

## Decision

Implement local budget tracking with automatic stop:

1. Track usage in `~/.apollo_usage.json` by billing cycle (YYYY-MM)
2. Default monthly budget: 2500 credits
3. Auto-stop when budget exhausted
4. Support `--max-credits N` to limit individual runs
5. Email-only by default (1 credit), phone opt-in

## Alternatives Considered

1. **No tracking, rely on Apollo dashboard**
   - Pros: Simpler
   - Cons: No pre-flight check, could burn credits before noticing

2. **Hard-coded limit per run**
   - Pros: Safe
   - Cons: Inflexible, doesn't track cumulative usage

3. **Local tracking with monthly rollover (chosen)**
   - Pros: Full visibility, auto-stop, resume-friendly
   - Cons: Could drift from Apollo's actual count

## Consequences

- Positive: Can't accidentally burn entire month's budget
- Positive: `--usage` flag shows remaining budget instantly
- Positive: Resume support - interrupted runs continue where left off
- Negative: Local tracking may drift from Apollo's actual count
- Mitigation: Periodic reconciliation with Apollo dashboard

## Implementation Notes

Key files:
- `enrichment/apollo_enrich.py` - CLI with budget tracking
- `~/.apollo_usage.json` - Usage data (not in repo)

Usage:
```bash
python3 enrichment/apollo_enrich.py --usage          # Check budget
python3 enrichment/apollo_enrich.py --max-credits 100  # Limit run
python3 enrichment/apollo_enrich.py --dry-run        # Preview, 0 credits
```

## Session Reference

Claude Code session: `data-pipeline-enrichment-setup`
Key discussion points:
- User asked about throttling credit usage
- Phone credits are 5-6x email cost
- Email-only default to optimize spend
- 631 TX dentists × ~50% match rate = ~315 credits estimated
