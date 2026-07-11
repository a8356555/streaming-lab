# ADR-000: Repo name

> Status: **PROPOSAL** — decision pending Alan
> Format: options → recommendation → rejected-why → reversal trigger

## Context

The lab was split out of `system-design/data-intensive-app`. It needs a name that
signals what it proves (distributed correctness / exactly-once), not a generic
"streaming" label.

## Options

| Name | For | Against |
|---|---|---|
| `streaming-lab` | short, neutral, already the working dir | vague — "streaming" says topic not thesis |
| `exactly-once-lab` | names the exact claim; searchable | slightly bold; invites "prove it" (which is the point) |
| `streaming-correctness-lab` | most descriptive | long |

## Recommendation (proposal only)

Conservative default kept so work can proceed: **`streaming-lab`**. The README
thesis and ADR-001 carry the "exactly-once" framing regardless of repo name, so
the name is low-stakes and reversible (GitHub redirects on rename).

## Decision

> TODO(Alan): pick the name. If renaming, do it before the first public writeup
> links to the repo. Record the reason here in your own words.

## Reversal trigger

Rename is cheap pre-publication; after the HN/Reddit writeup links land, treat the
URL as stable.
