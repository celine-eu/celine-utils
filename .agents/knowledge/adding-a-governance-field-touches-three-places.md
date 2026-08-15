# A governance field added in one place reads as permanently absent

Verified against the code on 2026-08-15.

## Three places, and the third is the one that is forgotten

Adding a field to the `governance.yaml` grammar means editing **all three**:

1. `schema/governance.schema.json` — so a file declaring it validates.
2. `GovernanceRule` (or a sub-model) in `src/celine/governance/models.py`.
3. **`KNOWN_KEYS`** in the same module.

`KNOWN_KEYS` is the frozenset `parse_rule` uses to split a raw block: keys inside it
become model fields, keys outside it are collected into `rule.extra`.

Omit step 3 and the field parses into `extra`, so `rule.<field>` reads as its default
forever — **silently**, with the schema still validating the file and the model still
constructing without error. Nothing warns, because from the parser's point of view
the file simply did not mention the field.

That is exactly how the `ontology` block failed on introduction.

Omit step 1 instead and the field still works — `governanceBlock` permits additional
properties — but `validate` reports it as an unknown key, and the schema stops
describing the format.

## Never build a `GovernanceRule` with keyword arguments

If the rule will be merged, it must be built through `model_validate` — which is what
`parse_rule` does.

Pydantic records the keys a document actually declared in `model_fields_set`, and the
entire merge layer (`celine.governance.merge`) reads it to distinguish *unset* from
*set to a falsy value*. Constructing with kwargs marks **every** field as set, which
degrades merging to "override always wins" and makes `expose: false` inexpressible.

This is silent too. The rule is valid, the fields are right, and only a merge
involving it behaves wrongly — which is how the same class of bug reached production
once already, as `dataspace.expose: false` in an overlay that failed to withdraw a
dataset.

Practical consequences:

- Build rules with `parse_rule(block)` or `GovernanceRule.model_validate(payload)`.
- A merge that produces a new model must validate the merged **dict**, not construct
  from attributes, so the set carries forward through a chain of overlays.
- A test that constructs a rule with kwargs and asserts a merge result is testing
  something other than what production does.

## Related

- `governance-is-a-thin-core.md` — why this package exists and what it may import.
- `docs/governance.md` — the field-by-field grammar, for readers rather than editors.
