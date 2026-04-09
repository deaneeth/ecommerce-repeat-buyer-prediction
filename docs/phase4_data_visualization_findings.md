# Phase 4: Data Visualization Findings (P4-T1)

## Scope

This document captures final Step 4 execution findings from Section 3 of src/Main_Analysis.py:

- P4-T1: Five visualizations for business storytelling

This final report is based on:

- Phase 1 findings (dataset profile and null patterns)
- Phase 2 findings (NoSQL validation context)
- Phase 3 findings (cleaning and descriptive analytics baselines)
- Final rerun notebook outputs for all five Phase 4 charts after code fixes

---

## Data and Context Baseline

From earlier phases:

- Total rows loaded: 20,692,840
- Rows after cleaning: 20,588,552
- Purchase events: 1,286,880
- Total revenue: 6,351,830.29
- Known sparsity: category_code is highly missing in source and filled as unknown in cleaning

Implication for visualization quality:

- Revenue, event mix, and hourly charts are stable and representative.
- Category-level analysis is valid but must acknowledge strong category_code sparsity.

---

## Execution Environment Note

Observed warning during run:

- Databricks Connect 15.4.0 is flagged as unsupported for serverless; 15.1.x is recommended.

Assessment:

- This warning did not invalidate generated plots.
- It is a runtime stability risk for later heavy phases and should be normalized before final full-run export.

---

## Visualization Results and Assessment

### Visualization 1: Top 10 Brands by Revenue (Bar)

Observed outputs:

- Top brand: unknown at 2,564,006.60
- Top-3 revenue share: 49.97%
- Top-10 revenue share: 67.09%

Assessment:

- Correct and fully consistent with Phase 3 Analysis 4.
- Strong business signal: revenue is concentrated across a limited set of brands.
- Unknown dominance is expected from preserved brand missingness.

Verdict: Good and valid.

---

### Visualization 2: Monthly Revenue Trend (Line)

Observed outputs:

- Highest month: Nov 2019 at 1,531,508.97
- Lowest month: Dec 2019 at 1,078,164.53
- Trend shape: rise to Nov, drop in Dec, rebound in Jan, mild decline in Feb

Assessment:

- Correct and fully aligned with Phase 3 monthly revenue outputs.
- Chronological ordering is correct via year_month derivation.

Verdict: Good and valid.

---

### Visualization 3: Event Type Distribution (Pie)

Observed outputs:

- View share: 46.76%
- Purchase share: 6.25%
- Cart and remove_from_cart shares match Phase 3 distribution

Assessment:

- Correct and coherent with Phase 3 Analysis 1.
- Uses project-truth percentages rather than generic benchmark assumptions.

Verdict: Good and valid.

---

### Visualization 4: Purchase Activity by Hour (Bar)

Observed outputs:

- Peak purchase hour: 11:00 UTC with 85,595 purchases
- Top 3 purchase hours: 11:00, 12:00, 10:00

Assessment:

- Correct and aligned with Phase 3 hourly behavior.
- Operationally useful for campaign and notification timing.

Verdict: Good and valid.

---

### Visualization 5: Top Product Categories by Purchase Volume (Bar)

Final rerun outputs (after category derivation fix):

- stationery: 6,327
- apparel: 4,977
- appliances: 4,138
- furniture: 1,458
- accessories: 314
- Top category share (within shown categories): 36.75%

Assessment:

- Category labels are now correct top-level categories.
- The chart is now consistent with corrected main_category derivation logic.
- Category concentration is clear, but interpretation remains directional because category_code sparsity is high.

Verdict: Good and valid.

---

## Overall Phase 4 Verdict

### Are the generated plots good?

Yes. All five final plots are correct, readable, and analytically consistent.

### Are results good for this dataset?

Yes. Results are internally consistent with cleaned-data behavior and all relevant Phase 3 baselines.

### Are results what we expected?

Yes, for this specific dataset profile:

- Browse-heavy event mix with meaningful purchase share
- Strong brand revenue concentration
- Clear monthly and hourly demand patterns
- Category concentration visible after corrected top-level extraction

### Is anything wrong now?

No blocking analytical issues remain in Phase 4 outputs.

### Do we need any further adjustments?

No Phase 4 chart-logic adjustments are required.

Optional non-plot adjustment:

- Align Databricks Connect to 15.1.x for serverless compatibility stability in later phases.

---

## Gate Check Status (P4-T1)

- [x] Five visualizations rendered from real dataset aggregations
- [x] At least three chart types used (bar, line, pie)
- [x] Every chart has title and axis labels
- [x] Every chart has written interpretation
- [x] Visual story is coherent with prior-phase findings
- [x] Category chart corrected and validated with final rerun output

Gate decision:

- PASS. Phase 4 is complete and final under current scope.

---

## Final Action Before Export

1. Export notebook artifacts with the current rerun outputs.
2. Use this finalized Phase 4 findings file in the report evidence pack.
3. Proceed to Phase 5 implementation.

---

## Conclusion

Phase 4 is finalized and submission-ready. The five visualizations are now fully aligned with cleaned data, prior-phase analytical baselines, and business interpretation requirements. No further Phase 4 fixes are required.
