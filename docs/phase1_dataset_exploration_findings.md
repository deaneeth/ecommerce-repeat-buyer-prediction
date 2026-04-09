# Phase 1: Dataset Exploration and Understanding (P1-T1, P1-T2)

## Scope
This document captures the Section 1 findings from the notebook workflow in src/Main_Analysis.py for:
- P1-T1: Load and inspect the full dataset
- P1-T2: Document industry context and business problem

Data source path used:
- /Volumes/workspace/default/cosmetics_data

Months loaded:
- 2019-Oct.csv
- 2019-Nov.csv
- 2019-Dec.csv
- 2020-Jan.csv
- 2020-Feb.csv

---

## Industry Context (E-Commerce Cosmetics and Beauty Retail)
- The cosmetics e-commerce industry is a large and fast-growing global market.
- Competition is high and customer switching costs are low, so retention is critical.
- Customer acquisition is expensive, which increases the value of conversion optimization and repeat purchasing.
- Behavioral analytics from clickstream events can improve conversion outcomes and marketing efficiency.

## Business Problem
Predict customer purchase conversion in an online cosmetics store.

Primary question:
- Given a browsing session with views and cart actions, can we predict whether the session ends with a purchase?

Secondary questions:
- What categories and brands attract the most customer activity?
- What temporal patterns appear in purchasing behavior?
- Can customers be segmented into meaningful behavioral groups?

---

## How Data Analytics Supports the Business
- Descriptive analytics:
  - Profile event flows, engagement patterns, missing data, and price distributions.
- Predictive analytics:
  - Build conversion models to identify sessions likely to purchase.
- Prescriptive analytics:
  - Recommend pricing, campaign timing, product placement, and personalization actions.

Organizational value:
- Reduce cart abandonment.
- Improve marketing ROI through better targeting.
- Optimize the product catalog using observed demand behavior.
- Improve customer lifetime value through personalized interventions.

---

## Observed Results from Current Run

### Row Volumes
- 2019-Oct: 4,102,283
- 2019-Nov: 4,635,837
- 2019-Dec: 3,533,286
- 2020-Jan: 4,264,752
- 2020-Feb: 4,156,682
- Total rows: 20,692,840

### Schema Validation
- event_time: timestamp
- event_type: string
- product_id: integer
- category_id: long
- category_code: string
- brand: string
- price: double
- user_id: integer
- user_session: string

Note:
- event_time was correctly recognized as timestamp in this run.

### Event Type Distribution
- view: 9,657,821 (46.67%)
- cart: 5,768,333 (27.88%)
- remove_from_cart: 3,979,679 (19.23%)
- purchase: 1,287,007 (6.22%)

### Missing Values
- category_code: 20,339,246 (98.29%)
- brand: 8,757,117 (42.32%)
- user_session: 4,598 (0.02%)
- other columns: 0

### Data Quality Flags
- Minimum price is -79.37, indicating invalid or reversal-like values that should be handled in Step 3 (cleaning).

---

## Comparison Against Prompt Benchmarks
The notebook execution is technically correct, but the observed data profile differs from the benchmark expectations in the task prompt:
- Expected view dominance (~85-90%) is not observed; actual is 46.67%.
- Expected category_code nulls (~30-40%) are not observed; actual is 98.29%.

Interpretation:
- Use observed dataset metrics as the authoritative baseline for downstream analysis.
- Treat benchmark values in the prompt as generic examples, not strict truth for this specific uploaded dataset.

---

## Gate Check Status
- [x] DataFrame loads without errors
- [x] df.count() returns millions of rows (15M+)
- [x] Schema shows expected core types (including timestamp event_time)
- [x] 4 distinct event types visible
- [x] Null analysis complete with column-level counts

---

## Recommended Next Steps (Step 3 Preparation)
1. Cast product_id and user_id to long if strict type consistency is required.
2. Define handling rules for negative prices.
3. Decide strategy for very high category_code missingness.
4. Build session-level features for conversion modeling.

---

## Phase 1 Complete Verification

- [x] Full dataset loaded into Spark DataFrame
- [x] Row count, schema, event type distribution, and null analysis all documented
- [x] Industry context and business problem clearly defined
- [x] All code cells show visible outputs in the notebook
