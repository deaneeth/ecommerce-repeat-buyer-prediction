# Phase 3: Data Processing with Apache Spark (P3-T1, P3-T2)

## Scope
This document captures Step 3 execution results from Section 2 of src/Main_Analysis.py:
- P3-T1: Data Cleaning
- P3-T2: Descriptive Analytics (8 analyses)

Context source files used:
- src/Main_Analysis.py
- docs/phase1_dataset_exploration_findings.md
- docs/phase2_mongodb_nosql_findings.md
- docs/phase3-parcial.md

---

## Data Context at Phase 3 Start
- Dataset: eCommerce Events History in Cosmetics Shop
- Path: /Volumes/workspace/default/cosmetics_data
- Months: 2019-Oct to 2020-Feb
- Baseline rows from Phase 1: 20,692,840

Known data quality issues entering Phase 3:
- category_code nulls: 20,339,246 (~98.29%)
- brand nulls: 8,757,117 (~42.32%)
- Invalid negative prices observed in baseline profiling

---

## P3-T1 Execution Summary (Data Cleaning)

### Cleaning Rules Applied
1. Fill missing category_code with unknown.
2. Fill missing brand with unknown.
3. Drop rows where event_type is null.
4. Drop rows where event_time is null.
5. Drop rows where price is null or <= 0.
6. Derive features:
   - event_date
   - event_hour
   - event_dayofweek
   - event_month
   - main_category (from category_code prefix)

### Pre-Clean Null Profile (Observed)
- event_time: 0
- event_type: 0
- product_id: 0
- category_id: 0
- category_code: 20,339,246
- brand: 8,757,117
- price: 0
- user_id: 0
- user_session: 4,598

### Row Impact (Observed)
- Rows after cleaning: 20,588,552
- Rows removed: 104,288 (0.50%)
- Rows retained: 20,588,552 (99.50%)

Interpretation:
- The cleaning strategy is conservative and preserves almost all behavioral signal.
- Most removals were caused by invalid/non-positive price and required non-null integrity checks.

### Cache Behavior (Observed)
- Cache attempt failed with serverless restriction:
  - NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE is not supported on serverless compute.
- Fallback behavior executed correctly and processing continued.

Interpretation:
- This is an environment limitation, not a logic error.
- Rebuild/preflight patterns are required for later phases instead of relying on cache persistence.

---

## P3-T2 Execution Summary (Descriptive Analytics)

### Analysis 1: Event Type Distribution
Observed:
- view: 9,627,329 (46.76%)
- cart: 5,718,963 (27.78%)
- remove_from_cart: 3,955,380 (19.21%)
- purchase: 1,286,880 (6.25%)

Interpretation:
- Behavior remains browse-heavy, but purchase-event share is meaningful.
- Event share is higher than typical benchmark assumptions, which supports strong downstream revenue analysis.

### Analysis 2: Revenue Analysis
Observed total revenue:
- 6,351,830.29

Observed monthly revenue:
- 2019-10: 1,212,093.99 (245,604 purchases, avg 4.94)
- 2019-11: 1,531,508.97 (322,400 purchases, avg 4.75)
- 2019-12: 1,078,164.53 (213,158 purchases, avg 5.06)
- 2020-01: 1,322,694.22 (263,760 purchases, avg 5.01)
- 2020-02: 1,207,368.58 (241,958 purchases, avg 4.99)

Top revenue month:
- 2019-11

Interpretation:
- November peak is consistent with promotion/holiday period effects.
- Revenue remains substantial across all months, indicating steady demand.

### Analysis 3: Top Products (Views vs Purchases)
Observed:
- Top viewed products are dominated by brands grattol, jessnail, uno, runail.
- Top purchased products include grattol, runail, irisk, uno, plus unknown brand records.
- Top-10 overlap between viewed and purchased products: 5 products.

Interpretation:
- Only partial overlap indicates conversion friction for some highly viewed products.
- Product-level optimization opportunities exist in pricing, positioning, or checkout journey.

### Analysis 4: Top 10 Brands by Revenue
Observed top brands include:
- unknown: 2,564,006.60 revenue (549,566 purchases)
- runail: 343,433.19
- grattol: 266,295.94
- irisk: 223,903.38
- uno: 190,719.46

Observed:
- Top-10 brand revenue share: 67.09% of total revenue.

Interpretation:
- Revenue is concentrated in a limited set of brands.
- Unknown brand contribution is very high, reflecting upstream missing brand values and limiting brand-level precision.

### Analysis 5: Hourly Activity Patterns
Observed:
- Peak browsing hour (UTC): 19
- Peak purchase hour (UTC): 11
- Purchase rate by hour is stable, roughly in the ~5.5% to ~7.0% band.

Interpretation:
- Browsing and purchasing peaks differ, suggesting separate windows for awareness vs conversion campaigns.
- Time-based targeting can improve campaign efficiency.

### Analysis 6: Day-of-Week Patterns
Observed purchase-rate by day:
- Highest: Friday (6.76%)
- Next strongest: Thursday (6.61%), Tuesday (6.34%)
- Lower range: Saturday (5.90%), Sunday (5.95%)

Interpretation:
- Weekday periods show stronger conversion than weekend in this data.
- Campaign timing should prioritize high-intent weekdays.

### Analysis 7: Conversion Funnel (Session Level)
Observed:
- Total sessions: 4,520,284
- Sessions with views: 4,265,922 (94.37%)
- Sessions with cart: 983,953 (21.77%)
- Sessions with purchase: 155,617 (3.44%)
- View to cart: 23.07%
- Cart to purchase: 15.82%
- Cart abandonment: 84.18%

Interpretation:
- Session-level conversion (3.44%) is realistic for e-commerce and aligns with expected range.
- Cart abandonment is the most critical leakage point and should drive prescriptive actions.

### Analysis 8: Basket Value
Observed:
- Orders analyzed: 155,617
- Mean basket value: 40.82
- Median basket value: 29.25
- 90th percentile basket value: 79.38
- Average items per order: 8.27
- Max basket value: 1,738.10

Interpretation:
- Basket values show a strong long-tail with clear high-value outliers.
- Median vs mean gap suggests skew and supports segment-aware pricing/offer strategy.

---

## Roadmap Alignment and Gate Status

### P3-T1 Gate Check
- [x] Cleaning steps documented with reasons
- [x] Null handling strategy explained (fill vs drop)
- [x] Row count before and after cleaning printed
- [x] Derived columns created (event_date, event_hour, event_dayofweek, main_category)
- [x] Cache attempted and gracefully handled for serverless runtime

### P3-T2 Gate Check
- [x] All 8 analyses executed
- [x] Each analysis has code output and business interpretation
- [x] Numbers are internally consistent (views > carts > purchases)
- [x] Conversion funnel is realistic (3.44% session purchase rate)
- [x] Revenue figures are non-zero and coherent across months
- [x] Top brands/products identified and discussed

### Phase 3 Complete Verification (Based on Current Evidence)
- [x] Data cleaning documented with before/after row counts
- [x] 8+ distinct analytical tasks completed
- [x] Every analysis has a business interpretation
- [x] Section execution from preflight through Analysis 8 completed without analytical errors
- [~] Results are cached for subsequent phases

Note on the last item:
- Serverless does not support cache/persist in this environment.
- Use the preflight rebuild approach as the operational equivalent.

---

## Issues, Risks, and Unusual Observations
1. Databricks Connect warning observed:
   - 15.4.0 is flagged as unsupported with serverless.
   - Recommendation: align to 15.1.x to reduce instability risk.
2. Unknown brand share is materially large in revenue outputs.
   - This is expected given baseline missingness and fill strategy.
3. Category granularity is limited due very high category_code sparsity.
   - main_category remains valid but often unknown.
4. Cart abandonment is very high (84.18%).
   - This is business-significant and should be highlighted in report recommendations.

---

## Go/No-Go Decision for Phase 4
Decision: GO.

Proceeding conditions:
1. Keep the preflight cell pattern before heavy sections (already implemented).
2. For visualization, include both:
   - Full view (including unknown)
   - Filtered view (excluding unknown) for brand/category charts
3. Run one full top-to-bottom notebook execution before final export to prove reproducibility.

No blockers were found that require redesign of Phase 3 logic before starting Phase 4.
