# Business Context — Repeat Buyer Retention

## The Business Problem

E-commerce platforms lose significant marketing spend by targeting users who will not return regardless of intervention. Broad, untargeted retention campaigns produce low conversion rates and poor return on investment.

This project solves a specific, high-value problem: **which customers who purchased in Oct–Dec 2019 will buy again in Jan–Feb 2020?** Identifying these users in advance allows the marketing team to concentrate budget where it generates measurable revenue.

---

## Why Repeat Buyer Retention Matters

- Repeat buyers cost 5–7× less to convert than new customers (Harvard Business Review).
- A customer who has already purchased is a proven revenue source — they have brand familiarity, payment credentials on file, and demonstrated willingness to spend.
- Identifying the highest-probability returners before a campaign removes guesswork and reduces wasted spend.

---

## The Two-Stage Funnel Approach

Attempting to score all 72,225 Oct–Dec buyers directly produces a base rate of 15.5% — too low to reach the required Precision ≥ 0.75 with any practical model.

The solution applies a two-stage funnel:

1. **Stage 1 — Cohort filter (business logic):** Isolate the 12,972 users who purchased on two or more distinct calendar dates. These are habitual buyers, not one-time visitors. This cohort has a 40.8% natural return rate.
2. **Stage 2 — ML ranking (GBT model):** Within the high-value cohort, a Gradient Boosted Tree model ranks users by return probability. Only the top-ranked users are targeted.

This approach separates predictable returners from users who need a promotional nudge, so the marketing team receives a ranked, actionable list rather than a large undifferentiated audience.

---

## Validated Model Performance

The Gradient Boosted Tree model achieved the following on the held-out test set:

| Metric | Result |
| --- | ---: |
| Precision | **0.7988** |
| Recall | **0.8329** |
| F1 Score | **0.8155** |
| AUC-ROC | **0.9177** |
| AUC-PR | **0.8901** |

Both targets (Precision ≥ 0.75 and F1 ≥ 0.75) were met.

An AUC-ROC of **0.9177** means the model correctly ranks a true returning customer above a non-returning customer approximately 91.8% of the time — strong discrimination on a near-balanced class distribution.

---

## Campaign Impact

Applying the model to the full Oct–Dec habitual buyer cohort (12,972 users):

| Campaign Metric | Value |
| --- | ---: |
| Users targeted by model | ~5,426 |
| True repeat buyers reached | ~4,334 (80% of targeted) |
| Lift over random targeting | **1.96×** |
| Random campaign yield (same budget) | ~2,211 returners |
| Additional conversions gained | ~2,123 |

With the same campaign budget, the GBT-targeted approach delivers **1.96× more retained customers** than a random outreach campaign. This translates directly to higher revenue per marketing dollar spent.

---

## Precision@K — Targeting Tiers

Different campaign objectives can use different targeting thresholds:

| Campaign Tier | Users Targeted | Precision | Lift |
| --- | ---: | ---: | ---: |
| Top 5% (VIP / high-value only) | 102 | 97.1% | 2.42× |
| Top 10% (high-confidence) | 204 | 95.6% | 2.38× |
| Top 20% (retention campaign) | 408 | 93.6% | 2.33× |
| **Top 30% (recommended)** | **613** | **90.5%** | **2.26×** |
| Top 40% (cost-effective broad) | 817 | 81.5% | 2.03× |
| Top 50% (maximum reach) | 1,022 | 71.6% | 1.79× |

**Recommendation:** Target the top 30% at threshold 0.666 — 91% precision with strong coverage and optimal budget efficiency.

---

## RFM Segment Actions

| Segment | Users | Return Rate | Recommended Action |
| --- | ---: | ---: | --- |
| Champions | 2,035 | 66.4% | VIP loyalty programs, early product access |
| Loyal Customers | 3,577 | 47.5% | Targeted re-engagement, bundle offers |
| Potential Loyalists | 1,082 | 37.9% | Engagement campaigns, personalised recommendations |
| Needs Attention | 1,254 | 35.2% | Time-limited discount offers |
| Recent Customers | 1,008 | 32.1% | Onboarding sequences, category discovery |
| Lost Customers | 4,016 | 26.4% | Win-back campaigns or deprioritise |

---

## Strategic Summary

- **Stop spending on all 72K users.** 60K of them are low-signal and would not convert regardless of campaign type.
- **Deploy the GBT model on the 12,972 habitual buyer cohort** to generate a ranked return-probability list.
- **Target the top 30%** for the highest combination of precision and reach.
- **Use Champions separately** for VIP and loyalty initiatives — their 66.4% return rate makes them the highest-ROI segment for premium treatment.

Same budget. 1.96× more retained customers. Measurable, model-driven ROI.
