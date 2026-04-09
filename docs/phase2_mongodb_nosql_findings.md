# Phase 2: MongoDB Atlas Validation and NoSQL Concepts (P2-T2, P2-T3)

## Scope
This document captures the Section 2 findings and report content for:
- P2-T2: Capture and verify MongoDB Atlas screenshots
- P2-T3: Document NoSQL concepts with project-specific examples

Evidence sources used:
- MongoDB Atlas UI screenshots captured for this phase
- Notebook workflow in src/MongoDB_Demo.py

---

## MongoDB Atlas Evidence (P2-T2)

### Verified Cluster Context
- Organization: University of Plymouth
- Project: Big-Data_Project
- Cluster: EcommerceRepeatBuyer-Cosmetics
- Cluster status: Active (green indicator)
- MongoDB version: 8.0.20
- Region: AWS Mumbai (ap-south-1)

### Verified Database and Collection Context
- Database name: cosmetics_ecommerce
- Collection name: events
- Document count shown in Atlas UI: 5000 (5K)

### Screenshot Validation Summary (4 Total)
1. Cluster overview screenshot confirms cluster identity and active status.
2. Database browser screenshot confirms cosmetics_ecommerce.events path.
3. Sample document screenshot confirms JSON-style document structure.
4. Collection-level view confirms 5000 documents in the events collection.

---

## NoSQL Concepts (P2-T3 Report Content)
MongoDB highlights key NoSQL benefits for this project because the dataset is event-based and semi-structured. In our implementation, records are stored as JSON-like documents in the cosmetics_ecommerce.events collection. This supports schema flexibility: documents do not require a rigid table definition, and optional fields can be missing or null without blocking ingestion. A concrete project example is category_code, which appears as null in many records while fields such as event_time, event_type, product_id, user_id, and user_session remain usable for analysis. In a relational model, frequent structure variation like this usually requires schema migrations, sparse tables, or additional normalization effort.

NoSQL also aligns well with big data characteristics in e-commerce clickstream analysis. We process high-volume behavior events and primarily run filter-and-aggregate workloads. For Atlas demonstration, a 5000-document sample was inserted and queried in the notebook, including connection checks, insert operations, brand-based retrieval, and aggregation pipelines (for example, top brands by purchase count and average price). These operations map naturally to document collections and reduce dependence on costly multi-table JOIN patterns for common analytics tasks.

Scalability is another major advantage. MongoDB Atlas provides managed distributed infrastructure, replica sets for high availability, and horizontal scale-out through sharding when data volume grows. Compared with relational databases, which are strongest for strict fixed-schema ACID workflows and complex joins, MongoDB is better suited here because the workload is write-heavy, semi-structured, and continuously evolving. For this project context, NoSQL offers better flexibility, operational simplicity, and scale-readiness.

---

## Gate Check Status

### P2-T2 Gate Check
- [x] 4 screenshots saved and clearly readable
- [x] Screenshots show correct database name, collection name, and document count
- [x] At least one screenshot shows the JSON document structure

### P2-T3 Gate Check
- [x] Schema flexibility explained with reference to the dataset (null category_code)
- [x] Why NoSQL suits big data explained with project context
- [x] Scalability benefits mentioned
- [x] At least one comparison point with relational databases included

---

## Phase 2 Complete Verification
- [x] MongoDB Atlas has 5000 documents in cosmetics_ecommerce.events
- [x] Connection, insertion, query, and aggregation demonstrated in notebook
- [x] 4 screenshots captured for report evidence
- [x] NoSQL concepts documented with project-specific examples
