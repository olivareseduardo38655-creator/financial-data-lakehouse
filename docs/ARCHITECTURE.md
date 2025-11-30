# Architecture: Financial Data Lakehouse

## 1. Data Flow (Medallion Architecture)

Data transitions from raw ingestion to analytical consumption following standard industry practices.

\\\mermaid
graph LR
    %% Style Definitions
    classDef bronze fill:#fff,stroke:#333,stroke-width:1px;
    classDef silver fill:#e0e0e0,stroke:#333,stroke-width:1px;
    classDef gold fill:#c0c0c0,stroke:#333,stroke-width:1px;
    classDef source fill:#f9f9f9,stroke:#333,stroke-width:1px,stroke-dasharray: 5 5;

    %% Nodes
    SRC[External Sources] -->|Raw Ingestion| B(Bronze Layer)
    
    subgraph Lakehouse
        B -->|Cleaning & Deduplication| S(Silver Layer)
        S -->|Aggregation & KPIs| G(Gold Layer)
    end

    G -->|Consumption| BI[BI Dashboards]
    G -->|Consumption| ML[Risk Models]

    %% Class Assignment
    class B bronze;
    class S silver;
    class G gold;
    class SRC source;
\\\

---

## 2. Entity Relationship Diagram (Conceptual Model)

Simplified banking system schema.

* **Customers:** Master customer data (KYC).
* **Accounts:** Contracted financial products.
* **Transactions:** Movement ledger.

\\\mermaid
erDiagram
    CUSTOMERS ||--o{ ACCOUNTS : "holds"
    ACCOUNTS ||--o{ TRANSACTIONS : "performs"
    
    CUSTOMERS {
        string customer_id PK
        string email
        string first_name
        string last_name
        date created_at
    }

    ACCOUNTS {
        string account_id PK
        string customer_id FK
        string account_type "Savings/Credit"
        double current_balance
    }

    TRANSACTIONS {
        string transaction_id PK
        string account_id FK
        double amount
        string transaction_type "Debit/Credit"
        timestamp transaction_date
    }
\\\

## 3. Layer Strategy

| Layer | Format | Write Strategy | Objective |
|-------|--------|----------------|-----------|
| **Bronze** | Delta | Append Only | Full history, immutable, source of truth. |
| **Silver** | Delta | Merge (Upsert) | Clean data, deduplicated, schema enforcement. |
| **Gold** | Delta | Overwrite / Merge | Fact and dimension tables ready for reporting. |
