# Architecture Decision Records

## ADR-001: Use PostgreSQL for Data Warehouse

**Date**: 2024-01-01  
**Status**: Accepted

### Context
Need to choose a database for storing time-series exchange rate data.

### Decision
Use PostgreSQL with TimescaleDB extension for time-series optimization.

### Rationale
- Mature, stable, widely supported
- Excellent Python ecosystem
- Native JSON support
- TimescaleDB provides compression and optimization for time-series

### Alternatives Considered
- ClickHouse: Specialized OLAP, but overkill for current volume
- MongoDB: Not ideal for analytical queries on structured data
- SQLite: Not suitable for production multi-user access

---

## ADR-002: Apache Airflow for Orchestration

**Date**: 2024-01-02  
**Status**: Accepted

### Context
Need workflow orchestration for ETL pipeline scheduling and monitoring.

### Decision
Use Apache Airflow for pipeline orchestration.

### Rationale
- Industry standard for data workflows
- Extensive monitoring and alerting
- Dynamic workflow definition via Python
- Large community and ecosystem

---

## ADR-003: dbt for Data Transformation

**Date**: 2024-01-03  
**Status**: Accepted

### Context
Need to transform raw data into analytics-ready tables with version control.

### Decision
Use dbt for data transformation and modeling.

### Rationale
- SQL-based transformations (easy to review and version)
- Built-in lineage and documentation
- Testing framework
- Supports multiple databases

---

## ADR-004: Python for Analytics and Scripting

**Date**: 2024-01-04  
**Status**: Accepted

### Context
Multiple analytical and ML tasks require flexibility and rich ecosystem.

### Decision
Use Python with libraries like Pandas, Scikit-learn, Prophet.

### Rationale
- Rich ML ecosystem
- Data manipulation capabilities
- Jupyter for exploration
- Integration with Airflow easy via Python operators
