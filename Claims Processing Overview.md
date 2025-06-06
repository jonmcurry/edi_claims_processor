Project Overview
This project builds a high-performance claims processing system with an integrated machine learning (ML) model for filter prediction. The system:
    Fetches claims from a PostgreSQL staging database.
    Applies a rules engine and ML-based filter prediction to assign filter numbers.
    Stores processing metrics in a PostgreSQL metrics database.
    Transfers validated claims to a SQL Server production database with facility management.
    Achieves a processing throughput of at least 100,000 records in 15 seconds (~6,667 records/second).
    Incorporates caching, asynchronous processing, connection pooling, bulk batch processing, and ML model optimization for performance, speed, and production-readiness.
    Targets 100% accuracy in filter prediction where feasible, with fallback to rule-based validation.
    Leverages config.yaml for configuration
    Use postgresql_create_edi_databases.sql as the framework for staging claims
    Use sqlserver_create_results_database.sql as the framework for storing results
    
    Incorporates claims validation rules:
        Facility ID needs to exist in the facility table
        Patient Account Number needs to exist
        Validate start and end date
        Valid financial class is in the facility lookup table(s)
        Valid date of birth
        Validate service line item dates fall within the start and end date of a claim
        If any of these validations fail, store the claim in either sql or postgres in a new table for failed claims and why they failed - this information needs to be viewed in a front end UI so determine which database to use
    
    Performs reimbursement calculation on each claim line item: RVU * Units * Conversion Factor ($36.04)
    Project directory structure needs to follow best practices
    Class files need to be separated for easier maintenance, troubleshooting
    Add async batch processing optimization: Your current batch processing could benefit from pipeline parallelization where stages overlap
    Implement connection pool warming: Pre-create database connections during startup
    Add in-memory caching for RVU data fetched from SQL Server to reduce database load
    Consider database read replicas for heavy read operations during validation
    Implement claim repair suggestions: AI-powered suggestions for fixing common errors
    Enhanced categorization: More granular error categories for better UI filtering
    Add comprehensive error handling: Standardized error types and recovery strategies
    Implement proper logging correlation: Request IDs across all components
    Add audit logging: Track all data modifications and access
    PII/PHI protection: Proper handling of sensitive healthcare data
    Add database connection health checks with automatic reconnection
    Implement read/write splitting for better performance
    Add database query optimization and indexing strategies
    
    Failed Claims UI:
        Since you're storing summaries in SQL Server for UI performance, add real-time sync status indicators
        Implement claim resolution workflows with approval processes
        Add analytics dashboard for failure pattern analysis

The system is designed for performance, overall optimization and best coding practices, scalability, reliability, compliance with CMS 1500 standards, and robust analytics, with a focus on production-ready ML integration.
