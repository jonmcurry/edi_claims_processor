edi_claims_processor/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── claims_processor.py
│   │   ├── edi_parser.py
│   │   ├── rules_engine.py
│   │   ├── ml_predictor.py
│   │   ├── reimbursement_calculator.py
│   │   └── batch_handler.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── postgres_handler.py
│   │   ├── sqlserver_handler.py
│   │   ├── connection_manager.py
│   │   └── models/
│   │       ├── __init__.py
│   │       ├── postgres_models.py
│   │       └── sqlserver_models.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── caching.py              # Caching mechanisms (e.g., in-memory caching for DB query results like RVU data)
│   │   ├── logging_config.py
│   │   ├── security.py
│   │   └── error_handler.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── endpoints.py
│   │   └── schemas.py
│   └── services/
│       ├── __init__.py
│       └── claim_repair_service.py
├── config/
│   ├── __init__.py
│   └── config.yaml
├── data/
│   └── sample_edi_claims/
│       └── sample_claim.edi
├── database_scripts/
│   ├── postgresql_create_edi_databases.sql
│   ├── sqlserver_create_results_database.sql
│   ├── failed_claims_table.sql
│   ├── postgres_indexes.sql
│   └── sqlserver_indexes.sql
├── logs/
│   └── app.log
│   └── audit.log
├── ml_model/
│   ├── __init__.py
│   ├── model.pkl
│   └── training_scripts/
│       └── train_model.py
├── .gitignore
├── requirements.txt
└── README.md