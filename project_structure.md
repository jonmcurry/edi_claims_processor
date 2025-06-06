edi_claims_processor/
├── app/
│   ├── init.py
│   ├── main.py
│   ├── processing/
│   │   ├── init.py
│   │   ├── claims_processor.py
│   │   ├── rules_engine.py
│   │   ├── ml_predictor.py
│   │   ├── reimbursement_calculator.py
│   │   └── batch_handler.py
│   ├── database/
│   │   ├── init.py
│   │   ├── postgres_handler.py
│   │   ├── sqlserver_handler.py
│   │   ├── connection_manager.py
│   │   └── models/
│   │       ├── init.py
│   │       ├── postgres_models.py
│   │       └── sqlserver_models.py
│   ├── utils/
│   │   ├── init.py
│   │   ├── caching.py
│   │   ├── logging_config.py
│   │   ├── security.py
│   │   └── error_handler.py
│   ├── api/
│   │   ├── init.py
│   │   ├── endpoints.py
│   │   └── schemas.py
│   └── services/
│       ├── init.py
│       └── claim_repair_service.py
├── config/
│   ├── init.py
│   └── config.yaml
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
│   ├── init.py
│   ├── model.pkl
│   └── training_scripts/
│       └── train_model.py
├── .gitignore
├── requirements.txt
└── README.md