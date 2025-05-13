project/
├── data/
│   ├── raw/
│   │   └── scraped/
│   │       ├── daily/
│   │       └── hourly/
│   └── processed/
│       ├── merged/
│       │   ├── daily/
│       │   └── hourly/
│       ├── interpolated/
│       └── formatted/
├── logs/
├── src/
│   ├── config/
│   │   ├── config.json
│   │   └── station_data.json
│   ├── data_acquisition/
│   │   └── jma/
│   │       ├── daily_scraper.py
│   │       ├── hourly_scraper.py
│   │       └── scraper.py
│   ├── data_processing/
│   │   └── jma/
│   │       ├── formatter.py
│   │       ├── interpolator.py
│   │       └── merger.py
│   └── utils/
│       ├── config_loader.py
│       └── logger.py
├── export_config.py
├── interactive_run.sh
├── Makefile
├── manage_stations.py
├── run.sh
├── run_pipeline.py
├── run_task.py
└── setup_dirs.py