# Airflow PostgreSQL ETL Pipeline for Wikipedia Pageviews Analysis

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.0%2B-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-blue)
![Docker](https://img.shields.io/badge/Docker-required-blue)
![Status](https://img.shields.io/badge/Status-Production-green)

## Overview

A comprehensive ETL solution that leverages Apache Airflow to extract, transform, and load Wikipedia pageviews data for major technology companies. The pipeline analyzes public interest trends for Google, Facebook, Amazon, Apple, and Microsoft through their Wikipedia pages.

## Architecture

![Project Architecture](imgs/Diagramme.png)

The system follows a classic ETL workflow:
1. **Extraction**: Fetching pageviews data from Wikimedia
2. **Transformation**: Filtering data for targeted companies
3. **Loading**: Storage in PostgreSQL database
4. **Analysis**: Identifying companies with the highest number of views

## Features

- Automated data pipeline orchestrated by Apache Airflow
- Data extraction from Wikimedia pageviews dumps
- Data transformation to filter for specific companies
- PostgreSQL database for data storage
- Analysis to identify pageviews trends
- Streamlit dashboard for visualization
- Containerized environment using Docker
- Scalable architecture for handling large datasets
- Configurable parameters for custom analysis
- Error handling and logging for robust operation

## Workflow

![Airflow DAG](imgs/etapesairflow.png)

The ETL pipeline consists of the following steps:

1. **Download Data**: Fetch compressed pageviews data from Wikimedia
2. **Extract Pageviews**: Decompress the downloaded file
3. **Filter Pageviews**: Transform data to keep only entries for selected companies
4. **Load Pageviews**: Store filtered data in PostgreSQL database
5. **Analyze Pageviews**: Perform analysis to identify the company with highest pageviews

## Results

The analysis identifies which technology company had the highest number of Wikipedia pageviews during the specified timeframe, providing insights into public interest.

![Airflow Results](imgs/resultatairflow.png)

Results can also be visualized through a Streamlit dashboard:

![Streamlit Dashboard](imgs/resultatstreamlit.png)

## Installation and Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher
- 4GB RAM minimum (8GB recommended)
- 10GB available disk space

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/ayoub-anhal/Airflow-PostgreSQL-ETL-Pipeline-for-Pageviews-Analysis.git
   cd Airflow-PostgreSQL-ETL-Pipeline-for-Pageviews-Analysis
   ```

2. **Create a `.env` file** in the project root with the following content:
   ```
   AIRFLOW_UID=50000
   DATABASE_URL=postgresql://airflow:airflow@localhost:5432/airflow
   ```

3. **Install required Python packages**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Start Docker containers**:
   ```bash
   docker-compose up -d
   ```

5. **Access the Airflow web interface** at http://localhost:8080/
   - Username: `airflow`
   - Password: `airflow`

6. **Activate and trigger the DAG** named "pageviews_dag" from the Airflow interface

7. **View results** in the Streamlit dashboard:
   ```bash
   streamlit run dags/streamlit.py
   ```

### Configuration Options

You can customize the pipeline by modifying the following parameters in the `dags/pageviews_dag.py` file:

```python
# List of companies to track
COMPANIES = ['Google', 'Facebook', 'Amazon', 'Apple', 'Microsoft']

# Time period for analysis
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 1, 31)

# Database configuration
DB_CONN_ID = 'postgres_default'
```

## Project Structure

```
├── dags/
│   ├── pageviews_dag.py   # Main DAG definition
│   ├── helper.py          # Utility functions for the DAG
│   └── streamlit.py       # Dashboard application
├── imgs/                  # Documentation images
├── docker-compose.yaml    # Docker Compose configuration
├── .env                   # Environment variables
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## Technologies Used

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data storage
- **Streamlit**: Data visualization
- **Docker**: Containerization
- **Python**: Programming language
- **Pandas**: Data manipulation and analysis
- **Matplotlib/Plotly**: Data visualization libraries

## Troubleshooting

If you encounter issues with the PostgreSQL container failing to start, ensure that:
1. The `.env` file is correctly configured
2. No other service is using port 5432
3. You have sufficient permissions to create Docker volumes

For connection issues between Streamlit and the database, verify that the DATABASE_URL environment variable is correctly set and accessible.

## Performance Considerations

- For large datasets, consider increasing the memory allocation for Docker containers
- The pipeline is optimized for hourly data processing; for real-time analysis, modifications may be required
- Database indexing is configured for optimal query performance on company name and date fields

## Future Improvements

- Add support for more companies
- Implement time series analysis to track pageview trends over time
- Add predictive models to correlate pageviews with stock price movements
- Enhance the Streamlit dashboard with more interactive visualizations
- Implement automated testing for pipeline components
- Add CI/CD integration for automated deployment
- Implement data quality checks throughout the pipeline

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Acknowledgments

- Wikimedia for providing open access to pageviews data
- Apache Airflow community for the powerful workflow orchestration tool
- Streamlit team for the intuitive data visualization framework

## Contact

Project Link: [https://github.com/ayoub-anhal/Airflow-PostgreSQL-ETL-Pipeline-for-Pageviews-Analysis](https://github.com/ayoub-anhal/Airflow-PostgreSQL-ETL-Pipeline-for-Pageviews-Analysis)
