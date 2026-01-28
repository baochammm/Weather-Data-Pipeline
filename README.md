# Weather Data Pipeline

## Project Overview

This project implements a simplified ETL (Extract–Load–Transform) pipeline that collects real-time weather data from the OpenWeatherMap API, stores raw data in a MySQL staging table, transforms it using SQL, and loads clean data into a fact table for analysis.

## Tech Stack

- Python
- MySQL
- Libraries: requests, mysql-connector-python, python-dotenv
- Architecture: ELT with Staging & Fact layers

## How to Run

### 1.Clone the repository

### 2.Install dependencies

```bash
pip install requests mysql-connector-python python-dotenv
```

### 3.Configure environment variables

Create a `.env` file and add your OpenWeatherMap API key and MySQL credentials:

```bash
WEATHER_API_KEY=your_openweathermap_api_key
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=weather_db
```

### 4.Setup Database

Execute the following SQL script in MySQL Workbench:

```bash
sql/create_tables.sql
```

### 5.Run data ingestion

```bash
python src/ingestion.py
```

### 6.Transform data

Run the transformation scripts:

```bash
sql/transformations.sql
```

## Result

The final `fact_weather` table provides a clean, query-ready format for BI tools or further data analysis.
![Result](./asset/result.png)
