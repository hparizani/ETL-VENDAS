# Sales ETL Pipeline (Python & SQL)

## 🚀 Project Overview

This project demonstrates a complete *ETL (Extract, Transform, Load)* process built with Python. It automates the ingestion of sales data from flat files (CSV) into a relational database, ensuring data integrity and structured storage for further analysis.

## 🛠 Tech Stack

* Language: Python 3.x
* Data Manipulation: Pandas
* Database Integration: SQLAlchemy
* Database: PostgreSQL / MySQL / SQLite (Configurable)
* Version Control: Git

## 📋 Key Features
* Extraction: Efficient reading of large CSV datasets using Pandas.
* Transformation: Data cleaning and type conversion.
  * handing missing values.
  * Standardizing formats for database compatibillity.
* Loading: Robust data persistence into a relational database using SQLAlchemy's ORM capabilities. 

## ⚙️ How to Run
1. Clone the repository:
   `git clone https://github.com/hparizani/ETL-VENDAS.git`
2. Install dependencies:
   `pip install -r requirementes.txt`
3. Run the pipeline:
   `python main.py`

## 📈 Future Improvements (Roadmap)
* [] Containerization: Add Docker and Docker Compose support for easy deploment.
* [] Logging & Monitoring: Implement Python's logging library for better traceability.
* [] API Integration: Develop a FastAPI layer to serve the processed data.
* [] Unit Testing: Add Pytest for critical data transformation functions.
