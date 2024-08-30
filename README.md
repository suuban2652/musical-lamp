# musical-lamp
lambda function
# Rick and Morty ETL Project

## Overview
This project demonstrates how to build an ETL (Extract, Transform, Load) pipeline using AWS services. It involves extracting data from the Rick and Morty API, transforming it, and loading it into AWS S3 for further analysis.

## Objectives
- **Understand AWS Lambda** for serverless data extraction, for data transformation.
- **Implement data storage** in Amazon S3 and querying our modeled data warehouse.
- **Learn to document and present** ETL processes effectively.

## Architecture Diagram
The ETL pipeline is illustrated in the diagram below:
![Data Flow Diagram](./diagrams/data_flow_diagram.png)

## Data Model
The data model of the final dataset includes four tables:
1. **Characters**: Information about characters (name, species, gender, etc.).
2. **Episodes**: Details of episodes (title, air date, characters involved).
3. **Locations**: Information about locations (name, type, dimension).
4. **Character_Episodes**: Relationship between characters and episodes.

Data model diagram:
![Data Model Diagram](./data_model/data_model_diagram.png)

## ETL Process

### Data Extraction
Data is extracted from the Rick and Morty API using AWS Lambda functions.

### Data Transformation
Data is transformed using AWS lambda functions.

### Data Loading
The transformed data is loaded into Amazon S3.

## Setup Instructions

### Prerequisites
- AWS account with necessary permissions.
- Python 3.x installed locally.

### Deployment
1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/RickAndMorty-ETL.git
   cd RickAndMorty-ETL
   
