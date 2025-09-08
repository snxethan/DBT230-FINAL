# DBT230 Final Project - Neo4j Database Application

A Java-based application that demonstrates Neo4j database connectivity and data processing for employment/occupation data analysis.

## Project Overview

This project (RDBL1 - Relational Database Lab 1) is designed to work with Neo4j graph database to process and analyze employment data. The application connects to a Neo4j database, reads data from files, and creates data objects for further analysis.

*Created: October 2024*

## Features

- **Neo4j Database Integration**: Connects to local Neo4j database instance
- **Batch Data Processing**: Efficiently processes large datasets (38+ million records) in batches
- **Occupation Data Mapping**: Maps occupation IDs to occupation descriptions
- **File-based Data Import**: Reads and processes data from external files
- **Optimized Performance**: Configured for high-performance data insertion with connection pooling

## Prerequisites

- **Java 18** or higher
- **Maven 3.6+** for dependency management
- **Neo4j Database** (local instance running on port 7687)
- Neo4j database credentials (default: neo4j/neo12345)

## Dependencies

- **Neo4j Java Driver** (v5.22.0) - Database connectivity
- **SLF4J API** (v2.1.0-alpha1) - Logging framework
- **SLF4J Simple** (v2.1.0-alpha1) - Logging implementation

## Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/snxethan/DBT230-FINAL.git
   cd DBT230-FINAL
   ```

2. **Install Neo4j:**
   - Download and install Neo4j Desktop or Neo4j Community Edition
   - Start Neo4j database service on `localhost:7687`
   - Set up authentication: username `neo4j`, password `neo12345`

3. **Build the project:**
   ```bash
   mvn clean compile
   ```

4. **Run the application:**
   ```bash
   mvn exec:java -Dexec.mainClass="Main"
   ```

## Project Structure

```
src/main/java/
├── Main.java              # Application entry point
├── Neo4jController.java   # Database connection and data processing
└── DataObject.java        # Data model for occupation/employment data
```

## Configuration

The application is configured with the following default settings:

- **Database URI**: `neo4j://localhost:7687`
- **Username**: `neo4j`
- **Password**: `neo12345`
- **Connection Pool Size**: 10,000
- **Batch Size**: 800,000 records

To modify these settings, update the constants in `Neo4jController.java`.

## Data Model

The `DataObject` class represents employment data with the following fields:
- `seriesID` - Unique identifier for data series
- `year` - Year of the data point
- `month` - Month of the data point  
- `value` - Numerical value (employment figures)
- `occupationID` - Occupation category identifier

## Performance

The application is optimized for large-scale data processing:
- Processes **38,861,474 records** in approximately **8 minutes**
- Uses batch processing with configurable batch sizes
- Implements connection pooling for optimal database performance

## Usage

1. Ensure Neo4j database is running and accessible
2. Place your data files in the appropriate directory
3. Run the main application class
4. The application will:
   - Connect to Neo4j database
   - Create data objects from files
   - Process and insert data in batches
   - Close database connection

## Troubleshooting

- **Connection Issues**: Verify Neo4j is running on port 7687
- **Authentication Errors**: Check username/password credentials
- **Performance Issues**: Adjust batch size and connection pool settings
- **Memory Issues**: Increase JVM heap size for large datasets

## Author(s)

- [**Ethan Townsend (snxethan)**](www.ethantownsend.dev)
- Ethan Smith
- Victor Keeler
- Jacob Brincefield

