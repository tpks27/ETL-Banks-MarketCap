# Acquiring and Processing Information on the World's Largest Banks 
## Project Description
This project extracts, transforms, and loads data about the largest banks' market capitalizations from a Wikipedia page. The data is converted into multiple currencies and stored in both CSV and SQLite database formats.
## Features
--**Extracts bank data from Wikipedia using BeautifulSoup**.

--**Transforms market capitalization values into GBP, EUR, and INR.**

--**Stores transformed data in a CSV file and an SQLite database.**

--**Logs progress and query results.**
## Prerequisites
Ensure you have Python installed (>= 3.8). You also need the required libraries installed.

## Files & Directories

--**src/etl_script.py: Main ETL script.**

--**data/exchange_rate.csv: Currency exchange rates.**

--**logs/code_log.txt: Logs progress and errors**

--**Banks.db: SQLite database storing the bank data.**

## Future Improvements
Add unit tests for better stability.
Enhance error handling and logging.
