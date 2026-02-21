# COMP-3610-Assignment-1
Overview
This project builds an end-to-end data pipeline for the NYC Yellow Taxi Trip dataset (January 2024). It ingests, cleans, and analyzes approximately 3 million trip records, and presents findings through an interactive Streamlit dashboard.

Repository Structure
assignment1/
	assignment1.ipynb   # Main notebook (Parts 1, 2, 3 prototyping)
	app.py              # Streamlit dashboard (Part 3)
	requirements.txt    # Python dependencies with version pins
	README.md           # This file
	.gitignore          # Excludes data/ directory
	data/               # NOT committed – auto-created by notebook
   		raw/
       			yellow_tripdata_2024-01.parquet
        		taxi_zone_lookup.csv

Setup Instructions
1. Clone the repository

```bash
git clone <"https://github.com/FaribaB/COMP-3610-Assignment-1">
cd <"COMP-3610-Assignment-1">
```

2. Create and activate a virtual environment
```bash
python -m venv venv
```

Mac/Linux:
```bash
source venv/bin/activate
```

Windows:
```bash
venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```
(you may need to use python -m pip install -r requirements.txt depending on your luck)

4. Run the Jupyter Notebook
```bash
jupyter notebook assignment1.ipynb
```

Run all cells in order. The notebook will automatically:
- Download the raw data files into `data/raw/`
- Validate, clean, and transform the data
- Run five SQL queries using DuckDB
- Prototype all five dashboard visualizations

NOTE:
The data files are not included in the repository. They will be downloaded automatically when you run the notebook or launch the app for the first time. An internet connection is required.

5. Launch the Streamlit Dashboard
```bash
streamlit run app.py
```
(you may need to use python -m streamlit run app.py , also depending on your luck)

The app will open at `http://localhost:8501`.



Data Sources
`yellow_tripdata_2024-01.parquet`
NYC Yellow Taxi trip records, January 2024 (~3M rows) 
[Download] (https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)

`taxi_zone_lookup.csv` 
TLC Taxi Zone lookup table 
[Download](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) 

Data provided by the [NYC Taxi and Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).


 Deployed Dashboard
 Live Dashboard: https://comp-3610-assignment-1-fariba-bhaggan.streamlit.app/

AI Tools Disclosure
See the final cell of `assignment1.ipynb` for a full AI tools disclosure statement.
