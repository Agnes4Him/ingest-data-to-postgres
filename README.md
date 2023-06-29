# ingest-data-to-postgres

This is a Data Engineering ETL project that involves writing a script that pulls data from the internet in csv.gz format, merge the data from diverse files into one by reading from each file using pandas library, then read from the merged file, transform this new data, and then load the data into PostgreSQL database.