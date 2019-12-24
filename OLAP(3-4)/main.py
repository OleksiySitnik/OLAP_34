import pandas as pd
from transform import create_factors_table, create_measurement_tables
from sqlalchemy import create_engine

connection = create_engine("postgresql://postgres:postgres@localhost:5432/OLAP_34")

worldwide = pd.read_csv('data/worldwide-daily-song-ranking.csv')
top_2017 = pd.read_csv('data/spotify-top-2017.csv')
top_2018 = pd.read_csv('data/spotify-top-2018.csv')

measurements = ['Genre', 'Artists', 'Name', 'Year', 'Month', 'Day', 'Key', 'Mode', 'Region']




if __name__ == "__main__":
    print("Loading data...")
    fact_df = create_factors_table(top_2017, top_2018, worldwide)
    print("Transform data...")
    create_measurement_tables(fact_df, measurements, connection)
    print("Load data to DB...")
    fact_df.to_sql('Factors', con=connection)
    print("Success!")


