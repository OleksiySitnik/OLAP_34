import pandas as pd


def clean_data(*dataframes):
    clean_dataframes = []

    for df in dataframes:
        clean_df = df.drop(['genre_x', 'Track Name', 'Artist', 'id'], axis=1)\
                     .rename(columns={'genre_y': 'genre'})\
                     .rename(str.capitalize, axis='columns')
        clean_df['Year'] = clean_df.apply(lambda x: x['Date'].split('-')[0], axis = 1)
        clean_df['Month'] = clean_df.apply(lambda x: x['Date'].split('-')[1], axis=1)
        clean_df['Day'] = clean_df.apply(lambda x: x['Date'].split('-')[2], axis=1)
        clean_df = clean_df.drop('Date', axis=1)
        clean_dataframes.append(clean_df)

    return clean_dataframes


def create_factors_table(df_1, df_2, full_df):

    merged_df_1 = pd.merge(left=full_df,right=df_1, left_on=['Track Name', 'Artist'], right_on=['name', 'artists'])
    merged_df_2 = pd.merge(left=full_df, right=df_2, left_on=['Track Name', 'Artist'], right_on=['name', 'artists'])

    clean_dataframes = clean_data(merged_df_1, merged_df_2)
    concat_df = pd.concat(clean_dataframes)

    return concat_df


def create_measurement_tables(df, measurements, conn):
    for measurement in measurements:
        measurement_df = df[measurement].unique()
        pd.DataFrame({measurement: measurement_df}).to_sql(measurement, con=conn)
        df[measurement] = df.apply(lambda x: measurement_df.tolist().index(x[measurement]) + 1, axis=1)


# if __name__ == "__main__":
#     table = pd.read_csv('data/worldwide-daily-song-ranking.csv')
#     top_2017 = pd.read_csv('data/spotify-top-2017.csv')
#     top_2018 = pd.read_csv('data/spotify-top-2018.csv')

    # df1 = create_factors_table(top_2017, top_2018, table)
    # print(find_index_of_measurement('rap', genres, 'Genres'))
    # print(df1[['Year', 'Month', 'Day']])
    # print(genres)
    # print(m2)