import glob
import os
import pandas as pd
import datetime
import sqlalchemy


def extract():
    """
    A function to extract data from the folder and load it in to a dataframe
    :return: a dataframe representing all the data
    """
    # Load the files
    root_dir = os.path.dirname(os.path.abspath(__file__))
    data_directory = os.path.join(root_dir, 'data')
    # list of file paths
    file_path_list = []

    # we walk through all subdirectories to find .json files
    for subdir, dirs, files in os.walk(data_directory):
        for file in files:
            file_path_list.append(os.path.join(subdir, file))

    # List of dataframes for each file
    dfs = [pd.read_json(x, lines=True) for x in file_path_list]

    # we concatenate all the dataframes in only one
    df = pd.concat(dfs, axis=0, ignore_index=True)
    return df


def transform(dataframe):
    """
    A function to transform the dataframe and meet the requirement listed in readme.md
    :param dataframe: the data to transform(panda.DataFrame)
    :return: the transformed dataframe
    """
    df_valid = dataframe.dropna()
    df_valid = df_valid[df_valid['page'] == 'NextSong']

    # transform ts into week of the year
    weeks = df_valid.ts.apply(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).weekday())
    weeks
    df_valid['week'] = weeks

    # We divide in dataframes for each week
    weeks_unique = weeks.drop_duplicates()

    df_per_week = {week: df_valid[df_valid['week'] == week] for week in weeks}

    for week, df in df_per_week.items():
        df_per_week[week] = df[['week', 'song', 'sessionId']] \
            .groupby(['week', 'song'], as_index=False) \
            .agg('count') \
            .sort_values('sessionId', ascending=False) \
            .head(1)

    best_song_per_week = pd.concat([df for df in df_per_week.values()],
                                   axis=0, ignore_index=True)
    return best_song_per_week


def load(dataframe):
    """
    A function to load the transformed dataframe in a postgresql table
    :param dataframe: dataframe to load
    :return:
    """
    user = 'dej'
    password = 'dej'
    host = 'localhost'
    port = '5432'
    database = 'etl_log'

    # Create the database engine
    conn_uri = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database)
    db_engine = sqlalchemy.create_engine(conn_uri)

    # Load to best_song_per_week table in etl_log
    dataframe.to_sql("best_song_per_week",
                     db_engine,
                     if_exists='replace')
