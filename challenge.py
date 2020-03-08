import sys
import json
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
# PostgreSQL password
from config import db_password

# combine all alternate titles into alt_title column
# change column names
def clean_wiki_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie

# parse dollars, all convert to a float
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan

# given json file "wiki_movies_file", load to DataFrame and cleanup. 
def wiki_movies_df_get(wiki_movies_file):
    # json file
    with open(f'{wiki_movies_file}', mode='r') as file:
        wiki_movies_raw = json.load(file)
    
    # drop movies: no directors, no imdb_link, with "No. of episodes"
    wiki_movies = [movie for movie in wiki_movies_raw
                    if ('Director' in movie or 'Directed by' in movie)
                        and 'imdb_link' in movie
                        and 'No. of episodes' not in movie]

    # now have a cleaner data
    clean_movies = [clean_wiki_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    # extract "imdb_id" and drop duplicates
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # drop rare columns (columns < 10%)
    min_cnt = 0.1 * len(wiki_movies_df)
    column_cnts = wiki_movies_df.count()
    wiki_columns_keep = [col for (col, cnt) in zip(column_cnts.index, column_cnts.tolist()) if (cnt >= min_cnt)]
    wiki_movies_df = wiki_movies_df[wiki_columns_keep]

    # Box Office
    box_office = wiki_movies_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # mis-spelled as millon or billon, make it optional and take it.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})',
                                        flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    # Budget data
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    # Release Date
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0],
                                                                            infer_datetime_format=True)
    wiki_movies_df.drop('Release date', axis=1, inplace=True)

    # Running time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    #wiki_movies_df.shape
    return wiki_movies_df

# given csv file "kaggle_movies_file", load to DataFrame and cleanup.
def kaggle_metadata_get(kaggle_movies_file):
    # it is a csv file
    kaggle_metadata = pd.read_csv(f'{kaggle_movies_file}', low_memory=False)

    # keep non-adult movies, drop "adult" column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

    # convert to proper data types
    kaggle_metadata['video'] = (kaggle_metadata['video'] == 'True')
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    return kaggle_metadata

# given csv file "ratings_file", load to DataFrame.
def ratings_get(ratings_file):
    # it is a csv file
    ratings = pd.read_csv(f'{ratings_file}')

    # convert to datetime
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # ratings.count
    # ratings['rating'].describe()
    return ratings

# -----------------------------------------------------------------------
# if kaggle_column is zero, fill in wiki_column value;
# finally drop wiki_column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)

# merge two df's: "wiki_movies_df" and "kaggle_metadata" 
def merge_datasets(wiki_movies_df, kaggle_metadata):
    # merge wiki_movies_df and kaggle_metadata
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # language
    # movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)

    # release date
    # drop this odd one (release date diffs, wiki > 2000, kaggle < 1960)
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') &
                                        (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # title
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    # runtime, budget, box offices
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # reorder and rename
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    movies_df = movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns')

#    movies_df.shape
    return movies_df

# merge df "ratings" to "movies_df" 
def merge_ratings(movies_df, ratings):
    # pivot
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')

    # rename
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    # merge
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    return movies_with_ratings_df

# ----------------------------------------------------------------------------
# movies etl. with given three files:
#   wikipedia json file: wiki_movies_file
#   Kaggle megadata csv file: kaggle_movies_file
#   MovieLens rating csv file: ratings_file
# export data to postgres "movie_data", table "movies" and "ratings"
#
def movies_etl(wiki_movies_file, kaggle_movies_file, ratings_file):
    # extract from given json file, clean data and return a DataFrame
    wiki_movies_df = wiki_movies_df_get(wiki_movies_file)

    # extract from given csv file, clean data and return a DataFrame
    kaggle_metadata = kaggle_metadata_get(kaggle_movies_file)

    # extract from given csv file, clean data and return a DataFrame
    ratings = ratings_get(ratings_file)

    # merge wiki_movies_df and kaggle_metadata
    movies_df = merge_datasets(wiki_movies_df, kaggle_metadata)

    # merge ratings
    movies_with_ratings_df = merge_ratings(movies_df, ratings)

    # now export to postgres.
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)

    # export to sql table "movies"
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')

    # export HUGE rating data.
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{ratings_file}', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        if (rows_imported == 0):
            data.to_sql(name='ratings', con=engine, if_exists='replace')
        elif (rows_imported <= 1000000):
            data.to_sql(name='ratings', con=engine, if_exists='append')
        else:
#           data.to_sql(name='ratings', con=engine, if_exists='append')
            pass
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time:.2f} total seconds elapsed')

## either get the filenames from command line or input.
if __name__ == "__main__":
    wiki_file_str = "Wikipedia movies file"
    kaggle_file_str = "Kaggle megadata file"
    ratings_file_str = "MovieLens ratings file"

    is_filenames_given = False
    if (len(sys.argv) > 3):
        is_filenames_given = True
        wiki_movies_file = sys.argv[1]
        kaggle_movies_file = sys.argv[2]
        ratings_file = sys.argv[3]

    check_filenames = True
    while True:
        if is_filenames_given:
            print("\nThese are the given filenames:")
            print(f"{wiki_file_str}: {wiki_movies_file}")
            print(f"{kaggle_file_str}: {kaggle_movies_file}")
            print(f"{ratings_file_str}: {ratings_file}")
            check_filenames = (input("all filenames are right? (yes/no)").lower() != "yes")

        if (check_filenames == False):
            break
            
        print("\nInput full filenames (with path):")
        wiki_movies_file = input(f"{wiki_file_str}: (Data/wikipedia.movies.json)")
        kaggle_movies_file = input(f"{kaggle_file_str}: (Data/movies_metadata.csv)")
        ratings_file = input(f"{ratings_file_str}: (Data/ratings.csv)")  
        is_filenames_given = True

    print("\nNow perform movies data ETL...")
    movies_etl(wiki_movies_file, kaggle_movies_file, ratings_file)
