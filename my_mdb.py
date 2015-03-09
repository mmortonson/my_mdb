#!/usr/bin/env python


import os.path
import argparse
import re
import urllib
import json
import sqlite3

# tables:
# movies: imdbID, title, year, released, runtime, rated,
#         plot, poster, metascore, imdbRating, imdbVotes
# genre: imdbID, genre
# series: imdbID, series
# actors: imdbID, name, order
# directors: imdbID, name
# writers: imdbID, name
# formats: imdbID, format
# viewings: imdbID, date


# add one or more viewing times: watched <movie> <date1> <date2> ...
#
# query: search runtime < 120 min and lastwatched > 1 year


class MovieDatabase(object):
    def __init__(self, file_name):
        self.base_url = 'http://www.omdbapi.com/?'
        self.connection = sqlite3.connect(file_name)
        self.cursor = self.connection.cursor()
        if not os.path.isfile(file_name):
            print 'Creating new database: {0}'.format(file_name)
            self.create_tables()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def create_tables(self):
        self.cursor.execute("CREATE TABLE movies (id TEXT PRIMARY KEY, " +
                            "title TEXT, yr INTEGER, released DATE, " +
                            "runtime INTEGER, rated TEXT, plot TEXT, " +
                            "poster TEXT, imdbRating FLOAT, " +
                            "imdbVotes INTEGER)")
        self.cursor.execute("CREATE TABLE formats (id TEXT, format TEXT, " +
                            "PRIMARY KEY (id, format))")
        self.cursor.execute("CREATE TABLE viewings (id TEXT, watched DATE, " +
                            "PRIMARY KEY (id, watched))")
        self.cursor.execute("CREATE TABLE series (id TEXT, series TEXT, " +
                            "PRIMARY KEY (id, series))")
        self.cursor.execute("CREATE TABLE genres (id TEXT, genre TEXT, " +
                            "PRIMARY KEY (id, genre))")
        self.cursor.execute("CREATE TABLE actors (id TEXT, name TEXT, " +
                            "ord INTEGER, PRIMARY KEY (id, name))")
        self.cursor.execute("CREATE TABLE directors (id TEXT, name TEXT, " +
                            "PRIMARY KEY (id, name))")
        self.cursor.execute("CREATE TABLE writers (id TEXT, name TEXT, " +
                            "PRIMARY KEY (id, name))")

    def add_movie(self, title, fmt):
        fmt = self.standardize_format(fmt)
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT title, format FROM " +
                "formats JOIN movies ON formats.id=movies.id " +
                "WHERE title=? AND format=?", (search_data['Title'], fmt)))
            if len(records) > 0:
                print 'Already in the database:'
                for r in records:
                    print u'{0} ({1})'.format(r[0], r[1])
            else:
                # add to formats
                self.cursor.execute("INSERT INTO formats VALUES (?, ?)",
                                    (search_data['imdbID'], fmt))
                # add to movies (if not already present)
                records = list(self.cursor.execute(
                    "SELECT id FROM movies WHERE id=?",
                    (search_data['imdbID'],)))
                if len(records) == 0:
                    movie_data = self.query_omdb_by_id(search_data['imdbID'])
                    if len(movie_data) > 0:
                        self.add_omdb_data(movie_data)
                print u'{0} ({1}) added'.format(search_data['Title'], fmt)

    def delete_movie(self, title, fmt):
        fmt = self.standardize_format(fmt)
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT id, format FROM formats WHERE id=? AND format=?",
                (search_data['imdbID'], fmt)))
            if len(records) == 0:
                print 'Not in the database.'
            else:
                self.cursor.execute(
                    "DELETE FROM formats WHERE id=? AND format=?",
                    (search_data['imdbID'], fmt))
                print u'{0} ({1}) deleted'.format(search_data['Title'], fmt)

    def add_to_series(self, title, series):
        search_data = self.search_omdb(title)
        if search_data is not None:
            self.cursor.execute("INSERT INTO series VALUES (?, ?)",
                                (search_data['imdbID'], series))
            print u'Added {0} to the series {1}'.format(
                search_data['Title'], series)

    def add_viewing_date(self, title, date):
        search_data = self.search_omdb(title)
        if search_data is not None:
            self.cursor.execute("INSERT INTO viewings VALUES (?, ?)",
                                (search_data['imdbID'], date))
            print u'Added viewing date {0} for {1}'.format(
                date, search_data['Title'])

    def search_omdb(self, title):
        query = 's=' + urllib.quote(title) + '&type=movie'
        results = self.omdb_query(query)
        if 'Search' not in results or len(results['Search']) == 0:
            print 'No movies found matching {0}'.format(title)
            movie_data = None
        elif len(results['Search']) > 1:
            print 'Select a movie, or press Enter to cancel:'
            for i, result in enumerate(results['Search']):
                print u'{0}: {1} ({2})'.format(i, result['Title'],
                                               result['Year'])
            choice = raw_input()
            if len(choice) > 0:
                try:
                    movie_data = results['Search'][int(choice)]
                except:
                    movie_data = None
            else:
                movie_data = None
        else:
            movie_data = results['Search'][0]
            print u'Found {0} ({1})'.format(movie_data['Title'],
                                            movie_data['Year'])
            print 'Is this the right movie?'
            confirm = raw_input()
            if confirm.strip().lower().startswith('n'):
                movie_data = None
        return movie_data

    def query_omdb_by_id(self, imdbID):
        query = 'i=' + imdbID
        return self.omdb_query(query)

    def omdb_query(self, query):
        have_response = False
        n_attempts = 0
        max_attempts = 10
        while not have_response and n_attempts < max_attempts:
            n_attempts += 1
            try:
                response = urllib.urlopen(self.base_url + query).read()
                data = json.loads(response)
                have_response = True
            except:
                pass
        if n_attempts == max_attempts:
            print 'No response from OMDB.'
            return {}
        else:
            return data

    def add_omdb_data(self, omdb_dict):
        self.cursor.execute(
            "INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (omdb_dict['imdbID'], omdb_dict['Title'], omdb_dict['Year'],
             omdb_dict['Released'], omdb_dict['Runtime'], omdb_dict['Rated'],
             omdb_dict['Plot'], omdb_dict['Poster'],
             omdb_dict['imdbRating'], omdb_dict['imdbVotes']))
        genres = self.split_into_list(omdb_dict['Genre'])
        for g in genres:
            self.cursor.execute("INSERT INTO genres VALUES (?, ?)",
                                (omdb_dict['imdbID'], g))
        actors = self.split_into_list(omdb_dict['Actors'])
        order = range(1, len(actors)+1)
        for a, o in zip(actors, order):
            self.cursor.execute("INSERT INTO actors VALUES (?, ?, ?)",
                                (omdb_dict['imdbID'], a, o))
        directors = self.split_into_list(omdb_dict['Director'])
        for d in directors:
            self.cursor.execute("INSERT INTO directors VALUES (?, ?)",
                                (omdb_dict['imdbID'], d))
        writers = self.split_into_list(omdb_dict['Writer'])
        for w in writers:
            self.cursor.execute("INSERT INTO writers VALUES (?, ?)",
                                (omdb_dict['imdbID'], w))

    def split_into_list(self, string):
        comma_split = string.split(',')
        return [s.strip() for s in comma_split]

    def standardize_format(self, fmt):
        valid_formats = ('blu ray', 'DVD', 'iTunes', 'UltraViolet')
        pattern = re.compile('[\W_]+')
        for f in valid_formats:
            if pattern.sub('', fmt).lower() == pattern.sub('', f).lower():
                return f
        raise ValueError


class InputParser(object):
    def __init__(self):
        self.input = []

    def read_input(self, prompt=''):
        input_string = raw_input(prompt)
        self.input = input_string.split()

    def get_input(self, i=None):
        if i is None:
            return self.input
        else:
            return self.input[i]

    def has_input(self):
        return len(self.input) > 0


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='name of the database file')
    args = parser.parse_args()

    mdb = MovieDatabase(args[0])
    input_parser = InputParser()
    while not input_parser.has_input() or \
            (input_parser.has_input() and
             not input_parser.get_input(0) in ('exit', 'quit', 'q')):
        input_parser.read_input()

    mdb.close()
