#!/usr/bin/env python


import os.path
import argparse
import re
import urllib
import json
import sqlite3
import datetime
import dateutil.relativedelta

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


def strip_operator(condition):
    op = ''
    i = 0
    for c in condition.strip():
        if c in ['=', '<', '>']:
            op += c
            i += 1
        else:
            break
    value = condition.strip()[i:].strip()
    if op in ['=', '<', '<=', '>', '>=']:
        return (op, value)
    else:
        print 'Invalid condition.'
        return ('', value)


def reverse_operator(op):
    if op[0] == '<':
        return '>' + op[1:]
    elif op[0] == '>':
        return '<' + op[1:]
    else:
        return op


class MovieDatabase(object):
    def __init__(self, file_name):
        new_db = not os.path.isfile(file_name)
        self.base_url = 'http://www.omdbapi.com/?'
        self.connection = sqlite3.connect(file_name)
        self.cursor = self.connection.cursor()
        if new_db:
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
        self.cursor.execute("CREATE TABLE viewings (id TEXT, view_date DATE, " +
                            "PRIMARY KEY (id, view_date))")
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
        self.create_latest_viewings()
        self.connection.commit()

    def add_movie(self, title, fmt):
        fmt = self.standardize_format(fmt)
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT title, format FROM " +
                "formats JOIN movies ON formats.id=movies.id " +
                "WHERE title=? AND format=?", (search_data['Title'], fmt)))
            if len(records) > 0:
                print '\nAlready in the database:'
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
                print u'\n{0} ({1}) added'.format(search_data['Title'], fmt)
            self.connection.commit()

    def delete_movie(self, title, fmt):
        fmt = self.standardize_format(fmt)
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT id, format FROM formats WHERE id=? AND format=?",
                (search_data['imdbID'], fmt)))
            if len(records) == 0:
                print '\nNot in the database.'
            else:
                self.cursor.execute(
                    "DELETE FROM formats WHERE id=? AND format=?",
                    (search_data['imdbID'], fmt))
                print u'\n{0} ({1}) deleted'.format(search_data['Title'], fmt)
            self.connection.commit()

    def get_all_movies(self):
        return list(self.cursor.execute("SELECT title FROM movies"))

    def search(self, filters):
        # replace these with query dictionary?
        query_string = "SELECT "
        query_columns = ["title"]
        query_tables = ["movies"]
        query_filters = []
        query_values = []
        # replace if statements with loop over keywords and new function?
        if 'runtime' in filters:
            condition = filters['runtime']
            op, value = strip_operator(condition)
            if not op:
                return []
            query_columns.append("runtime")
            query_filters.append("runtime " + op + " ?")
            query_values.append(value)
        if 'series' in filters:
            value = filters['series']
            query_tables.append("series")
            query_columns.append("series")
            query_filters.append("series = ?")
            query_values.append(value)
        if 'last_viewed' in filters:
            condition = filters['last_viewed']
            op, value = strip_operator(condition)
            if not op:
                return []
            else:
                op = reverse_operator(op)
            try:
                number, unit = value.split()
                if unit.lower()[0] == 'y':
                    interval = {'years': int(number)}
                elif unit.lower()[0] == 'm':
                    interval = {'months': int(number)}
                elif unit.lower()[0] == 'd':
                    interval = {'days': int(number)}
                else:
                    raise ValueError
            except:
                print 'Invalid date interval format.'
                return []
            today = datetime.date.today()
            delta = dateutil.relativedelta.relativedelta(**interval)
            value = (today - delta).isoformat()
            query_tables.append("latest_viewings")
            query_columns.append("COALESCE(view_date, '0001-01-01') " +
                                 "AS latest_date")
            query_filters.append("latest_date " + op + " ?")
            query_values.append(value)

        query_string += ", ".join(query_columns) + " FROM " + \
            " LEFT JOIN ".join(query_tables)
        if filters:
            if len(query_tables) > 1:
                query_string += " ON movies.id = " + \
                    ".id AND movies.id = ".join(query_tables[1:]) + ".id"
            query_string += " WHERE " + " AND ".join(query_filters)
        records = list(self.cursor.execute(query_string, query_values))
        return records

    def add_to_series(self, title, series):
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT id, series FROM series WHERE id=? AND series=?",
                (search_data['imdbID'], series)))
            if len(records) == 0:
                self.cursor.execute("INSERT INTO series VALUES (?, ?)",
                                    (search_data['imdbID'], series))
            print u'\nAdded {0} to the series {1}'.format(
                search_data['Title'], series)
            self.connection.commit()

    def add_viewing_date(self, title, date):
        search_data = self.search_omdb(title)
        if search_data is not None:
            records = list(self.cursor.execute(
                "SELECT id, view_date FROM viewings WHERE id=? AND view_date=?",
                (search_data['imdbID'], date)))
            if len(records) == 0:
                self.cursor.execute("INSERT INTO viewings VALUES (?, ?)",
                                    (search_data['imdbID'], date))
                self.create_latest_viewings()
            print u'\nAdded viewing date {0} for {1}'.format(
                date, search_data['Title'])
            self.connection.commit()

    def create_latest_viewings(self):
        self.cursor.execute("DROP VIEW IF EXISTS latest_viewings")
        self.cursor.execute("CREATE VIEW latest_viewings AS " +
                            "SELECT id, MAX(view_date) AS view_date " +
                            "FROM viewings GROUP BY id")

    def search_omdb(self, title):
        query = 's=' + urllib.quote(title) + '&type=movie'
        results = self.omdb_query(query)
        if 'Search' not in results or len(results['Search']) == 0:
            print '\nNo movies found matching {0}'.format(title)
            movie_data = None
        elif len(results['Search']) > 1:
            print '\nSelect a movie, or press Enter to cancel:'
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
            print u'\nFound {0} ({1})'.format(movie_data['Title'],
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
            print '\nNo response from OMDB.'
            return {}
        else:
            return data

    def add_omdb_data(self, omdb_dict):
        self.cursor.execute(
            "INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (omdb_dict['imdbID'], omdb_dict['Title'], omdb_dict['Year'],
             omdb_dict['Released'], omdb_dict['Runtime'].split()[0],
             omdb_dict['Rated'], omdb_dict['Plot'], omdb_dict['Poster'],
             omdb_dict['imdbRating'], omdb_dict['imdbVotes']))
        genres = self.split_into_list(omdb_dict['Genre'])
        for g in set(genres):
            self.cursor.execute("INSERT INTO genres VALUES (?, ?)",
                                (omdb_dict['imdbID'], g))
        actors = self.split_into_list(omdb_dict['Actors'])
        order = range(1, len(actors)+1)
        for a, o in zip(actors, order):
            self.cursor.execute("INSERT INTO actors VALUES (?, ?, ?)",
                                (omdb_dict['imdbID'], a, o))
        directors = self.split_into_list(omdb_dict['Director'])
        for d in set(directors):
            self.cursor.execute("INSERT INTO directors VALUES (?, ?)",
                                (omdb_dict['imdbID'], d))
        writers = self.split_into_list(omdb_dict['Writer'])
        for w in set(writers):
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

    def read_option(self, options):
        for i, option in enumerate(options):
            print u'{0}: {1}'.format(i, option)
        option_number = raw_input('Select an option: ')
        try:
            self.input = [options[int(option_number)]]
        except:
            self.input = []

    def get_input(self, i=None):
        if i is None:
            return self.input
        else:
            return self.input[i]

    def has_input(self):
        return len(self.input) > 0


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('file', help='name of the database file')
    args = arg_parser.parse_args()

    mdb = MovieDatabase(args.file)
    input_parser = InputParser()
    while not input_parser.has_input() or \
            (input_parser.has_input() and
             not input_parser.get_input(0) == 'Exit'):
        print
        menu = ['Search', 'Add movie', 'Delete movie',
                'Add viewing date', 'Add series name', 'Exit']
        input_parser.read_option(menu)
        if input_parser.has_input() and \
                input_parser.get_input(0) == 'Add movie':
            title = raw_input('\nMovie?\n')
            fmt = raw_input('\nFormat?\n')
            mdb.add_movie(title, fmt)
        elif input_parser.has_input() and \
                input_parser.get_input(0) == 'Delete movie':
            title = raw_input('\nMovie?\n')
            fmt = raw_input('\nFormat?\n')
            mdb.delete_movie(title, fmt)
        elif input_parser.has_input() and \
                input_parser.get_input(0) == 'Search':
            filters = {}
            n_filters = 0
            output_string = u'{0}'
            runtime = raw_input('\nRuntime in minutes (e.g. < 120)?\n' +
                                '(Press Enter to skip this filter.)\n')
            if runtime:
                filters['runtime'] = runtime
                n_filters += 1
                output_string += ' - {' + str(n_filters) + '} min.'
            last_viewed = raw_input('\nTime since last viewing, ' +
                                    'in years, months, or days ' +
                                    '(e.g. > 1 year)?\n' +
                                    '(Press Enter to skip this filter.)\n')
            if last_viewed:
                filters['last_viewed'] = last_viewed
                n_filters += 1
                output_string += ' - last viewed {' + str(n_filters) + '}'
            series = raw_input('\nName of series (e.g. Star Wars)?\n' +
                               '(Press Enter to skip this filter.)\n')
            if series:
                filters['series'] = series
                n_filters += 1
                output_string += ' - {' + str(n_filters) + '} series'
            print
            results = mdb.search(filters)
            if results:
                for movie in results:
                    movie = list(movie)
                    if '0001-01-01' in movie:
                        movie[movie.index('0001-01-01')] = '?'
                    print output_string.format(*movie)
            else:
                print 'No matching results.'
        elif input_parser.has_input() and \
                input_parser.get_input(0) == 'Add viewing date':
            title = raw_input('\nMovie?\n')
            date = raw_input('\nDate? (YYYY-MM-DD)\n')
            mdb.add_viewing_date(title, date)
        elif input_parser.has_input() and \
                input_parser.get_input(0) == 'Add series name':
            title = raw_input('\nMovie?\n')
            series = raw_input('\nSeries?\n')
            mdb.add_to_series(title, series)

    mdb.close()
