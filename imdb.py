#!/usr/bin/env python3

import gzip
import sqlite3
import requests

class IMDB():
    def __init__(self, filename: str = 'resources/movies.db',) -> None:
        self.dataset_urls = {
            'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
            'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
        }

        self.connection = sqlite3.connect(filename)
        self.cursor = self.connection.cursor()

    def download_dataset(self, filename: str, url: str) -> None:
        response = requests.get(url)
        with open(filename, 'wb') as file:
            file.write(response.content)

    def update(self) -> bool:
        self.cursor.execute('DROP TABLE IF EXISTS movies')
        self.cursor.execute('DROP TABLE IF EXISTS ratings')
        self.connection.commit()

        for name, url in self.dataset_urls.items():
            # self.download_dataset(f'{name}.tsv.gz', url)
            pass

        self.cursor.execute('CREATE TABLE movies (tconst TEXT, primaryTitle TEXT, originalTitle TEXT, isAdult INTEGER, year INTEGER, runtime INTEGER, genres TEXT)')
        self.cursor.execute('CREATE TABLE ratings (tconst TEXT, rating REAL, votes INTEGER)')
        self.connection.commit()

        with gzip.open('title_basics.tsv.gz', 'r') as file:
            for line in file:
                values = line.decode('utf-8').split('\t')
                if values[1] == 'movie':
                    self.cursor.execute('INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?)', (values[0], values[2], values[3], values[4], values[5], values[7], values[8]))
            self.connection.commit()

if __name__ == '__main__':
    imdb = IMDB()
    imdb.update()
