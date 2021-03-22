#!/usr/bin/env python3

import os
import re
import gzip
import sqlite3
import requests

from abc import (ABC, abstractmethod)
from typing import (List, Optional)
from dataclasses import dataclass

@dataclass
class Movie():
    tconst: str
    primary_title: str
    original_title: str
    is_adult: int
    year: int
    runtime: int
    generes: str
    rating: float
    votes: int

    def post_url(self) -> Optional[str]:
        movie_url = f'https://www.imdb.com/title/{self.tconst}/'
        response = requests.get(movie_url)
        poster_url_pattern = r'^\s*"image":\s*"(https:\/\/.+\.jpg)",$'
        html_content = response.content.decode('utf-8')
        match = re.search(poster_url_pattern, html_content, flags=re.MULTILINE)
        return match.group(1) if match else match

@dataclass
class Table(ABC):
    cursor: sqlite3.Cursor
    connection: sqlite3.Connection
    name: str
    dataset_url: str
    schema: str

    def download(self) -> Optional[bool]:
        response = requests.get(self.dataset_url)
        with requests.get(self.dataset_url, stream = True) as response_stream:
            with open(f'{self.name}.tsv.gz', 'wb') as file:
                for chunk in response_stream.iter_content(chunk_size=8192):
                    file.write(chunk)
        return True

    def load_data_from_file(self) -> None:
        with gzip.open(f'{self.name}.tsv.gz', 'r') as file:
            skip = True
            for line in file:
                if skip: # skip header
                    skip = False
                    continue
                yield line.decode('utf-8').strip().split('\t')

    def drop(self) -> None:
        self.cursor.execute(f'DROP TABLE IF EXISTS {self.name}')
        self.connection.commit()

    def create(self) -> None:
        self.cursor.execute(f'CREATE TABLE {self.name} {self.schema}')
        self.connection.commit()

    def cleanup(self) -> None:
        if os.path.exists(f'{self.name}.tsv.gz'):
            os.remove(f'{self.name}.tsv.gz')

    @abstractmethod
    def insert(self) -> None:
        pass

class MoviesTable(Table):
    def insert(self) -> None:
        is_movie = lambda title: title[1] == 'movie'
        for title in filter(is_movie, self.load_data_from_file()):
            self.cursor.execute('INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?)', (title[0], title[2], title[3], title[4], title[5], title[7], title[8]))
        self.connection.commit()

class RatingsTable(Table):
    def insert(self) -> None:
        for title in self.load_data_from_file():
            self.cursor.execute('INSERT INTO ratings VALUES (?, ?, ?)', (title[0], title[1], title[2]))
        self.connection.commit()

class IMDB():

    def __init__(self, filename: str = 'resources/movies.db',) -> None:
        self.connection = sqlite3.connect(filename)
        self.cursor = self.connection.cursor()
        self.tables = (
            MoviesTable(
                cursor = self.cursor,
                connection = self.connection,
                name = 'movies',
                dataset_url = 'https://datasets.imdbws.com/title.basics.tsv.gz',
                schema = '(tconst TEXT, primary_title TEXT, original_title TEXT, is_adult INTEGER, year INTEGER, runtime INTEGER, genres TEXT)',
            ),
            RatingsTable(
                cursor = self.cursor,
                connection = self.connection,
                name = 'ratings',
                dataset_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz',
                schema = '(tconst TEXT, rating REAL, votes INTEGER)',
            ),
        )

    def update(self) -> None:
        for table in self.tables:
            if table.download():
                table.drop()
                table.create()
                table.insert()
            table.cleanup()

    def random_movies(self, ratings: int = 1000, number: int = 3) -> List:
        self.cursor.execute('SELECT * FROM movies NATURAL JOIN ratings WHERE votes > ? ORDER BY RANDOM() limit ?', (ratings, number))
        print(tuple(map(lambda movie: Movie(*movie).post_url(), self.cursor.fetchall())))

if __name__ == '__main__':
    imdb = IMDB()
    imdb.update()
    imdb.random_movies()
