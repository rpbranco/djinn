#!/usr/bin/env python3

import os
import re
import gzip
import logging
import sqlite3
import requests

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Generator, List, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dataclass
class Movie:
    tconst: str
    primary_title: str
    original_title: str
    is_adult: int
    year: int
    runtime: int
    genres: str
    rating: float
    votes: int

    @property
    def url(self) -> str:
        return f"https://www.imdb.com/title/{self.tconst}/"

    def poster_url(self) -> Optional[str]:
        headers = {
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        }
        response = requests.get(self.url, headers=headers)
        poster_url_pattern = r'\s*"image":\s*"(https:\/\/.+?\.jpg)",'
        html_content = response.content.decode("utf-8")
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
        # response = requests.get(self.dataset_url)
        with requests.get(self.dataset_url, stream=True) as response_stream:
            with open(f"{self.name}.tsv.gz", "wb") as file:
                for chunk in response_stream.iter_content(chunk_size=8192):
                    file.write(chunk)
        return True

    def load_data_from_file(self) -> Generator[List[str], Any, Any]:
        with gzip.open(f"{self.name}.tsv.gz", "r") as file:
            skip = True
            for line in file:
                if skip:  # skip header
                    skip = False
                    continue
                yield line.decode("utf-8").strip().split("\t")

    def drop(self) -> None:
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.name}")
        self.connection.commit()

    def create(self) -> None:
        self.cursor.execute(f"CREATE TABLE {self.name} {self.schema}")
        self.connection.commit()

    def cleanup(self) -> None:
        if os.path.exists(f"{self.name}.tsv.gz"):
            os.remove(f"{self.name}.tsv.gz")

    @abstractmethod
    def insert(self) -> None:
        pass


class MoviesTable(Table):
    def insert(self) -> None:
        is_movie = lambda title: title[1] == "movie"
        for title in filter(is_movie, self.load_data_from_file()):
            self.cursor.execute(
                "INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?)",
                (title[0], title[2], title[3], title[4], title[5], title[7], title[8]),
            )
        self.connection.commit()


class RatingsTable(Table):
    def insert(self) -> None:
        for title in self.load_data_from_file():
            self.cursor.execute(
                "INSERT INTO ratings VALUES (?, ?, ?)", (title[0], title[1], title[2])
            )
        self.connection.commit()


class IMDB:
    def __init__(
        self,
        filename: str = "resources/movies.db",
    ) -> None:
        self.connection = sqlite3.connect(filename)
        self.cursor = self.connection.cursor()
        self.tables = (
            MoviesTable(
                cursor=self.cursor,
                connection=self.connection,
                name="movies",
                dataset_url="https://datasets.imdbws.com/title.basics.tsv.gz",
                schema="(tconst TEXT, primary_title TEXT, original_title TEXT, is_adult INTEGER, year INTEGER, runtime INTEGER, genres TEXT)",
            ),
            RatingsTable(
                cursor=self.cursor,
                connection=self.connection,
                name="ratings",
                dataset_url="https://datasets.imdbws.com/title.ratings.tsv.gz",
                schema="(tconst TEXT, rating REAL, votes INTEGER)",
            ),
        )

    def update(self) -> None:
        for table in self.tables:
            if table.download():
                table.drop()
                table.create()
                table.insert()
            table.cleanup()

    def random_movies(
        self,
        amount: int = 3,
        rating: Tuple[str, int] = (">", 0),
        votes: Tuple[str, int] = (">", 0),
        duration: Tuple[str, int] = (">", 0),
        year: Tuple[str, int] = (">", 0),
        genre: str = "",
    ) -> Generator[Movie, None, None]:
        # NOTE: rating, votes and duration must have a >, < or = on their first position
        for movie_data in self.connection.execute(
            f"""
                SELECT * FROM movies NATURAL JOIN ratings
                WHERE rating {rating[0]} ? AND votes {votes[0]} ? AND runtime {duration[0]} ? AND year {year[0]} ? AND genres LIKE '%'||?||'%'
                ORDER BY RANDOM()
                LIMIT ?
                """,
            (rating[1], votes[1], duration[1], year[1], genre, amount),
        ):
            yield Movie(*movie_data)

    def count_movies(
        self,
        amount: int = 3,
        rating: Tuple[str, int] = (">", 0),
        votes: Tuple[str, int] = (">", 0),
        duration: Tuple[str, int] = (">", 0),
        year: Tuple[str, int] = (">", 0),
        genre: str = "",
    ) -> Generator[Movie, None, None]:
        # NOTE: rating, votes and duration must have a >, < or = on their first position
        self.cursor.execute(
            f"""
                SELECT COUNT() FROM movies NATURAL JOIN ratings
                WHERE rating {rating[0]} ? AND votes {votes[0]} ? AND runtime {duration[0]} ? AND year {year[0]} ? AND genres LIKE '%'||?||'%'
                ORDER BY RANDOM()
                LIMIT ?
                """,
            (rating[1], votes[1], duration[1], year[1], genre, amount),
        )
        return self.cursor.fetchone()[0]

    def count_movies_2(
        self,
        constraints: Optional[List[List[Tuple[str, str, str]]]],
    ) -> Optional[int]:
        if constraints is None:
            self.cursor.execute(
                f"""
                SELECT COUNT() FROM movies NATURAL JOIN ratings
                """,
            )
            return self.cursor.fetchone()[0]

        where_clause = format_constraints(constraints)
        if where_clause is None:
            return None

        clause, values = where_clause
        logger.info(f"{clause}, {values}")

        self.cursor.execute(
            f"""
            SELECT COUNT() FROM movies NATURAL JOIN ratings
            WHERE {clause}
            """,
            values,
        )
        return self.cursor.fetchone()[0]


    def random_movies_2(
        self,
        amount: str,
        constraints: Optional[List[List[Tuple[str, str, str]]]] = None,
    ) -> Generator[Movie, None, None]:

        amount = amount.strip()
        if not amount.isdigit():
            amount = "3"

        query = f"""
                SELECT * FROM movies NATURAL JOIN ratings
                ORDER BY RANDOM()
                LIMIT ?
                """
        values = [amount]

        if constraints is not None:
            where_clause = format_constraints(constraints)
            if where_clause is None:
                return None

            clause, clause_values = where_clause
            logger.info(f"{clause}, {values}")

            query = f"""
                    SELECT * FROM movies NATURAL JOIN ratings
                    WHERE {clause}
                    ORDER BY RANDOM()
                    LIMIT ?
                    """

            values = clause_values + values
            logger.info(values)

        for movie_data in self.connection.execute(query, values):
            yield Movie(*movie_data)

def format_constraints(
    constraints: List[List[Tuple[str, str, str]]],
) -> Optional[Tuple[str, List[str]]]:

    allowed_columns = {
        "rating",
        "votes",
        "runtime",
        "year",
        "genres",
    }

    allowed_operations = {
        "<=",
        "<",
        ">=",
        ">",
        "=",
        "<>",
    }

    values = []
    or_clauses = []
    for constraint in constraints:

        and_clauses = []
        for column, operation, value in constraint:
            if column not in allowed_columns or operation not in allowed_operations:
                return None


            if column == "genres":
                if operation != "=":
                    return None

                operation = "like"
                value = f"%{value}%"

            and_clauses.append(f"{column} {operation} ?")
            values.append(value)

        or_clauses.append(" and ".join(and_clauses))

    return " or ".join(or_clauses), values


if __name__ == "__main__":
    imdb = IMDB()
    # imdb.update()
    # imdb.random_movies()
    # print(imdb.count_movies(rating=('>', 10)))
