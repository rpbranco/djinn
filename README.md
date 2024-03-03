# Djinn

Djinn is a discord bot that provides an interface to interact with a movie database.

## Interactions

In order to interact with the bot, one must mention it.

| Operation | Explanation |
| --- | --- |
| fetch | gets a number of movies |
| poll | gets a number of movies and creates a poll |

The user can make use of parameters to restrict the search space.
Parameter values must be specified inside parenthesis.
They may appear in any order and the spacing is irrelevant.

| Parameter | Example |
| --- | --- |
| rating | `rating > 3` |
| votes | `votes > 1000` |
| duration | `duration > 0` |

### Grammar

```ebnf
<statement> ::= <condition> | <statement> " and " <condition> | <statement> " or " <condition> ;

<condition> ::=
  <search_parameter> " " <search_comparator> " " <number>
  | "genres = " <movie_genres>
;

<search_parameter> ::= "rating" | "votes" | "runtime" | "year" ;
<search_comparator> ::= "<=" | "<" | ">=" | ">" | "=" | "<>" ;

<movie_genres> ::=
  "Action" | "Adult" | "Adventure" | "Animation" | "Biography" | "Comedy" |
  "Crime" | "Documentary" | "Drama" | "Family" | "Fantasy" | "Film-Noir" |
  "Game-Show" | "History" | "Horror" | "Music" | "Musical" | "Mystery" |
  "News" | "Reality-TV" | "Romance" | "Sci-Fi" | "Short" | "Sport" |
  "Talk-Show" | "Thriller" | "War" | "Western"
;

<positive_digit> ::= "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" ;
<number> ::= <positive_digit> { <positive_digit> | "0"  } | "0"
```

### Examples

Obtain 3 random movies with a rating better than 3.

```
@Djinn fetch 3 (rating > 3)
```

Start a pool for 5 random movies with ratings better than 3 and more than 1000 votes.

```
@Djinn poll 5 (rating > 3 votes > 1000)
```
