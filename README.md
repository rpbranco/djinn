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

### Examples

Obtain 3 random movies with a rating better than 3.

```
@Djinn fetch 3 (rating > 3)
```

Start a pool for 5 random movies with ratings better than 3 and more than 1000 votes.

```
@Djinn poll 5 (rating > 3 votes > 1000)
```
