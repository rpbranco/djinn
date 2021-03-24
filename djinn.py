#!/usr/bin/env python3

import re
import json
import random
import asyncio
import discord
import requests

from imdb import (IMDB, Movie)
from collections import defaultdict
from typing import (Dict, List, Union, Any, Optional, Tuple, Union, Callable)


def load(path: str) -> str:
    with open(path, 'r') as f:
        return f.read().strip()


class Request():
    operation: Tuple[str, int] = None
    rating: Tuple[str, float] = None
    votes: Tuple[str, int] = None
    duration: Tuple[str, int] = None
    genre: str = None

    @staticmethod
    def parse_parameter(
        parameter_name: str,
        message: str,
        amount_type: Callable = int,
    ) -> Tuple[str, Union[int, float]]:
        match = re.search(f'\(.*{parameter_name} *([=<>]) *(\d+(.\d+)?).*\)',
                          message)
        if match:
            return (match.group(1), amount_type(match.group(2)))
        return match

    def __init__(self, message: str) -> None:
        operation_match = re.search(r'(fetch|poll)( \d)', message)
        if not operation_match:
            return
        self.operation = (operation_match.group(1),
                          int(operation_match.group(2)))

        self.rating = Request.parse_parameter('rating', message, float)
        self.votes = Request.parse_parameter('votes', message)
        self.duration = Request.parse_parameter('duration', message)

        genre_match = re.search(r'\(.*genre *= *(\w+).*\)', message)
        if genre_match:
            self.genre = genre_match.group(1)

    def query(self) -> Dict:
        arguments = dict()
        if self.operation:
            arguments['amount'] = self.operation[1]
        if self.rating:
            arguments['rating'] = self.rating
        if self.votes:
            arguments['votes'] = self.votes
        if self.duration:
            arguments['duration'] = self.duration
        if self.genre:
            arguments['genre'] = self.genre
        return arguments


class Djinn(discord.Client):

    vote_emoji = b'\xf0\x9f\x91\x8d'.decode('utf-8')

    def __init__(
        self,
        movie_db: IMDB,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **options: Any,
    ) -> None:
        super().__init__(loop=loop, **options)
        self.movie_db = movie_db
        self.channels_with_polls = set()

    async def on_ready(self) -> None:
        print(f'{self.user} is ready to start working!')

    def format_movie_embed(self, movie: Movie) -> discord.Embed:
        poster_url = movie.poster_url()
        embed = discord.Embed(title=f'{movie.original_title} ({movie.year})',
                              description=movie.url,
                              color=0xe2b616)
        if poster_url not in (None, 'n/a', 'N/A'):
            embed.set_image(url=poster_url)

        embed.add_field(name='Rating', value=f'{movie.rating}/10')
        embed.add_field(name='Votes', value=f'{movie.votes}')
        embed.add_field(name='Duration', value=f'{movie.runtime} minutes')
        embed.add_field(name='Genres',
                        value=f'{movie.genres.replace(",", ", ")}')
        return embed

    def random_movie_embeds(
        self,
        **options: Any,
    ) -> List[discord.Embed]:
        movie_embeds: List[discord.Embed] = list()
        for movie in self.movie_db.random_movies(**options):
            embed = self.format_movie_embed(movie)
            movie_embeds.append(embed)
        return movie_embeds

    async def count_votes(
        self,
        channel: discord.abc.Messageable,
        messages: List[discord.message.Message],
    ) -> Dict[int, List[discord.message.Message]]:
        votes = defaultdict(list)
        for index, message in enumerate(messages):
            message = await channel.fetch_message(message.id)
            valid_reactions = filter(
                lambda reaction: reaction.emoji == Djinn.vote_emoji,
                message.reactions)
            reaction_count = next(valid_reactions).count
            votes[reaction_count].append(message)
        return votes

    async def wait_to_count_votes(
        minutes: int,
        channel: discord.abc.Messageable,
    ) -> None:
        await channel.send(
            f'I will wait {minutes} minutes before counting the votes.')
        await asyncio.sleep(minutes * 60)
        await channel.send('I will start counting the votes.')

    async def poll(self,
                   channel: discord.abc.Messageable,
                   options: Dict = {}) -> None:
        self.channels_with_polls.add(channel)
        await channel.send('Wait while I search my boundless library')

        messages = await self.publish_movies(channel=channel,
                                             options=options,
                                             reaction=Djinn.vote_emoji)

        await self.wait_to_count_votes(10)

        election_results = await self.count_votes(channel, messages)

        candidates = election_results[max(election_results)]
        result = candidates[0] if len(candidates) == 1 else random.choice(
            candidates)
        await result.reply('You shall watch this movie.')
        self.channels_with_polls.remove(channel)

    async def publish_movies(
            self,
            channel: discord.abc.Messageable,
            options: Dict = {},
            reaction: str = None) -> List[discord.message.Message]:
        movie_embeds: List[discord.Embed] = self.random_movie_embeds(**options)

        messages: List[discord.message.Message] = list()
        for embed in movie_embeds:
            message = await channel.send(embed=embed)
            if reaction is not None:
                await message.add_reaction(reaction)
            messages.append(message)
        return messages

    async def on_message(self, message: discord.message.Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        request = Request(message.content.lower())
        if request.operation:
            if message.channel in self.channels_with_polls:
                await message.channel.send('Stop spamming!')
            if request.operation[0] == 'fetch':
                await self.publish_movies(channel=message.channel,
                                          options=request.query())
            elif request.operation[0] == 'poll':
                await self.poll(message.channel, options=request.query())


if __name__ == '__main__':
    DISCORD_TOKEN = load('keys/discord_token')
    movie_db = IMDB()

    djinn = Djinn(movie_db)
    djinn.run(DISCORD_TOKEN)
