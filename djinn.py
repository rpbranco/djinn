#!/usr/bin/env python3

import re
import json
import random
import asyncio
import discord
import requests

from imdb import (IMDB, Movie)
from collections import defaultdict
from abc import (ABC, abstractmethod)
from typing import (Dict, List, Union, Any, Optional, Tuple, Union, Callable)


def load(path: str) -> str:
    with open(path, 'r') as f:
        return f.read().strip()


class Query():
    @staticmethod
    def parse_limit(
            parameter_name: str,
            raw_query: str,
            limit_type: Callable = int,
            default: Tuple[str, int] = ('>', 0),
    ) -> Tuple[str, Any]:
        pattern = f'\(.*{parameter_name} *([=<>]) *(\d+(.\d+)?).*\)'
        match = re.search(pattern, raw_query)
        if match:
            return (match.group(1), limit_type(match.group(2)))
        return default

    @staticmethod
    def parse_genre(raw_query: str, default: str = '') -> str:
        match = re.search(r'\(.*genre *= *(\w+).*\)', raw_query)
        if match:
            return match.group(1)
        return default

    @staticmethod
    def parse_amount(raw_query: str, default: str = 3) -> int:
        match = re.search(f'(fetch|poll) (\d)', raw_query)
        if match:
            return int(match.group(2))
        return default

    def __init__(self, raw_query: str = '') -> None:
        self.amount = Query.parse_amount(raw_query)
        self.rating = Query.parse_limit('rating', raw_query, float)
        self.votes = Query.parse_limit('votes', raw_query, int)
        self.duration = Query.parse_limit('duration', raw_query, int)
        self.genre = Query.parse_genre(raw_query)

    def to_dict(self) -> Dict:
        return self.__dict__.copy()


class Command(ABC):
    @staticmethod
    def parse_command_identifier(message: str) -> str:
        # TODO: use class identifier to update pattern
        pattern = f'(fetch|poll) \d'
        match = re.search(pattern, message)
        if match:
            return match.group(1)
        return match

    @classmethod
    def build(
        cls,
        bot: discord.Client,
        channel: discord.abc.Messageable,
        message: str,
    ) -> Optional['Command']:
        identifier = Command.parse_command_identifier(message)
        for subclass in cls.__subclasses__():
            if identifier == subclass.identifier:
                return subclass(bot, channel, Query(message))
        return None

    def __init__(
            self,
            bot: discord.Client,
            channel: discord.abc.Messageable,
            query: Query = Query(),
    ) -> None:
        self.bot = bot
        self.channel = channel
        self.query = query

    @abstractmethod
    async def process(self):
        pass

    @staticmethod
    def format_movie_embed(movie: Movie) -> discord.Embed:
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

    def random_movie_embeds(self, ) -> List[discord.Embed]:
        movie_embeds: List[discord.Embed] = list()
        for movie in self.bot.movie_db.random_movies(**self.query.to_dict()):
            embed = Command.format_movie_embed(movie)
            movie_embeds.append(embed)
        return movie_embeds

    async def publish_movies(
        self,
        reaction: str = None,
    ) -> List[discord.message.Message]:
        movie_embeds: List[discord.Embed] = self.random_movie_embeds()

        messages: List[discord.message.Message] = list()
        for embed in movie_embeds:
            message = await self.channel.send(embed=embed)
            if reaction:
                await message.add_reaction(reaction)
            messages.append(message)
        return messages


class Fetch(Command):
    identifier: str = 'fetch'

    async def process(self, bot, channel):
        if not self.bot.register_channel(self.channel):
            return await self.channel.send('Stop spamming.')

        await self.publish_movies()

        self.bot.delist_channel(self.channel)


class Poll(Command):
    identifier: str = 'poll'

    async def count_votes(
        self,
        messages: List[discord.message.Message],
    ) -> Dict[int, List[discord.message.Message]]:
        votes = defaultdict(list)
        for index, message in enumerate(messages):
            message = await self.channel.fetch_message(message.id)
            valid_reactions = filter(
                lambda reaction: reaction.emoji == Djinn.vote_emoji,
                message.reactions)
            reaction_count = next(valid_reactions).count
            votes[reaction_count].append(message)
        return votes

    async def broadcast_winner(
        self,
        election_results: Dict[int, List[discord.message.Message]],
    ) -> None:
        candidates = election_results[max(election_results)]
        result = random.choice(candidates)
        await result.reply('You shall watch this movie.')

    async def wait_to_count_votes(self, minutes: int) -> None:
        await self.channel.send(
            f'I will wait {minutes} minutes before counting the votes.')
        await asyncio.sleep(minutes * 60)
        await self.channel.send('I will start counting the votes.')

    async def process(self, bot, channel):
        if not self.bot.register_channel(self.channel):
            return await self.channel.send('Stop spamming.')

        await self.channel.send('Wait while I search my boundless library')

        messages = await self.publish_movies(reaction=bot.vote_emoji)
        await self.wait_to_count_votes(10)
        election_results = await self.count_votes(messages)
        await self.broadcast_winner(election_results)

        self.bot.delist_channel(self.channel)


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
        self.busy_channels = set()

    async def on_ready(self) -> None:
        print(f'{self.user} is ready to start working!')

    def register_channel(self, channel: discord.abc.Messageable) -> bool:
        if channel not in self.busy_channels:
            self.busy_channels.add(channel)
            return True
        return False

    def delist_channel(self, channel: discord.abc.Messageable) -> bool:
        if channel in self.busy_channels:
            self.busy_channels.remove(channel)
            return True
        return False

    async def on_message(self, message: discord.message.Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        command = Command.build(
            bot=self,
            channel=message.channel,
            message=message.content.lower(),
        )
        if command:
            await command.process(bot=self, channel=message.channel)


if __name__ == '__main__':
    DISCORD_TOKEN = load('keys/discord_token')
    movie_db = IMDB()

    djinn = Djinn(movie_db)
    djinn.run(DISCORD_TOKEN)
