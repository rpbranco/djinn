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
from typing import (Dict, List, Union, Any, Optional, Tuple, Union, Callable,
                    Generator)


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
        match = re.search(f'(fetch|poll) (\d+)', raw_query)
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
        pattern = f'(fetch|poll|cancel)'
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

    @property
    def can_be_processed(self) -> bool:
        return not self.bot.is_channel_busy(self.channel)

    @abstractmethod
    async def process(self) -> None:
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

    def is_valid_amount(self) -> bool:
        return 0 < self.query.amount <= 10

    def random_movie_embeds(self, ) -> Generator[discord.Embed, None, None]:
        for movie in self.bot.movie_db.random_movies(**self.query.to_dict()):
            yield Command.format_movie_embed(movie)

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

    async def process(self) -> None:
        if not self.is_valid_amount():
            return await self.channel.send(
                'Please specify a number from 1 to 10.')

        await self.channel.send('Wait while I search my boundless library.')
        messages = await self.publish_movies()

        if not messages:
            return await self.channel.send(
                'Could not find anything matching your description.')

        if len(messages) != self.query.amount:
            return await self.channel.send('This is all I could find.')


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

    async def process(self) -> None:
        if not self.is_valid_amount():
            return await self.channel.send(
                'Please specify a number from 1 to 10.')

        await self.channel.send('Wait while I search my boundless library')

        messages = await self.publish_movies(reaction=self.bot.vote_emoji)
        await self.wait_to_count_votes(10)
        election_results = await self.count_votes(messages)
        await self.broadcast_winner(election_results)


class Cancel(Command):
    identifier: str = 'cancel'

    @property
    def can_be_processed(self) -> bool:
        return self.bot.is_channel_busy(self.channel)

    async def process(self) -> None:
        self.bot.deregister_command(self.channel)
        await self.channel.send('Command cancelled')


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
        self.running_commands: Dict[discord.abc.Messageable, Command] = dict()

    def is_channel_busy(self, channel: discord.abc.Messageable) -> bool:
        task = self.running_commands.get(channel)
        return task and not task.done()

    def register_command(
        self,
        channel: discord.abc.Messageable,
        task: asyncio.Task,
    ) -> None:
        if not self.is_channel_busy(channel):
            self.running_commands[channel] = task

    def deregister_command(
        self,
        channel: discord.abc.Messageable,
    ) -> None:
        if self.is_channel_busy(channel):
            self.running_commands.get(channel).cancel()

    async def on_ready(self) -> None:
        print(f'{self.user} is ready to start working!')

    async def on_message(self, message: discord.message.Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        command = Command.build(
            bot=self,
            channel=message.channel,
            message=message.content.lower(),
        )
        if not command:
            return

        if not command.can_be_processed:
            await message.reply('This command cannot be run.')
            return

        command_task = asyncio.create_task(command.process())
        self.register_command(message.channel, command_task)
        try:
            await command_task
        except asyncio.CancelledError:
            pass
        self.deregister_command(message.channel)


if __name__ == '__main__':
    DISCORD_TOKEN = load('keys/discord_token')
    movie_db = IMDB()

    djinn = Djinn(movie_db)
    djinn.run(DISCORD_TOKEN)
