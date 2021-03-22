#!/usr/bin/env python3

import json
import random
import asyncio
import discord
import requests

from imdb import (IMDB, Movie)
from collections import defaultdict
from typing import (Dict, List, Union, Any, Optional)


def load(path: str) -> str:
    with open(path, 'r') as f:
        return f.read().strip()


class Djinn(discord.Client):
    def __init__(
        self,
        movie_db: IMDB,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **options: Any,
    ) -> None:
        super().__init__(loop=loop, **options)
        self.movie_db = movie_db
        self.vote_emoji = b'\xf0\x9f\x91\x8d'.decode('utf-8')
        self.star_emoji = b'\xe2\xad\x90'.decode('utf-8')
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

    def random_movie_embeds(self, amount: int = 3) -> List[discord.Embed]:
        movie_embeds: List[discord.Embed] = list()
        for movie in self.movie_db.random_movies(ratings=1000,
                                                 minimum_rating=0,
                                                 number=amount):
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
                lambda reaction: reaction.emoji == self.vote_emoji,
                message.reactions)
            reaction_count = tuple(
                map(lambda reaction: reaction.count, valid_reactions))[0]
            votes[reaction_count].append(message)
        return votes

    async def movie_poll(self, channel: discord.abc.Messageable):
        self.channels_with_polls.add(channel)
        await channel.send('Wait while I search my boundless library')

        movie_embeds: List[discord.Embed] = self.random_movie_embeds(5)

        messages: List[discord.message.Message] = list()
        for embed in movie_embeds:
            message = await channel.send(embed=embed)
            await message.add_reaction(self.vote_emoji)
            messages.append(message)

        await channel.send('I will wait 10 minutes before counting the votes.')
        await asyncio.sleep(600)
        await channel.send('I will start counting the votes.')

        election_results = await self.count_votes(channel, messages)

        candidates = election_results[max(election_results)]
        result = candidates[0] if len(candidates) == 1 else random.choice(
            candidates)
        await result.reply('You shall watch this movie.')
        self.channels_with_polls.remove(channel)

    async def on_message(self, message: discord.message.Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        if 'give me movies' in message.content.lower():
            if message.channel in self.channels_with_polls:
                await message.channel.send('Stop spamming!')
                return

            await self.movie_poll(message.channel)


if __name__ == '__main__':
    DISCORD_TOKEN = load('keys/discord_token')
    movie_db = IMDB()

    djinn = Djinn(movie_db)
    djinn.run(DISCORD_TOKEN)
