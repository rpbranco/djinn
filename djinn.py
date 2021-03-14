#!/usr/bin/env python3

import json
import random
import asyncio
import discord
import requests

from collections import defaultdict
from typing import (Dict, List, Union, Any, Optional)


def load(path: str) -> str:
    with open(path, 'r') as f:
        return f.read().strip()


class MovieDB():
    def __init__(
        self,
        url: str,
        imdb_ids_path: str = 'resources/imdb_movie_ids',
        **default_parameters: str,
    ) -> None:
        self.url = url
        self.default_parameters = default_parameters
        self.imdb_ids = self._load_imdb_ids(imdb_ids_path)

    def _load_imdb_ids(self, path: str) -> List[str]:
        with open(path, 'r') as f:
            return f.read().split()

    def request(self, **parameters) -> Dict:
        parameters.update(self.default_parameters)
        response = requests.get(self.url, params=parameters)
        return response.json()

    def get_random_movie(self) -> Dict:
        movie: Dict = None
        while not movie or movie.get('Response') == 'False':
            imdb_id = random.choice(self.imdb_ids)
            movie = self.request(i=imdb_id)
        return movie


class Djinn(discord.Client):
    def __init__(
        self,
        movie_db: MovieDB,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **options: Any,
    ) -> None:
        super().__init__(loop=loop, **options)
        self.movie_db = movie_db
        self.vote_emoji = b'\xf0\x9f\x91\x8d'.decode('utf-8')

    async def on_ready(self) -> None:
        print(f'{self.user} is ready to start working!')

    def integer_to_emoji(self, integer: int) -> str:
        encoded_emoji = str(integer).encode(
            'utf-8') + b'\xef\xb8\x8f\xe2\x83\xa3'
        return encoded_emoji.decode('utf-8')

    def emoji_to_integer(self, emoji: str) -> int:
        return int(emoji[0])

    def format_movie_embed(self, movie_metadata: Dict) -> discord.Embed:
        title = movie_metadata.get('Title')
        year = movie_metadata.get('Year')
        imdb_id = movie_metadata.get('imdbID')
        poster_url = movie_metadata.get('Poster')
        embed = discord.Embed(
            title=f'{title} ({year})',
            description=f'https://www.imdb.com/title/{imdb_id}',
            color=0xff00ff)
        if poster_url not in (None, 'n/a', 'N/A'):
            embed.set_image(url=poster_url)
        return embed

    def random_movie_embeds(self, amount: int = 3) -> List[discord.Embed]:
        movie_embeds: List[discord.Embed] = list()
        for index in range(amount):
            movie_metadata = self.movie_db.get_random_movie()
            embed = self.format_movie_embed(movie_metadata)
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

    async def start_movie_poll(self, channel: discord.abc.Messageable):
        await channel.send('Wait while I search my boundless library')

        movie_embeds: List[discord.Embed] = self.random_movie_embeds(3)

        messages: List[discord.message.Message] = list()
        for embed in movie_embeds:
            message = await channel.send(embed=embed)
            await message.add_reaction(self.vote_emoji)
            messages.append(message)

        await channel.send('I will wait 10 minutes before counting the votes.')
        await asyncio.sleep(10)
        await channel.send('I will start counting the votes.')

        election_results = await self.count_votes(channel, messages)

        candidates = election_results[max(election_results)]
        result = candidates[0] if len(candidates) == 1 else random.choice(
            candidates)
        await result.reply('You shall watch this movie.')

    async def on_message(self, message: discord.message.Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        if 'give me movies' in message.content.lower():
            await self.start_movie_poll(message.channel)


if __name__ == '__main__':
    DISCORD_TOKEN = load('keys/discord_token')
    OMDB_API_KEY = load('keys/omdb_api_key')
    movie_db = MovieDB('https://www.omdbapi.com/', apikey=OMDB_API_KEY)

    djinn = Djinn(movie_db)
    djinn.run(DISCORD_TOKEN)
