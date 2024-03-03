#!/usr/bin/env python3

import random
import logging
import asyncio

from imdb import IMDB, Movie

from discord.abc import Messageable
from discord.message import Message
from discord import Client, Intents, Embed

from collections import defaultdict
from typing import Optional, List, Tuple, Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Djinn(Client):

    def init(self):
        self.movie_db = IMDB()
        # TODO: decorator to populate dict
        self.commands = {
            "fetch": self.fetch,
            "count": self.count,
            "poll": self.poll,
            "cancel": self.cancel,
        }

        self.calls: Dict[int, asyncio.Task] = {}

    async def on_ready(self) -> None:
        logger.info(f'{self.user} is ready to start working!')

    async def on_message(self, message: Message) -> None:
        if message.author == self.user or self.user not in message.mentions:
            return

        message_content = clean_message(message.content, message.raw_mentions)
        logger.debug(message.channel.id, message_content)

        # <mention> <command> <arguments>:
        # @Djinn count where rating > 8 and genre = Comedy or rating > 9
        # => ((("rating", ">", "9"), ("genre", "=", "commedy")), ("rating", ">", "9"))
        fn_name, _, arguments = message_content.strip().partition(" ")

        await self.execute(fn_name, message.channel, arguments)
        # await fn(message.channel, arguments)

    async def on_message_edit(self, _: Message, message_after: Message) -> None:
        await self.on_message(message_after)

    async def execute(self, fn_name: str, channel: Messageable, arguments: str):
        fn = self.commands.get(fn_name, None)
        if fn is None:
            await channel.send(f'I do not recognize the command "{fn_name}"')
            return

        if fn_name == "cancel":
            await fn(channel, arguments)
            return

        task = self.calls.get(channel.id, None)
        if task is not None and not task.done():
            await channel.send(f'Cannot run "{fn_name}", one wish at a time')
            return

        self.calls[channel.id] = asyncio.create_task(fn(channel, arguments))

    async def display_movies(
        self,
        channel: Messageable,
        amount: str,
        parsed_constraints: Optional[List[List[Tuple[str, str, str]]]],
    ) -> List[Message]:
        messages = []
        for movie in self.movie_db.random_movies_2(amount, parsed_constraints):
            movie_embed = to_embed(movie)
            message = await channel.send(embed=movie_embed)
            messages.append(message)
        return messages

    async def cancel(self, channel: Messageable, arguments: str) -> None:
        task = self.calls.get(channel.id, None)
        if task is None or task.done():
            await channel.send(f"I am not doing anything...")
            return

        task.cancel()
        await channel.send(f"Stop? Seriously? Just when the party was getting started! Alright, spill your wishes then.")

    async def fetch(self, channel: Messageable, arguments: str):
        amount, _, constraints = arguments.partition("where")
        parsed_constraints = parse_search_terms(constraints.strip())
        await channel.send(f"Here are some movies that even I can't believe are in my collection. Don't blame me if it gets weird!")
        await self.display_movies(channel, amount, parsed_constraints)

    async def count(self, channel: Messageable, arguments: str):
        _, _, constraints = arguments.partition("where")
        parsed_constraints = parse_search_terms(constraints)
        amount = self.movie_db.count_movies_2(parsed_constraints)

        if amount is None:
            await channel.send(f"The provided constraints are not properly formatted")
            return

        await channel.send(f"Found {amount} movies!")

    async def poll(self, channel: Messageable, arguments: str):
        amount, _, constraints = arguments.partition("where")
        parsed_constraints = parse_search_terms(constraints.strip())

        await channel.send(f"Here are some movies that even I can't believe are in my collection. Don't blame me if it gets weird!")
        messages = await self.display_movies(channel, amount, parsed_constraints)
        vote_emoji = b'\xf0\x9f\x91\x8d'.decode('utf-8')
        await add_reaction(messages, vote_emoji)

        wait_m = 10
        await channel.send(f'I will wait {wait_m} minutes before counting the votes.')
        await asyncio.sleep(wait_m * 60)
        await channel.send(f'Counting the votes...')

        messages = await update_messages(messages)
        reaction_count = count_reactions(messages, vote_emoji)

        candidate_messages = reaction_count[max(reaction_count)]
        winner_message = random.choice(candidate_messages)
        winner_embeds = winner_message.embeds
        message_text = "You shall watch this movie."
        if len(winner_embeds) == 1:
            winner_title = winner_embeds[0].title
            message_text = f"You shall watch ***{winner_title}***."
        await winner_message.reply(message_text)


async def update_messages(messages: List[Message]) -> List[Message]:
    updated_messages = []
    for message in messages:
        message = await message.channel.fetch_message(message.id)
        updated_messages.append(message)
    return updated_messages

def get_reaction_count(message: Message, emoji: str) -> int:
    for message_reaction in message.reactions:
        if message_reaction.emoji == emoji:
            return message_reaction.count
    return 0

def count_reactions(messages: List[Message], emoji: str):
    count = defaultdict(list)
    for message in messages:
        reaction_count = get_reaction_count(message, emoji)
        count[reaction_count].append(message)
    return count

async def add_reaction(messages: List[Message], reaction: str):
    for message in messages:
        await message.add_reaction(reaction)

def to_embed(movie: Movie) -> Embed:
    poster_url = movie.poster_url()
    movie_embed = Embed(title=f'{movie.original_title} ({movie.year})',
                          description=movie.url,
                          color=0xe2b616)
    if poster_url not in (None, 'n/a', 'N/A'):
        movie_embed.set_image(url=poster_url)

    movie_embed.add_field(name='Rating', value=f'{movie.rating}/10')
    movie_embed.add_field(name='Votes', value=f'{movie.votes}')
    movie_embed.add_field(name='Duration', value=f'{movie.runtime} minutes')
    movie_embed.add_field(name='Genres',
                    value=f'{movie.genres.replace(",", ", ")}')
    return movie_embed

def clean_message(content: str, raw_mentions: List[int]) -> str:
    for raw_mention in raw_mentions:
        content = content.replace(f"<@{raw_mention}>", "").strip()
    return content

def parse_search_terms(text: str) -> Optional[List[List[Tuple[str, str, str]]]]:
    # NOTE: parenthesis are not allowed therefore we can rely on the default
    # operator precedence.
    search_terms = list()
    for conditions in text.split(" or "):

        local_search_terms = list()
        for condition in conditions.split(" and "):
            tokens = condition.split()
            if len(tokens) != 3:
                return None

            local_search_terms.append(tuple(tokens))

        if len(local_search_terms) == 0:
            return None

        search_terms.append(local_search_terms)

    return search_terms


def load(path: str) -> str:
    with open(path, 'r') as f:
        return f.read().strip()

def setup_logging(level: int):
    module_names = ("__main__", "imdb")
    handlers = []

    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s: %(message)s')
    for module_name in module_names:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.addFilter(logging.Filter(module_name))
        handlers.append(handler)

    logging.basicConfig(level=level, handlers=handlers)

if __name__ == '__main__':
    setup_logging(logging.INFO)

    DISCORD_TOKEN = load('keys/discord_token')

    intents = Intents.default()
    intents.message_content = True

    djinn = Djinn(intents=intents)

    djinn.init()
    djinn.run(DISCORD_TOKEN, log_handler=None)
