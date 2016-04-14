from pymongo import MongoClient
from time import sleep
from random import random


def wait(delay=2, variation=1):
    m, x, c = variation, random(), delay - variation / 2
    sleep(m * x + c)

SITE = "http://www.transfermarkt.co.uk/"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

client = MongoClient()

if client:
    regions = client.transfermarkt.regions
    tournaments = client.transfermarkt.tournaments
    seasons = client.transfermarkt.seasons
    matches = client.transfermarkt.matches
    teams = client.transfermarkt.teams
    players = client.transfermarkt.players
    managers = client.transfermarkt.managers
