from settings import SITE, HEADERS, regions, tournaments, seasons, matches, teams, players, managers, referees, venues, wait
from lxml import html
from urllib.parse import unquote
import requests
from datetime import date, time, datetime
from ast import literal_eval


def get_regions():
    url = '{0}/site/dropDownLaender'.format(SITE)

    r = requests.get(url, headers=HEADERS)
    print(r.url)

    if r.status_code != 200:
        wait()
        return False

    htmltext = literal_eval(r.text)

    content = html.fromstring(htmltext)
    values = content.xpath('option/@value')
    labels = content.xpath('option/text()')

    for label, value in zip(labels, values):
        regions.update_one({'region': int(value)}, {'$setOnInsert': {'name': label, 'type': False}}, upsert=True)

    regions.update_one({'region': 'fifa'}, {'$setOnInsert': {'name': 'FIFA', 'type': True}}, upsert=True)
    regions.update_one({'region': 'europa'}, {'$setOnInsert': {'name': 'Europe', 'type': True}}, upsert=True)
    regions.update_one({'region': 'asien'}, {'$setOnInsert': {'name': 'Asia', 'type': True}}, upsert=True)
    regions.update_one({'region': 'amerika'}, {'$setOnInsert': {'name': 'America', 'type': True}}, upsert=True)
    regions.update_one({'region': 'afrika'}, {'$setOnInsert': {'name': 'Africa', 'type': True}}, upsert=True)

    wait()


def get_tournaments(region_id):
    region = regions.find_one({'region': region_id})
    if region is None:
        return False

    urls = {
        False: '{0}/wettbewerbe/national/wettbewerbe/{region}',
        True: '{0}/wettbewerbe/{region}'
    }
    url = urls[region['type']].format(SITE, **region)

    r = requests.get(url, headers=HEADERS)
    print(r.url, region['name'])

    if r.status_code != 200:
        wait()
        return False

    content = html.fromstring(r.text)
    main = content.xpath('//div/table/tbody/tr/td[@class="hauptlink"]/table/tr/td[2]')
    side = content.xpath('//div[@class="box"]/div/li')

    # Competitions
    for row in side if region['type'] else main:
        tournaments.update_one({'tournament': unquote(row.xpath('a/@href')[0].split('/')[-1])},
                               {'$setOnInsert': {'name': row.xpath('a/@title')[0],
                                                 'region': region['region']}},
                               upsert=True)

    # National teams
    for row in list() if region['type'] else side:
        teams.update_one({'team': int(unquote(row.xpath('a/@href')[0].split('/')[-1]))},
                         {'$setOnInsert': {'name': row.xpath('a/@title')[0],
                                           'region': region['region'],
                                           'national': True}},
                         upsert=True)

    wait()


def get_seasons(tournament_id):
    tournament = tournaments.find_one({'tournament': tournament_id})
    if tournament is None:
        return False

    url = '{0}/wettbewerb/startseite/wettbewerb/{tournament}'.format(SITE, **tournament)

    r = requests.get(url, headers=HEADERS)
    print(r.url, tournament['name'])

    if r.status_code != 200:
        wait()
        return False

    if 'cup' not in tournament:
        tournament['cup'] = True if r.url.split('/')[-2] == 'pokalwettbewerb' else False
        tournaments.save(tournament)

    content = html.fromstring(r.text)
    for row in content.xpath('//div[@class="inline-select"]/select[@name="saison_id"]/option'):
        seasons.update_one({'tournament': tournament['tournament'], 'season': int(unquote(row.xpath('@value')[0]))},
                           {'$setOnInsert': {'name': row.xpath('text()')[0],
                                             'region': tournament['region']}},
                           upsert=True)

    wait()


def get_fixtures(tournament_id, season_id):
    season = seasons.find_one({'tournament': tournament_id, 'season': season_id})
    tournament = tournaments.find_one({'tournament': tournament_id})
    if tournament.get('cup') == 1:
        url = '{0}/spielplan/gesamtspielplan/pokalwettbewerb/{tournament}/saison_id/{season}'.format(SITE, **season)
    else:
        url = '{0}/spielplan/gesamtspielplan/wettbewerb/{tournament}/saison_id/{season}'.format(SITE, **season)
    r = requests.get(url, headers=HEADERS)
    print(r.url, tournament['name'], season['name'])

    if r.status_code != 200:
        wait()
        return False

    content = html.fromstring(r.text)
    datestamp, timestamp = date.min, time.min
    for row in content.xpath('//div[@class="box"]/table/tbody/tr[not(td/@colspan)]'):
        teams.update_one({'team': int(row.xpath('td[3]/a/@id')[0])},
                         {'$setOnInsert': {'name': row.xpath('td[3]/a/text()')[0],
                                           'region': tournament['region'],
                                           'national': False}},
                         upsert=True)
        teams.update_one({'team': int(row.xpath('td[7]/a/@id')[0])},
                         {'$setOnInsert': {'name': row.xpath('td[7]/a/text()')[0],
                                           'region': tournament['region'],
                                           'national': False}},
                         upsert=True)

        if row.xpath('td[2]/text()')[0].strip():
            timestamp = datetime.strptime(row.xpath('td[2]/text()')[0].strip(), '%I:%M %p').time()

        if row.xpath('td[1]/a/@href'):
            datestring = row.xpath('td[1]/a/@href')[0].split('/')[-1]
            if datestring == '0000-00-00':
                datestamp = datestamp.min
            else:
                datestamp = datetime.strptime(row.xpath('td[1]/a/@href')[0].split('/')[-1], '%Y-%m-%d')
        else:
            datestamp = datetime.strptime(row.xpath('td[1]/text()')[0].strip().split(' ')[-1], '%m/%d/%y')

        matches.update_one({'match': int(row.xpath('td[5]/a/@href')[0].split('/')[-1])},
                           {'$setOnInsert': {'season': season['season'],
                                             'tournament': tournament['tournament'],
                                             'region': tournament['region']},
                            '$set': {'date': datestamp,
                                     'time': datetime.combine(datestamp.date(), timestamp),
                                     'home': {'team': int(row.xpath('td[3]/a/@id')[0])},
                                     'away': {'team': int(row.xpath('td[7]/a/@id')[0])},
                                     'score': row.xpath('td[5]/a/text()')[0]}},
                           upsert=True)

    wait()


def get_lineups(match_id):
    match = matches.find_one({'match': match_id})
    url = '{0}/spielbericht/aufstellung/spielbericht/{match}'.format(SITE, **match)
    r = requests.get(url, headers=HEADERS)
    print(r.url)

    content = html.fromstring(r.text)
    tables = content.xpath('//div[@class="box"]')

    for table in tables:
        section = table.xpath('div/text()')[0].split(' ')[-1].lower()
        side = 'home' if int(table.xpath('div/a/@id')[0]) == match['home']['team'] else 'away'

        match[side][section] = list()
        for row in table.xpath('div/table[@class="items"]/tr'):
            if section == 'manager':
                manager_name = row.xpath('td[1]/table/tr/td[2]/a/@title')[0]
                manager_id = row.xpath('td[1]/table/tr/td[2]/a/@href')[0].split('/')[-1]
                manager_nationality = row.xpath('td[2]/img/@title')[0]

                managers.update_one({'manager': int(manager_id)},
                                    {'$setOnInsert': {'name': manager_name,
                                                      'nationality': manager_nationality}},
                                    upsert=True)
                match[side][section] = int(manager_id)

            else:
                player_position = row.xpath('td[1]/@title')[0]
                player_number = row.xpath('td[1]/div/text()')[0]
                player_name = row.xpath('td[2]/table/tr/td[2]/a/@title')[0]
                player_id = row.xpath('td[2]/table/tr/td[2]/a/@id')[0]
                player_nationality = row.xpath('td[3]/img/@title')[0]

                players.update_one({'player': int(player_id)},
                                   {'$setOnInsert': {'name': player_name,
                                                     'nationality': player_nationality}},
                                   upsert=True)

                match[side][section].append({'player': int(player_id),
                                              'position': player_position,
                                              'number': player_number})

    for venue in content.xpath('//div[@class="sb-spieldaten"]/p[3]/span/a'):
        venues.update_one({'venue': int(venue.xpath('@href')[0].split('/')[-3])},
                          {'$setOnInsert': {'name': venue.xpath('text()')[0]}},
                          upsert=True)
        match['venue'] = int(venue.xpath('@href')[0].split('/')[-3])

    for attendance in content.xpath('//div[@class="sb-spieldaten"]/p[3]/span/strong/text()'):
        match['attendance'] = int(attendance.replace(' Spectators', '').replace('.', ''))

    for referee in content.xpath('//div[@class="sb-spieldaten"]/p[3]/a'):
        referees.update_one({'referee': int(referee.xpath('@href')[0].split('/')[-1])},
                            {'$setOnInsert': {'name': referee.xpath('@title')[0]}},
                            upsert=True)
        match['referee'] = int(referee.xpath('@href')[0].split('/')[-1])

    matches.save(match)

    wait()

if __name__ == '__main__':
    get_regions()
    for region in regions.find().sort('name'):
        get_tournaments(region['region'])
    for tournament in tournaments.find().sort('tournament'):
        get_seasons(tournament['tournament'])
    for season in seasons.find().sort([('season', -1), ('tournament', 1)]).batch_size(1):
        get_fixtures(season['tournament'], season['season'])
    for match in matches.find().sort('date', -1):
        get_lineups(match['match'])
