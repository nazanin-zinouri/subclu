from ..utils import load_csv, aabb_analogies_from_tuples

def analogy_names():
    return ['uni_to_city', 'north_america_sports']

def analogy_tuples(name):
    if name == 'uni_to_city':
        return aabb_analogies_from_tuples(load_csv(__file__, "uni_to_city"))
    elif name == 'north_america_sports':
        return north_america_sports()
        
def north_america_sports():
    # in format: city,sport,team
    all_teams = list(load_csv(__file__, "north_america_sports"))

    # two types of analogies.
    # first, city->team as city->team (sport fixed)
    sports = set([team[1] for team in all_teams])
    for sport in sports:
        teams = [(team[2], team[0]) for team in all_teams if team[1] == sport]
        for analogy in aabb_analogies_from_tuples(teams):
            yield(analogy)

    # second, team->sport as team->sport (city fixed)
    cities = set([team[0] for team in all_teams])
    for city in cities:
        teams = [(team[2], team[1]) for team in all_teams if team[0] == city]
        if len(teams) > 1:
            for analogy in aabb_analogies_from_tuples(teams):
                yield(analogy)