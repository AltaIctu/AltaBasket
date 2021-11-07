from data_handler import Country, Season, Team, Game, Statistics


def seasons_boundaries(ctry_name):
    season_dict = {}
    for year in Country(ctry_name).years:
        start = Season(year, ctry_name).season_date("start")
        end = Season(year, ctry_name).season_date("end")
        season_dict[year] = [start, end]
    return season_dict

print(seasons_boundaries("pl"))
