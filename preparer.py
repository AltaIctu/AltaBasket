from data_handler import Country, Season, Team, Game, Statistics
import pandas as pd

def seasons_boundaries(ctry_name):
    season_dict = {}
    for year in Country(ctry_name).years:
        start = Season(year, ctry_name).season_date("start")
        end = Season(year, ctry_name).season_date("end")
        season_dict[year] = [start, end]
    return season_dict

def metadata_returner(meta):
    return meta["HOME"], meta["AWAY"], meta["DATE"], meta["SCORE_HOME"], meta["SCORE_AWAY"]

def split_game(df, home_team_name, away_team_name):
    df_t1_mask = df["TEAM"] == home_team_name
    df_t2_mask = df["TEAM"] == away_team_name
    return df[df_t1_mask], df[df_t2_mask]

# print(seasons_boundaries("pl"))
game_num = 0
year = 2015
ctry_name = "pl"
game_list = Season(year,"pl").games_list()
num_games_back = 2
# Metadata for single game
def get_single_team_zipped():
    single_game_meta = game_list.iloc[game_num]
    home_team_name, away_team_name, date, home_score, away_score  = metadata_returner(single_game_meta)
    team_obj = Team(year,ctry_name,home_team_name)
#   2 games home, 2 away version
    home_last_games = team_obj.last_x_games_ha(home_team_name, num_games_back, date, "home")
    away_last_games = team_obj.last_x_games_ha(home_team_name, num_games_back, date, "away")
    all_games = pd.concat([home_last_games, away_last_games]).reset_index(drop=True)
    mnums = sorted(list(home_last_games["MNUM"].drop_duplicates()) + list(away_last_games["MNUM"].drop_duplicates()))

    def zip_games_map(team_name, mnum):
        single_game_mask = all_games["MNUM"] == mnum
        single_game = all_games[single_game_mask]
        return Game(year, ctry_name, team_name, date).zip_team(single_game)

    def pts_sum_map(zipped_df):
        temp_date = zipped_df["DATE"].iloc[0]
        temp_name = zipped_df["TEAM"].iloc[0]
        temp_team_obj = Team(year, ctry_name, temp_name)
        pts_gained = temp_team_obj.pts_sum(temp_date, "gained", "all")
        pts_lost = temp_team_obj.pts_sum(temp_date, "loss", "all")
        zipped_df["PTS_GAINED"] = pts_gained
        zipped_df["PTS_LOST"] = pts_lost
        return zipped_df




# Pts sum
# pts_gained_so_far = team_obj.pts_sum(date,"gained","all")
# pts_lost_so_far = team_obj.pts_sum(date,"loose","all")



