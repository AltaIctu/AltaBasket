import sqlite3
import pandas as pd
import numpy as np


class SqlRequest:
    """ Connect with db, make req and get respond"""

    def __init__(self, request):
        self.conn = sqlite3.connect(f'scores_data.db')
        self.c = self.conn.cursor()
        self.respond = self.c.execute(request)

class Season:
    """ year - pass the year when season starts,
        country - pass country shortcut which is processing"""

    import pandas as pd
    import numpy as np

    def __init__(self, year, country):
        self.year = year
        self.country = country
        self.start_date = self.season_date("start")
        self.end_date = self.season_date("end")
        self.columns = [i[0] for i in pd.read_csv(f"columns_{self.country}.csv").iloc]

    def numpy_df(self, respond, numpy=False):
        if not numpy:
            return Season.pd.DataFrame(respond, columns=self.columns)
        else:
            return Season.np.array(respond)

    def season_date(self, which):

        data_dict = {"start": [f"{self.year}-05-01", ">", min],
                     "end": [f"{self.year + 1}-07-01", "<", max]}

        date = data_dict[which][0]
        sign = data_dict[which][1]
        get_f = data_dict[which][2]

        c = SqlRequest(f''' SELECT DATE FROM scores_{self.country} WHERE DATE {sign} "{date}" ''')
        respond = c.respond.fetchall()
        assert len(respond) > 0, "Returned date has 0 length. Check if year is correct."
        return get_f(respond)[0]

    def single_game(self, mnum, numpy=False):
        game_respond = SqlRequest(f''' SELECT * from scores_{self.country} 
        WHERE MNUM  = {mnum}''').respond.fetchall()
        return self.numpy_df(game_respond, numpy)

    def games_df(self, numpy=False):
        games = SqlRequest(f''' SELECT * from scores_{self.country} 
        WHERE MNUM IN {self.mnums}''').respond.fetchall()
        return self.numpy_df(games, numpy)

    @property
    def teams(self):
        teams_respond = SqlRequest(f''' SELECT DISTINCT TEAM from scores_{self.country} 
        WHERE DATE BETWEEN  date('{self.start_date}') and date('{self.end_date}')''').respond.fetchall()
        return [i[0] for i in teams_respond]

    @property
    def mnums(self):
        mnums_resp = SqlRequest(f''' SELECT DISTINCT MNUM from scores_{self.country} 
        WHERE DATE BETWEEN  date('{self.start_date}') and date('{self.end_date}')''').respond.fetchall()
        return tuple(i[0] for i in mnums_resp)

    @property
    def benjamins(self):
        previous_season_teams = Season(self.year - 1, self.country).teams
        return [i for i in self.teams if i not in previous_season_teams]

    @property
    def games_count(self):
        return len(set(self.mnums))


class Team(Season):

    def __init__(self, year, country, name):
        super().__init__(year, country)
        self.name = name

    @property
    def mnums(self):
        team_mnums = SqlRequest(f''' SELECT DISTINCT MNUM FROM scores_{self.country} 
        WHERE TEAM = "{self.name}" ''').respond.fetchall()
        season_mnums = super().mnums
        return tuple(i[0] for i in team_mnums if i[0] in season_mnums)

    @property
    def games_count(self):
        return len(set(self.mnums))

    def games_dates(self, mnums=False):
        if not mnums:
            games = self.games_df()["DATE"].drop_duplicates().sort_values()
        else:
            games = self.games_df()[["DATE", "MNUM"]].drop_duplicates().sort_values("DATE")
        return games.reset_index(drop=True)

    def games_df(self, numpy=False):
        games_respond = SqlRequest(f''' SELECT * FROM scores_{self.country}
        WHERE MNUM IN {self.mnums} ''').respond.fetchall()
        return self.numpy_df(games_respond, numpy)

    def last_x_mnums_before(self, name, x, date):
        self.date_checker(self.year, date)
        mnums_before_date = SqlRequest(f''' SELECT MNUM FROM scores_{self.country}
        WHERE DATE < date("{date}") AND TEAM = "{name}"''').respond.fetchall()
        mnums_before_date = sorted(set(mnums_before_date))[-x:]
        return tuple(i[0] for i in mnums_before_date)

    def last_x_games_all(self, name, x, date, numpy=False):
        mnums_before_date = self.last_x_mnums_before(name, x, date)
        games_respond = SqlRequest(f''' SELECT * FROM scores_{self.country}
        WHERE MNUM IN {mnums_before_date} ''').respond.fetchall()
        return self.numpy_df(games_respond, numpy)

    def last_x_games_ha(self, name, x, date, ha, numpy=False):
        to_preview = 15
        mnums_before_date = self.last_x_mnums_before(name, to_preview, date)
        temp_arr = []
        home_away_dict = {"home": 0, "away": -1}

        for mnum in sorted(mnums_before_date, reverse=True):
            game_respond = SqlRequest(f''' SELECT * FROM scores_{self.country}
            WHERE MNUM = "{mnum}" ''').respond.fetchall()
            temp_df = self.numpy_df(game_respond, numpy)
            if temp_df["TEAM"].iloc[home_away_dict[ha]] == name:
                temp_arr.append(temp_df)
                if len(temp_arr) == x:
                    return pd.concat(temp_arr[::-1]).reset_index(drop=True)

    def games_list(self):
        games_df = self.games_df(numpy=False)
        games_dates = self.games_dates(mnums = True)

        mnums  = games_dates["MNUM"]

        def teams_names_scores_dates(mnum):
            mask = games_df["MNUM"] == mnum
            game_df = games_df[mask]
            home_team = game_df["TEAM"].iloc[0]
            away_team = game_df["TEAM"].iloc[-1]
            score_home = game_df["SCORE"].iloc[0]
            score_away = game_df["SCORE"].iloc[-1]
            return home_team, away_team, score_home, score_away

        home_teams, away_teams, scores_home, scores_away = zip(*map(teams_names_scores_dates, mnums))
        games_list_df = pd.DataFrame({"MNUM" : mnums, "HOME" : home_teams, "AWAY" : away_teams,
                                      "DATE" : games_dates["DATE"], "SCORE_HOME" : scores_home,
                                      "SCORE_AWAY" : scores_away})

        assert games_list_df.shape[1] > 0 and games_list_df.isna().sum().sum() == 0
        games_list_df = games_list_df.sort_values("DATE")
        return games_list_df

    def date_checker(self, year, date):
        if date not in list(self.games_dates()):
            raise ValueError(f"Date {date} must be from indicated season {year}")

    def gain_pts_sum(self, date):
        self.date_checker(self.year, date)
        games_list = self.games_list()
        mask_home  = (games_list["HOME"] == self.name) & (games_list["DATE"] < date)
        mask_away  = (games_list["AWAY"] == self.name) & (games_list["DATE"] < date)
        return games_list[mask_home]["SCORE_HOME"].sum() + games_list[mask_away]["SCORE_AWAY"].sum()

class Game(Team):

    def __init__(self, year, country, name, date):
        super().__init__(year, country, name)
        self.name = name
        self.date = date
        self.date_checker(self.year, self.date)

    @property
    def mnum(self):
        return self.one_team_df()["MNUM"].iloc[0]

    def one_team_df(self, numpy=False):
        game_respond = SqlRequest(f''' SELECT * FROM scores_{self.country}
                                  WHERE DATE = date("{self.date}") AND TEAM = "{self.name}"''').respond.fetchall()
        return self.numpy_df(game_respond, numpy)

    def df(self, numpy=False):
        mnum = self.one_team_df(numpy)["MNUM"].iloc[0]
        game = super().single_game(mnum, numpy)
        return game

    def zip_team(self, by_feature = "EFF", players_num = 8):
        game = self.one_team_df()
        game = game.sort_values(by_feature, ascending= False).reset_index(drop = True)
        first_part = game.iloc[:players_num-1]
        last_part  = game.iloc[-players_num-1:]

        last_part  =  last_part.select_dtypes(include=[np.number]).mean()
        last_part["MNUM"]   = first_part["MNUM"].iloc[0]
        last_part["DATE"]   = first_part["DATE"].iloc[0]
        last_part["PLAYER"] = "Players Meaned"
        last_part["TEAM"]   = self.name

        return pd.concat([first_part, pd.DataFrame(last_part).T], axis = 0).reset_index(drop = True)

    @property
    def which_week(self):
        return list(self.games_dates()).index(self.date)

class Statistics(Team):
    games_num_to_take = 100

    def __init__(self, year, country, name, date):
        super().__init__(year, country, name)
        self.date = date
        self.last_x_games_all = super().last_x_games_all(self.name, Statistics.games_num_to_take, self.date)

    def win_ratio_last_x(self, x, which="all"):

        mnums = self.last_x_games_all["MNUM"].drop_duplicates()[::-1]
        y_arr = []

        def all_ha():
            row = self.last_x_games_all[mask].iloc[0]
            if row["TEAM"] == self.name:
                temp_y = row["Y_TEAM_1"]
                y_arr.append(temp_y)
            else:
                row = self.last_x_games_all[mask].iloc[-1]
                temp_y = row["Y_TEAM_2"]
                y_arr.append(temp_y)

        def ha():
            home_away_dict = {"home": 0, "away": -1}
            row = self.last_x_games_all[mask].iloc[home_away_dict[which]]

            if row["TEAM"] == self.name:
                temp_y = row["Y_TEAM_1"] if which == "home" else row["Y_TEAM_2"]
                y_arr.append(temp_y)

        for mnum in mnums:
            mask = self.last_x_games_all["MNUM"] == mnum
            if which == "all":
                all_ha()
            elif which == "home" or which == "away":
                ha()
            if len(y_arr) == x:
                break
        return sum(y_arr) / len(y_arr)
# print(Team(2018, "pl", "Anwil Wloclawek").games_dates(mnums = True))
# print(Statistics(2012, "pl", "Anwil Wloclawek", "2019-04-10").win_ratio_last_x(5))
# print(Statistics("pl", "Anwil Wloclawek", "2019-04-10").last_x_games_all(5))
# print(Game(2018, "pl", "Anwil Wloclawek", "2019-04-10").which_week)
print(Game(2018, "pl", "Anwil Wloclawek", "2019-04-10").zip_team())