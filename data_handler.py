import sqlite3
import pandas as pd
import numpy as np


def sql_request(request):
    conn = sqlite3.connect(f'scores_data.db')
    c = conn.cursor()
    respond = c.execute(request)
    return respond.fetchall()


class Season:
    """ Init input:
        year - pass the year when season starts,
        country - pass country shortcut which is processing

        Methods:
            numpy_df(respond[sql], numpy[bool])
            season_date(which[str])
            single_game(mnum[int], numpy[bool])
            games_df(numpy[bool])
        Properties:
            teams, mnums, bencjamins, games_count
        """

    def __init__(self, year, country):
        self.year = year
        self.country = country
        self.start_date = self.season_date("start")
        self.end_date = self.season_date("end")
        self.columns = [i[1] for i in sql_request("PRAGMA table_info(scores_pl)")]


    def numpy_df(self, respond, numpy=False):
        if not numpy:
            return pd.DataFrame(respond, columns=self.columns)
        else:
            return np.array(respond)

    def season_date(self, which):

        data_dict = {"start": [f"{self.year}-05-01", ">", min],
                     "end": [f"{self.year + 1}-07-01", "<", max]}

        date = data_dict[which][0]
        sign = data_dict[which][1]
        get_f = data_dict[which][2]

        respond = sql_request(f''' SELECT DATE FROM scores_{self.country} WHERE DATE {sign} "{date}" ''')
        assert len(respond) > 0, "Returned date has 0 length. Check if year is correct."
        return get_f(respond)[0]

    def single_game(self, mnum, numpy=False):
        game_respond = sql_request(f''' SELECT * from scores_{self.country} 
        WHERE MNUM  = {mnum}''')
        return self.numpy_df(game_respond, numpy)

    def games_df(self, numpy=False):
        games = sql_request(f''' SELECT * from scores_{self.country} 
        WHERE MNUM IN {self.mnums}''')
        return self.numpy_df(games, numpy)

    @property
    def teams(self):
        teams_respond = sql_request(f''' SELECT DISTINCT TEAM from scores_{self.country} 
        WHERE DATE BETWEEN  date('{self.start_date}') and date('{self.end_date}')''')
        return [i[0] for i in teams_respond]

    @property
    def mnums(self):
        mnums_resp = sql_request(f''' SELECT DISTINCT MNUM from scores_{self.country} 
        WHERE DATE BETWEEN  date('{self.start_date}') and date('{self.end_date}')''')
        return tuple(i[0] for i in mnums_resp)

    @property
    def benjamins(self):
        previous_season_teams = Season(self.year - 1, self.country).teams
        return [i for i in self.teams if i not in previous_season_teams]

    @property
    def games_count(self):
        return len(set(self.mnums))



class Team(Season):
    """
    Init input:
        year[int], country[str], name[str]
    Methods:
        games_dates(mnums[int])
        games_df(numpy[bool])
        last_x_mnums_before(self, name, x, date):
        last_x_games_all(name[str], x[int], date[str YYYY-MM-DD], numpy[bool]):
        last_x_games_ha((name[str], x[int], date[str YYYY-MM-DD], ha[str home/away], numpy[boo]):
        games_list():
        date_checker(self, year[int], date[str YYYY-MM-DD]):
        pts_sum(date[str YYYY-MM-DD] gained_or_loose_pts[str gained/loss], all_home_or_away[str all/home/away], whole_season[bool])
    Properties:
        mnums

    """
    def __init__(self, year, country, name):
        super().__init__(year, country)
        self.name = name

    @property
    def mnums(self):
        team_mnums = sql_request(f''' SELECT DISTINCT MNUM FROM scores_{self.country} 
        WHERE TEAM = "{self.name}" ''')
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
        games_respond = sql_request(f''' SELECT * FROM scores_{self.country}
        WHERE MNUM IN {self.mnums} ''')
        return self.numpy_df(games_respond, numpy)

    def last_x_mnums_before(self, name, x, date):
        self.date_checker(self.year, date)
        mnums_before_date = sql_request(f''' SELECT MNUM FROM scores_{self.country}
        WHERE DATE < date("{date}") AND TEAM = "{name}"''')
        mnums_before_date = sorted(set(mnums_before_date))[-x:]
        return tuple(i[0] for i in mnums_before_date)

    def last_x_games_all(self, name, x, date, numpy=False):
        mnums_before_date = self.last_x_mnums_before(name, x, date)
        games_respond = sql_request(f''' SELECT * FROM scores_{self.country}
        WHERE MNUM IN {mnums_before_date} ''')
        return self.numpy_df(games_respond, numpy)

    def last_x_games_ha(self, name, x, date, ha, numpy=False):
        to_preview = 15
        mnums_before_date = self.last_x_mnums_before(name, to_preview, date)
        temp_arr = []
        home_away_dict = {"home": 0, "away": -1}

        for mnum in sorted(mnums_before_date, reverse=True):
            game_respond = sql_request(f''' SELECT * FROM scores_{self.country}
            WHERE MNUM = "{mnum}" ''')
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
            mask       = games_df["MNUM"] == mnum
            game_df    = games_df[mask]
            home_team  = game_df["TEAM"].iloc[0]
            away_team  = game_df["TEAM"].iloc[-1]
            score_home = game_df["SCORE"].iloc[0]
            score_away = game_df["SCORE"].iloc[-1]
            y          = 1 if score_home > score_away else 0
            return home_team, away_team, score_home, score_away, y

        home_teams, away_teams, scores_home, scores_away, y = zip(*map(teams_names_scores_dates, mnums))
        games_list_df = pd.DataFrame({"MNUM" : mnums, "HOME" : home_teams, "AWAY" : away_teams,
                                      "DATE" : games_dates["DATE"], "SCORE_HOME" : scores_home,
                                      "SCORE_AWAY" : scores_away, "Y" : y})

        assert games_list_df.shape[1] > 0 and games_list_df.isna().sum().sum() == 0
        games_list_df = games_list_df.sort_values("DATE")
        return games_list_df

    def date_checker(self, year, date):
        if date not in list(self.games_dates()):
            raise ValueError(f"Date {date} must be from indicated season {year}")

    def pts_sum(self, date, gained_or_loose_pts, all_home_or_away, whole_season):
        self.date_checker(self.year, date)
        games_list = self.games_list(whole_season)
        if gained_or_loose_pts == "gained":
            mask_home  = (games_list["HOME"] == self.name) & (games_list["DATE"] < date)
            mask_away  = (games_list["AWAY"] == self.name) & (games_list["DATE"] < date)
        elif gained_or_loose_pts == "loose":
            mask_home  = (games_list["HOME"] != self.name) & (games_list["DATE"] < date)
            mask_away  = (games_list["AWAY"] != self.name) & (games_list["DATE"] < date)
        if all_home_or_away == "all"    : return games_list[mask_home]["SCORE_HOME"].sum() + games_list[mask_away]["SCORE_AWAY"].sum()
        elif all_home_or_away == "home" : return games_list[mask_home]["SCORE_HOME"].sum()
        elif all_home_or_away == "away" : return games_list[mask_away]["SCORE_AWAY"].sum()

class Game(Team):
    """
    Init input:
        year[int], country[str], name[str], date[str]
    Methods:
        one_team_df(numpy[bool])
        df(numpy[bool])
        zip_team(by_feature[str], players_num[int])
        which_week(sin_cos[bool])
    Properties:
        mnum
    """
    def __init__(self, year, country, name, date):
        super().__init__(year, country, name)
        self.name = name
        self.date = date
        self.date_checker(self.year, self.date)

    @property
    def mnum(self):
        return self.one_team_df()["MNUM"].iloc[0]

    def one_team_df(self, numpy=False):
        game_respond = sql_request(f''' SELECT * FROM scores_{self.country}
                                  WHERE DATE = date("{self.date}") AND TEAM = "{self.name}"''')
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

    def which_week(self, sin_cos=False):
        dates = self.games_dates()
        date  = self.date
        if sin_cos:
            from math import pi
            norm = 2 * pi * list(dates).index(date) / len(dates)
            c = np.cos(norm)
            s = np.sin(norm)
            return np.round(s,3), np.round(c,3)
        else:
            return list(dates).index(date)


class Statistics(Team):
    """
     Init input:
        year[int], country[str], name[str], date[str]
    Methods:
        win_ratio_last_x(x[int], which[str all/home/away]
        standings(whole_season[bool])
    """
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

    def standings(self, whole_season):
        games_list_df = self.games_list()
        if not whole_season:
            date_mask = games_list_df["DATE"] < self.date
            games_list_df = games_list_df[date_mask]

        def home_away_standings(home_or_away):
            name_mask  = games_list_df[home_or_away] == self.name
            games = games_list_df[name_mask]
            if home_or_away == "HOME":
                wins_num, loss_num   = games["Y"].sum(), games[games["Y"] == 0].shape[0]
            elif home_or_away == "AWAY":
                loss_num, wins_num = games["Y"].sum(), games[games["Y"] == 0].shape[0]

            return wins_num, loss_num
        win_loss_dict = {}
        win_loss_dict["home_wins_num"], win_loss_dict["home_loss_num"] = home_away_standings(home_or_away = "HOME")
        win_loss_dict["away_wins_num"], win_loss_dict["away_loss_num"] = home_away_standings(home_or_away = "AWAY")
        return win_loss_dict


# print(Statistics(2012, "pl", "Anwil Wloclawek", "2019-04-10").win_ratio_last_x(5))
# print(Team(2018, "pl", "Anwil Wloclawek").games_dates())
# print(Statistics(2018, "pl", "Anwil Wloclawek", "2019-04-20").standings(whole_season=False))
print(Game(2018, "pl", "Anwil Wloclawek", "2019-04-10").which_week(sin_cos = True))
# print(Game(2018, "pl", "Anwil Wloclawek", "2019-04-10").zip_team())
# print(len(sql_request("PRAGMA table_info(scores_pl)")))
