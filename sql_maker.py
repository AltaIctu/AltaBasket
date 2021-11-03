import sqlite3
import pandas as pd

columns = ['PLAYER', 'MIN', 'OR', 'DR', 'AST', 'STL', 'BLK', 'PTS', 'EFF', 'TEAM',
           'SCORE', 'MNUM', 'DATE', 'Y_TEAM_1', 'Y_TEAM_2', 'AGE', 'HEIGHT', 'EXP',
           'TEAM_EXP', 'M1', 'A1', 'M2', 'A2', 'M3', 'A3', 'DOY_COS', 'DOY_SIN',
           'DOW_COS', 'DOW_SIN']


def data_to_sql(data, columns, country):
    conn = sqlite3.connect(f'scores_data.db')
    c = conn.cursor()

    c.execute(
        f'CREATE TABLE IF NOT EXISTS scores_{country} (PLAYER text, MIN number, OR_ number, DR number, STL number, BLK number, PTS number, EFF number, TEAM text, SCORE number, MNUM nubmer, DATE text, Y_TEAM_1 number, Y_TEAM_2 number, AGE number, HEIGHT number, EXP number, TEAM_EXP number, M1 number, A1 number, M2 number, A2 number, M3 number, A3 number, DOY_COS number, DOY_SIN number, DOW_COS number, DOW_SIN number)')
    conn.commit()

    scores = pd.read_csv(data)
    scores.columns = columns

    scores.to_sql(f'scores_{country}', conn, if_exists='replace', index=False)
    pd.Series(columns).to_csv(f"columns_{country}.csv", index=False)


data_to_sql("pl_data.csv", columns, "pl")