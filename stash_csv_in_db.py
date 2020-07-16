import psycopg2
from psycopg2.extensions import AsIs


def stash_standings_in_db(standings, league):
    print('=========== STASHING STANDINGS ==============')
    print('')
    print('')
    conn = psycopg2.connect("host='all-them-stats.chure6gtnama.us-east-1.rds.amazonaws.com' port='5432' "
                            "dbname='stats_data' user='bundesstats' password='bundesstats'")
    delete_statement = 'delete from standings_' + league + ';'
    cursor = conn.cursor()
    cursor.execute(delete_statement)
    for index in range(len(standings)):
        # standings[index]["index"] = index
        columns = standings[index].keys()
        values = [standings[index][column] if standings[index][column] else None for column in columns]
        print(values)
        if not values:
            continue
        db_name = 'standings_' + league
        insert_statement = 'insert into ' + db_name + ' (%s) values %s'

        cursor.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

    conn.commit()
    conn.close()
    print('=========== DONE STASHING STANDINGS ==============')
    print('')
    print('')

def stash_in_db(list, league):
    print('=========== STASHING ==============')
    print('')
    print('')
    conn = psycopg2.connect("host='all-them-stats.chure6gtnama.us-east-1.rds.amazonaws.com' port='5432' "
                            "dbname='stats_data' user='bundesstats' password='bundesstats'")
    delete_statement = 'delete from full_game_log_' + league + ';'
    cursor = conn.cursor()
    cursor.execute(delete_statement)
    for index in range(len(list)):
        list[index]["index"] = index
        columns = list[index].keys()
        values = [list[index][column] if list[index][column] else None for column in columns]
        print(values)
        if not values:
            continue
        db_name = 'full_game_log_' + league
        insert_statement = 'insert into ' + db_name + ' (%s) values %s'

        cursor.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

    conn.commit()
    conn.close()
    print('=========== DONE STASHING ==============')
    print('')
    print('')
