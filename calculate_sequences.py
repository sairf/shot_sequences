import mysql.connector
from mysql.connector import Error

def shot_sequences(game_id):
	try:
		connection = mysql.connector.connect(host='localhost',
								 database='stats',
								 user='cenas',
								 password='tElM0!5/')
		if connection.is_connected():
			db_Info = connection.get_server_info()
			#print("Connected to MySQL database... MySQL Server version on ",db_Info)
			cursor = connection.cursor(dictionary=True)
			cursor.execute(f"select g.*, p.id_team, case when p.known_name = '' then p.full_name else p.known_name end as player_name from stats.game_all_events g, stats.player_stats p where g.game_id = {game_id} and g.player_id = p.player_id and p.year = g.season_id and g.competition_id = p.id_competition and (g.home_team_id = p.id_team or g.away_team_id = p.id_team) order by min, sec;")
			record = cursor.fetchall()
			seq_list = []
			seq = [] #sequence (list of passes): #pass {id_team, player_id, player_name, pass_x, pass_y, min, sec}
			shots = ["Miss", "Post", "Attempt Saved", "Goal", "Chance Missed"]
			for i, row in enumerate(record):
				if row["chance_type"] in shots:
					seq.append({'id_team':row["id_team"], 'player_id':row["player_id"], 'player_name':row["player_name"], 'pass_x':row["x"], 'pass_y':row["y"], 'min':row["min"], 'sec':row["sec"]})
					for j in range(i-1, 0, -1):
						if record[j]["id_team"] == row["id_team"]:
							if record[j]["chance_type"] == "Pass":
								seq.append({'id_team':record[j]["id_team"], 'player_id':record[j]["player_id"], 'player_name':record[j]["player_name"], 'pass_x':record[j]["x"], 'pass_y':record[j]["y"], 'min':record[j]["min"], 'sec':record[j]["sec"]})
						else:
							seq_list.append(seq)
							seq = []
							break
			print(seq_list)
	except Error as e :
		print ("Error while connecting to MySQL", e)
	finally:
		#closing database connection.
		if(connection.is_connected()):
			cursor.close()
			connection.close()
			print("MySQL connection is closed")
	return;
	
shot_sequences(855278)