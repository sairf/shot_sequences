import mysql.connector
import datetime
import pickle

from mysql.connector import Error


def get_sequences_by_competition_and_year(competition_id, year):
	try:
		print("START - " + str(datetime.datetime.now()))
		connection = mysql.connector.connect(host='localhost',
								 database='stats',
								 user='cenas',
								 password='tElM0!5/')
		if connection.is_connected():
			db_Info = connection.get_server_info()
			#print("Connected to MySQL database... MySQL Server version on ",db_Info)
			cursor = connection.cursor(dictionary=True)			
			cursor.execute(f"select g.*, p.id_team, case when p.known_name = '' then p.full_name else p.known_name end as player_name from stats.game_all_events g, stats.player_stats p where g.player_id = p.player_id and p.year = g.season_id and g.competition_id = p.id_competition and (g.home_team_id = p.id_team or g.away_team_id = p.id_team) and g.competition_id = {competition_id} and g.season_id = {year} order by game_id, min desc, sec desc LIMIT 2000;")
			record = cursor.fetchall()
			print("Fetch done - " + str(datetime.datetime.now()))
			all_seqs_list = []
			seq_list = []
			seq = [] #sequence (list of passes): #pass {id_team, player_id, player_name, pass_x, pass_y, min, sec}
			game_seq_dict = {}
			shots = ["Miss", "Post", "Attempt Saved", "Goal", "Chance Missed"]
			last_game_id = last_id_team = 0
			for i, row in enumerate(record):
				if row["chance_type"] in shots and last_id_team == 0: #sequence start
					seq.append({'id_team':row["id_team"], 'player_id':row["player_id"], 'player_name':row["player_name"], 'shot_x':row["x"], 'shot_y':row["y"], 'min':row["min"], 'sec':row["sec"]})
					last_game_id = row["game_id"]
					last_id_team = row["id_team"]
					print("i: " + str(i) + " - " + "sequence start: " + str(row["id_team"]) + " game: " + str(row["game_id"]))
				else:
					if not last_id_team: # not in a sequence;
						if not row["game_id"] == record[i-1]["game_id"] and i-1 > 0: #different game, append to sequence dictionary
							last_game_id = 0
							last_id_team = 0
							game_seq_dict["game_id"] = row["game_id"]
							game_seq_dict["sequence_list"] = seq_list
							all_seqs_list.append(game_seq_dict)
							seq_list = []
							print("i: " + str(i) + " - " + "diff team: " + str(row["id_team"]) + " diff game: " + str(row["game_id"]))
						else:
							print("i: " + str(i) + " - " + "Ignore: " + str(row["id_team"]) + " game: " + str(row["game_id"]))
					else: # in a sequence
						if row["id_team"] == last_id_team and row["game_id"] == last_game_id: # same game and team of the last row, append to sequence;
							if row["chance_type"] == 'Pass':
								seq.append({'id_team':row["id_team"], 'player_id':row["player_id"], 'player_name':row["player_name"], 'shot_x':row["x"], 'shot_y':row["y"], 'min':row["min"], 'sec':row["sec"]})
								print("i: " + str(i) + " - " + "same team: " + str(row["id_team"]) + " same game: " + str(row["game_id"]))
							else:
								print("i: " + str(i) + " - " + "same team but not a pass: " + str(row["id_team"]) + " same game: " + str(row["game_id"]))
						elif row["game_id"] == last_game_id: #same game but different team;
							if row["chance_type"] == 'Save': 
								#Ignore, sequence continues;
								print("i: " + str(i) + " - " + "Ignore Save: " + str(row["id_team"]) + " same game: " + str(row["game_id"]))
							else:
								last_id_team = 0
								seq_list.append(seq);
								seq = []
								print("i: " + str(i) + " - " + "diff team: " + str(row["id_team"]) + " same game: " + str(row["game_id"]))
			#adding to all sequences list the last game, when there are no more rows to analyse
			game_seq_dict["game_id"] = row["game_id"]
			game_seq_dict["sequence_list"] = seq_list
			all_seqs_list.append(game_seq_dict)
							
	except Error as e :
		print ("Error while connecting to MySQL", e)
	finally:
		#closing database connection.
		if(connection.is_connected()):
			cursor.close()
			connection.close()
			print("MySQL connection is closed")
	print("END - " + str(datetime.datetime.now()))
	return all_seqs_list;
	
#shot_sequences(855278)

# def get_all_sequences(competition_id, year):
	# try:
		# connection = mysql.connector.connect(host='localhost',
								 # database='stats',
								 # user='cenas',
								 # password='tElM0!5/')
		# if connection.is_connected():
			# db_Info = connection.get_server_info()
			# #print("Connected to MySQL database... MySQL Server version on ",db_Info)
			# cursor = connection.cursor(dictionary=True)
			# cursor.execute(f"select distinct g.game_id from stats.game_all_events g where g.season_id = {year} and g.competition_id = {competition_id};")
			# record = cursor.fetchall()
			# game_sequences_list = []
			# for game in record:
				# game_sequences_list.append(get_sequences_by_game(game["game_id"]))
	# except Error as e :
		# print ("Error while connecting to MySQL", e)
	# finally:
		# #closing database connection.
		# if(connection.is_connected()):
			# cursor.close()
			# connection.close()
			# print("MySQL connection is closed")
	# return game_sequences_list;
	
# #get_all_sequences(99, 2018)

def sequence_passes(sequence):
	print(sequence)
	return len(sequence);
	
seq_list = get_sequences_by_competition_and_year(99, 2018)
#with open('list.txt', 'wb') as filehandle:
#	pickle.dump(seq_list, filehandle)
print(seq_list)