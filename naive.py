# Default Python Libraries
import sys, math, string, random

# Libraries for MySql Connection
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.errors import ProgrammingError

# Libraries for SciPy
from stat import *
from numpy import array
from scipy.cluster.vq import vq, kmeans2, whiten
import numpy as np

# Libraries for Matplotlib / Pylab
import pylab as pl
import matplotlib
from mpl_toolkits.mplot3d import Axes3D


# Initialize the connection
connection = None

# Create DB Connection
def makeConnection():
	# Load in the connection global
	global connection
	connection = mysql.connector.connect(user='root',
		password='',
		host='127.0.0.1',
		database='parkinsons')
	connection.autocommit = True
	print("Connection Successful")

# Close DB Connection
def closeConnection():
	# Load in the connection global
	global connection
	connection.close()
	print("Connection Closed")


# Select parkinsons and a specific column from a specific
# aggregation table using a group by specified (or left blank)
def getColumnFromTable(column, agg, group_by = None):
	group_by_column = "GROUP BY name, {0} ".format(group_by) if group_by is not None else ""
	query = "SELECT parkinsons, {0} FROM {1} {2}".format(column, agg, group_by_column)
	return query


# Initialize the prob_parkinsons P(P=1) to 0
prob_parkinsons = 0

# Get and set probability of Parkinsons
# based on the query params
def setProbParkinsons(agg, group_by = None):
	# Load in P(P=1) to update
	global prob_parkinsons

	# Load in the connection global
	global connection

	from_table = agg
	group_by_statement = "GROUP BY {0} ".format(group_by) if group_by is not None else ""

	parkinsons_query = "SELECT * FROM {0} WHERE parkinsons = 1".format(from_table)

	parkinsons_query = "SELECT COUNT(*) FROM ({0}) AS A {1}".format(parkinsons_query, group_by_statement)
	
	total_query = "SELECT * FROM {0}".format(from_table)

	total_query = "SELECT COUNT(*) FROM ({0}) AS A {1}".format(total_query, group_by_statement)

	cursor = connection.cursor()
	
	try:
		cursor.execute(parkinsons_query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(parkinsons_query)
		closeConnection()
		sys.exit()
	parkinsons_count = cursor.fetchall()

	try:
		cursor.execute(total_query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(total_query)
		closeConnection()
		sys.exit()
	total_count = cursor.fetchall()

	if len(parkinsons_count) == len(total_count):
		if len(parkinsons_count) > 1:
			prob_parkinsons = {}
			for i in range(len(parkinsons_count)):
				prob_parkinsons[i] = parkinsons_count[i][0] / total_count[i][0]
		else:
			prob_parkinsons = parkinsons_count[0][0] / total_count[0][0]
	else:
		print(parkinsons_count)
		print(total_count)
		sys.exit()


# Gaussian probability function P(V=v|X)
# Must still P(V=v|X)P(X)
def prob(v, mu, sig):
	front = 1 / (math.sqrt(2 * math.pi * sig))
	back = math.exp(-1 * ((v - mu)**2) / (2 * sig))
	return front * back


# Get mean and std dev agg_per_day overall
def statsAggPerDayOverall():
	print("Aggregated by Day. Ungrouped")
	# Load in the connection global
	global connection

	# Create the cursor object
	cursor = connection.cursor()

	# Load in P(P=1) global
	global prob_parkinsons

	agg = "agg_per_day"

	# Get P(P=1) using the above info
	setProbParkinsons(agg)

	print("Parkinsons Probability Prior: {0}".format(prob_parkinsons))

	'''
	Latitude Varianvce
	'''

	# mu 	: [P,!P]
	# sig 	: [P,!P]
	lat_var = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Variance trait
	query = ("SELECT "
		"parkinsons, lat_var_mu, lat_var_sig "
		"FROM lat_var_per_day_overall_view "
		"WHERE lat_var_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_var['mu'].append(result[1])
		lat_var['sig'].append(result[2])
	
	# print(lat_var['mu'], lat_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_var", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	lat_var_right = 0
	lat_var_count = 0
	for result in results:
		lat_var_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_var['mu'][0], lat_var['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_var['mu'][1], lat_var['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_var_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Variance
	'''

	long_var = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Variance trait
	query = ("SELECT "
		"parkinsons, long_var_mu, long_var_sig "
		"FROM long_var_per_day_overall_view "
		"WHERE long_var_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_var['mu'].append(result[1])
		long_var['sig'].append(result[2])
	
	# print(long_var['mu'], long_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_var", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	long_var_right = 0
	long_var_count = 0
	for result in results:
		long_var_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_var['mu'][0], long_var['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_var['mu'][1], long_var['sig'][1]))
		evidence = p_prob + not_p_prob
		if evidence == 0:
			continue
			# print("ZERO:: ({0} , {1} , {2})".format(result[1], long_var['mu'][0], long_var['sig'][0]))
			# print("Prob:: {0}".format(prob(result[1], long_var['mu'][0], long_var['sig'][0])))
			# print("ZERO:: ({0} , {1} , {2})".format(result[1], long_var['mu'][1], long_var['sig'][1]))
			# print("ProbNot:: {0}".format(prob(result[1], long_var['mu'][1], long_var['sig'][1])))
			# sys.exit()
		if result[0] is 1 and p_prob > not_p_prob:
			long_var_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Traveled
	'''

	gps_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Traveled trait
	query = ("SELECT "
		"parkinsons, gps_traveled_mu, gps_traveled_sig "
		"FROM gps_traveled_per_day_overall_view "
		"WHERE gps_traveled_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		gps_traveled['mu'].append(result[1])
		gps_traveled['sig'].append(result[2])
	
	# print(gps_traveled['mu'], gps_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("gps_traveled", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	gps_traveled_right = 0
	gps_traveled_count = 0
	for result in results:
		gps_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], gps_traveled['mu'][0], gps_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], gps_traveled['mu'][1], gps_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			gps_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Range
	'''

	gps_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Range trait
	query = ("SELECT "
		"parkinsons, gps_range_mu, gps_range_sig "
		"FROM gps_range_per_day_overall_view "
		"WHERE gps_range_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		gps_range['mu'].append(result[1])
		gps_range['sig'].append(result[2])
	
	# print(gps_range['mu'], gps_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("gps_range", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	gps_range_right = 0
	gps_range_count = 0
	for result in results:
		gps_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], gps_range['mu'][0], gps_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], gps_range['mu'][1], gps_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			gps_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Results
	'''

	print("Latitude Variance Classification Success Rate: {0}".format(lat_var_right/lat_var_count))

	print("Longitude Variance Classification Success Rate: {0}".format(long_var_right/long_var_count))
	
	print("Traveled Classification Success Rate: {0}".format(gps_traveled_right/gps_traveled_count))
		
	print("Range Classification Success Rate: {0}".format(gps_range_right/gps_range_count))
	

# Get mean and std dev agg_per_day gb_dayofweek
def statsAggPerDayGroupByDayOfWeek():
	print("Aggregating by Day. Grouping by Day Of Week")
	# Load in the connection global
	global connection

	# Create the cursor object
	cursor = connection.cursor()

	# Load in P(P=1) global
	global prob_parkinsons

	agg = "agg_per_day"
	group_by = "DAYOFWEEK(record_day)"

	# Get P(P=1) using the above info
	setProbParkinsons(agg)

	print("Parkinsons Probability Prior: {0}".format(prob_parkinsons))

	'''
	Latitude Varianvce
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	lat_var = {
	'mu' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
		}, 'sig' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
		}
	}

	# Get the mean and std dev for the Latitude Variance trait
	query = ("SELECT "
		"* "
		"FROM lat_var_per_day_gb_dayofweek_view "
		"WHERE lat_var_sig <> 0 "
		"ORDER BY parkinsons, dayofweek DESC"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()
	# (dayofweek, parkinsons, lat_var_mu, lat_var_sig)

	# Store mean and std dev into their respective areas
	for row in results:
		# Add value to appropriate section
		if row[1] == 1:
			lat_var['mu'][row[0]][0] = row[2]
			lat_var['sig'][row[0]][0] = row[3]
		else:
			lat_var['mu'][row[0]][1] = row[2]
			lat_var['sig'][row[0]][1] = row[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), lat_var", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, lat_var)

	# Use gaussian distribution
	subjects = {}
	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = lat_var['mu'][dayofweek][0]
		npmu = lat_var['mu'][dayofweek][1]
		psig = lat_var['sig'][dayofweek][0]
		npsig = lat_var['sig'][dayofweek][1]

		# Get their partial probability and multiply it to their existing partial
		p_prob = prob(v, pmu, psig)
		np_prob = prob(v, npmu, npsig)
		evidence = (prob_parkinsons)*(p_prob)+(1 - prob_parkinsons)*(np_prob)
		if evidence == 0:
			continue
		# Subject doesn't exist in dict, add
		if name not in subjects:
			subjects[name] = {'p_prob':1,'np_prob':1,'evidence':1,'parkinsons':parkinsons}
		subjects[name]['p_prob'] *= p_prob
		subjects[name]['np_prob'] *= np_prob
		subjects[name]['evidence'] *= evidence

	correct = 0
	counter = 0
	for (name, subject) in subjects.items():
		has_park = False
		if (prob_parkinsons)*subject['p_prob'] > (1 - prob_parkinsons)*subject['np_prob']:
			# print("{0}: Parkinsons".format(name))
			has_park = True
		# else:
			# print("{0}: Not Parkinsons".format(name))
		PP = (prob_parkinsons)*subject['p_prob']/subject['evidence']
		# print("{0}: {1}% Chance of Parkinsons".format(name,PP))
		if has_park and subject['parkinsons'] == 1:
			correct += 1
		counter += 1
	print("Lat_Var :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Longitude Variance
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	long_var = {
	'mu' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}, 'sig' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}}

	# Get the mean and std dev for the Latitude Variance trait
	query = ("SELECT "
		"* "
		"FROM long_var_per_day_gb_dayofweek_view "
		"WHERE long_var_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			long_var['mu'][result[0]][0] = result[2]
			long_var['sig'][result[0]][0] = result[3]
		else:
			long_var['mu'][result[0]][1] = result[2]
			long_var['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), long_var", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, long_var)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = long_var['mu'][dayofweek][0]
		npmu = long_var['mu'][dayofweek][1]
		psig = long_var['sig'][dayofweek][0]
		npsig = long_var['sig'][dayofweek][1]

		# Get their partial probability and multiply it to their existing partial
		p_prob = prob(v, pmu, psig)
		np_prob = prob(v, npmu, npsig)
		evidence = (prob_parkinsons)*(p_prob)+(1 - prob_parkinsons)*(np_prob)
		if evidence == 0:
			continue
		# Subject doesn't exist in dict, add
		if name not in subjects:
			subjects[name] = {'p_prob':1,'np_prob':1,'evidence':1,'parkinsons':parkinsons}
		subjects[name]['p_prob'] *= p_prob
		subjects[name]['np_prob'] *= np_prob
		subjects[name]['evidence'] *= evidence

	correct = 0
	counter = 0
	for (name, subject) in subjects.items():
		has_park = False
		if (prob_parkinsons)*subject['p_prob'] > (1 - prob_parkinsons)*subject['np_prob']:
			# print("{0}: Parkinsons".format(name))
			has_park = True
		# else:
			# print("{0}: Not Parkinsons".format(name))
		PP = (prob_parkinsons)*subject['p_prob']/subject['evidence']
		# print("{0}: {1}% Chance of Parkinsons".format(name,PP))
		if has_park and subject['parkinsons'] == 1:
			correct += 1
		counter += 1
	print("Long_Var :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Traveled
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	gps_traveled = {
	'mu' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}, 'sig' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}}

	# Get the mean and std dev for the GPS Traveled trait
	query = ("SELECT "
		"* "
		"FROM gps_traveled_per_day_gb_dayofweek_view "
		"WHERE gps_traveled_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			gps_traveled['mu'][result[0]][0] = result[2]
			gps_traveled['sig'][result[0]][0] = result[3]
		else:
			gps_traveled['mu'][result[0]][1] = result[2]
			gps_traveled['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), gps_traveled", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, gps_traveled)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = gps_traveled['mu'][dayofweek][0]
		npmu = gps_traveled['mu'][dayofweek][1]
		psig = gps_traveled['sig'][dayofweek][0]
		npsig = gps_traveled['sig'][dayofweek][1]

		# Get their partial probability and multiply it to their existing partial
		p_prob = prob(v, pmu, psig)
		np_prob = prob(v, npmu, npsig)
		evidence = (prob_parkinsons)*(p_prob)+(1 - prob_parkinsons)*(np_prob)
		if evidence == 0:
			continue
		# Subject doesn't exist in dict, add
		if name not in subjects:
			subjects[name] = {'p_prob':1,'np_prob':1,'evidence':1,'parkinsons':parkinsons}
		subjects[name]['p_prob'] *= p_prob
		subjects[name]['np_prob'] *= np_prob
		subjects[name]['evidence'] *= evidence

	correct = 0
	counter = 0
	for (name, subject) in subjects.items():
		has_park = False
		if (prob_parkinsons)*subject['p_prob'] > (1 - prob_parkinsons)*subject['np_prob']:
			# print("{0}: Parkinsons".format(name))
			has_park = True
		# else:
			# print("{0}: Not Parkinsons".format(name))
		PP = (prob_parkinsons)*subject['p_prob']/subject['evidence']
		# print("{0}: {1}% Chance of Parkinsons".format(name,PP))
		if has_park and subject['parkinsons'] == 1:
			correct += 1
		counter += 1
	print("GPS_Traveled :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Range
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	gps_range = {
	'mu' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}, 'sig' : {
		1:[0,0],
		2:[0,0],
		3:[0,0],
		4:[0,0],
		5:[0,0],
		6:[0,0],
		7:[0,0],
	}}

	# Get the mean and std dev for the GPS Range trait
	query = ("SELECT "
		"* "
		"FROM gps_range_per_day_gb_dayofweek_view "
		"WHERE gps_range_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			gps_range['mu'][result[0]][0] = result[2]
			gps_range['sig'][result[0]][0] = result[3]
		else:
			gps_range['mu'][result[0]][1] = result[2]
			gps_range['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), gps_range", agg)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, gps_range)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = gps_range['mu'][dayofweek][0]
		npmu = gps_range['mu'][dayofweek][1]
		psig = gps_range['sig'][dayofweek][0]
		npsig = gps_range['sig'][dayofweek][1]

		# Get their partial probability and multiply it to their existing partial
		p_prob = prob(v, pmu, psig)
		np_prob = prob(v, npmu, npsig)
		evidence = (prob_parkinsons)*(p_prob)+(1 - prob_parkinsons)*(np_prob)
		if evidence == 0:
			continue
		# Subject doesn't exist in dict, add
		if name not in subjects:
			subjects[name] = {'p_prob':1,'np_prob':1,'evidence':1,'parkinsons':parkinsons}
		subjects[name]['p_prob'] *= p_prob
		subjects[name]['np_prob'] *= np_prob
		subjects[name]['evidence'] *= evidence

	correct = 0
	counter = 0
	for (name, subject) in subjects.items():
		has_park = False
		if (prob_parkinsons)*subject['p_prob'] > (1 - prob_parkinsons)*subject['np_prob']:
			# print("{0}: Parkinsons".format(name))
			has_park = True
		# else:
			# print("{0}: Not Parkinsons".format(name))
		PP = (prob_parkinsons)*subject['p_prob']/subject['evidence']
		# print("{0}: {1}% Chance of Parkinsons".format(name,PP))
		if has_park and subject['parkinsons'] == 1:
			correct += 1
		counter += 1
	print("GPS_Range :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))


# Get mean and std dev agg_per_day gb_dayofweek
def statsAggPerHourOverall():
	print("Aggregated by Hour. Ungrouped")
	# Load in the connection global
	global connection

	# Create the cursor object
	cursor = connection.cursor()

	# Load in P(P=1) global
	global prob_parkinsons

	agg = "agg_per_hour"
	group_by = None

	# Get P(P=1) using the above info
	setProbParkinsons(agg, group_by)

	print("Parkinsons Probability Prior: {0}".format(prob_parkinsons))

	'''
	Latitude Varianvce
	'''

	# mu 	: [P,!P]
	# sig 	: [P,!P]
	lat_var = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Variance trait
	query = ("SELECT "
		"parkinsons, lat_var_mu, lat_var_sig "
		"FROM lat_var_per_hour_overall_view "
		"WHERE lat_var_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_var['mu'].append(result[1])
		lat_var['sig'].append(result[2])
	
	# print(lat_var['mu'], lat_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_var", agg, group_by)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	lat_var_right = 0
	lat_var_count = 0
	for result in results:
		lat_var_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_var['mu'][0], lat_var['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_var['mu'][1], lat_var['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_var_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Variance
	'''

	long_var = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Variance trait
	query = ("SELECT "
		"parkinsons, long_var_mu, long_var_sig "
		"FROM long_var_per_hour_overall_view "
		"WHERE long_var_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_var['mu'].append(result[1])
		long_var['sig'].append(result[2])
	
	# print(long_var['mu'], long_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_var", agg, group_by)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	long_var_right = 0
	long_var_count = 0
	for result in results:
		long_var_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_var['mu'][0], long_var['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_var['mu'][1], long_var['sig'][1]))
		evidence = p_prob + not_p_prob
		if evidence == 0:
			continue
			# print("ZERO:: ({0} , {1} , {2})".format(result[1], long_var['mu'][0], long_var['sig'][0]))
			# print("Prob:: {0}".format(prob(result[1], long_var['mu'][0], long_var['sig'][0])))
			# print("ZERO:: ({0} , {1} , {2})".format(result[1], long_var['mu'][1], long_var['sig'][1]))
			# print("ProbNot:: {0}".format(prob(result[1], long_var['mu'][1], long_var['sig'][1])))
			# sys.exit()
		if result[0] is 1 and p_prob > not_p_prob:
			long_var_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Latitude Traveled
	'''

	gps_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, gps_traveled_mu, gps_traveled_sig "
		"FROM gps_traveled_per_hour_overall_view "
		"WHERE gps_traveled_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		gps_traveled['mu'].append(result[1])
		gps_traveled['sig'].append(result[2])
	
	# print(gps_traveled['mu'], gps_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("gps_traveled", agg, group_by)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	gps_traveled_right = 0
	gps_traveled_count = 0
	for result in results:
		gps_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], gps_traveled['mu'][0], gps_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], gps_traveled['mu'][1], gps_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			gps_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Range
	'''

	gps_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, gps_range_mu, gps_range_sig "
		"FROM gps_range_per_hour_overall_view "
		"WHERE gps_range_sig <> 0"
	)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		gps_range['mu'].append(result[1])
		gps_range['sig'].append(result[2])
	
	# print(gps_range['mu'], gps_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("gps_range", agg, group_by)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()
		
	results = cursor.fetchall()

	gps_range_right = 0
	gps_range_count = 0
	for result in results:
		gps_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], gps_range['mu'][0], gps_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], gps_range['mu'][1], gps_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			gps_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Results
	'''

	print("Latitude Variance Classification Success Rate: {0}".format(lat_var_right/lat_var_count))

	print("Longitude Variance Classification Success Rate: {0}".format(long_var_right/long_var_count))
	
	print("GPS Traveled Classification Success Rate: {0}".format(gps_traveled_right/gps_traveled_count))
	
	print("GPS Range Classification Success Rate: {0}".format(gps_range_right/gps_range_count))
	

# Returns a list of (points,tags)
def rows_to_points_and_tags(rows, num_fields):
	# Pull in all variables but parkinsons and
	# use it as a 'point'
	points = []
	tags = []
	for row in rows:
		point = []
		for i in range(num_fields):
			vi = i+1
			point.append(row[vi])
		tags.append([row[0]])
		points.append(point)

	return (points,tags)

def kmeans(points, tags, k = 2, laplace = 1):
	num_fields = len(points[0])
	# Create a numpy array
	points_array = array([points])
	# In order to calculate the standard deviation and mean
	std = np.std(points_array, axis=1)[0]
	mean = np.mean(points_array, axis=1)[0]
	# print(std)
	# print(mean)

	# For each variable in the fields array
	for i in range(num_fields):
		# Create temp variables
		temp_points = []
		temp_tags = []
		# And keep only those results which are within 2 std of the mean
		temp_mean = mean[i]
		temp_std = std[i]
		for j in range(len(points)):
			# If the point is within two standard devs, keep it
			if (abs(points[j][i] - temp_mean)) < (2 * temp_std):
				temp_tags.append(tags[j])
				temp_points.append(points[j])
		# Update the arrays and repeat
		points = temp_points
		tags = temp_tags

	# Make into k-means array
	features = array(points)
	# whitened = whiten(features)
	# Generate the clusters
	clusters = kmeans2(features, k)

	# Get centroids and tags for points
	cluster_centroids = clusters[0]
	cluster_tags = clusters[1]

	# print(cluster_tags[0])
	# print(tags[0])

	# Assign cluster tag to tags array
	for i in range(len(cluster_tags)):
		tags[i].append(cluster_tags[i])

	tags = array(tags)

	# print(tags)
	# print(tags[:,1])
	# Create a figure
	if num_fields == 1:
		pl.hist(features)
		pl.hist(cluster_centroids[:,0])
	if num_fields == 2:
		pl.scatter(features[:,0], features[:,1], c=tags[:,1])
		pl.scatter(cluster_centroids[:,0], cluster_centroids[:,1], c='r')
	if num_fields == 3:
		fig = pl.figure()
		ax = Axes3D(fig)
		ax.scatter(features[:,0],features[:,1],features[:,2], c=tags[:,1])
		ax.scatter(cluster_centroids[:,0], cluster_centroids[:,1], cluster_centroids[:,2], c='r')
	pl.show()

	num_p = 0
	total_points = 0
	num_c = [0] * k
	num_c_given_p = [0] * k
	num_c_given_not_p = [0] * k

	# [p,c]
	for tag in tags:
		num_c[tag[1]] += 1
		total_points += 1
		if tag[0] == 1:
			num_p += 1
			num_c_given_p[tag[1]] += 1
		if tag[0] == 0:
			num_c_given_not_p[tag[1]] += 1

	prob_c = [1] * k
	prob_c_given_p = [1] * k
	prob_c_given_not_p = [1] * k

	prob_p = (num_p + laplace) / (total_points + (2 * laplace))
	prob_not_p = 1 - prob_p

	for c in range(len(num_c)):
		prob_c_given_p[c] = (num_c_given_p[c] + laplace) / (num_p + (k * laplace))
		prob_c_given_not_p[c] = (num_c_given_not_p[c] + laplace) / ((total_points - num_p) + (k * laplace))

	for c in range(len(num_c)):
		prob_c[c] = (num_c[c] + laplace) / (total_points + (k * laplace))

	node = {
		'k': k,
		'centroids': cluster_centroids,
		'prob_c': prob_c,
		'prob_c_given_p' : prob_c_given_p,
		'prob_c_given_not_p' : prob_c_given_not_p
	}

	return (node,prob_p)

# Take in a node and a point and return its partial
# Point must have the same fields as the node was created with
def naive_node(node, point):
	# Unpack the node
	k = node['k']
	centroids = node['centroids']
	prob_c = node['prob_c']
	prob_c_given_p = node['prob_c_given_p']
	prob_c_given_not_p = node['prob_c_given_not_p']

	tag = 0
	closest = sum_squared_errors(point, centroids[0])

	for i in range(k):
		distance = sum_squared_errors(point, centroids[i])
		if distance < closest:
			tag = i

	return ((prob_c_given_p[tag] / prob_c[tag]),(prob_c_given_not_p[tag] / prob_c[tag]))

def sum_squared_errors(point, centroid):
	return sum(pow((point - centroid),2))

def generate_training(table, fields):
	# Load in the connection global
	global connection

	# Create the cursor object
	cursor = connection.cursor()

	# Create the fields string and query
	columns = ",".join(fields)
	query = getColumnFromTable(columns, table)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()

	# Get the query results
	results = cursor.fetchall()

	# Get a random sampling of ~80% to train with
	results = random.sample(results, round(len(results)*.8))

	# Set Variables
	num_fields = len(fields)

	return rows_to_points_and_tags(results,num_fields)

# Main Function
if __name__ == '__main__':
	# Connect to the DB
	makeConnection()

	#try:
	print("Parkinsons Prediction Results:")

	##
	## Single Node Gaussian Bayes
	##

	# Get the stats for agg_per_hour overall
	# statsAggPerHourOverall()

	# Get the stats for agg_per_day overall
	statsAggPerDayOverall()

	# Get the stats for agg_per_day gb_dayofweek
	# statsAggPerDayGroupByDayOfWeek()

	##
	## Naive Bayes Approach
	##

	table = 'agg_per_day'
	fields = ['age', 'lat_var', 'long_var', 'gps_range','gps_traveled']

	points, tags = generate_training(table, fields)

	age = [[x[0]] for x in points]
	variance = [ [x[1],x[2]] for x in points]
	gps_range = [ [x[3]] for x in points]
	gps_traveled = [ [x[4]] for x in points]

	nodes = []
	age_node, prob_p = kmeans(age, tags, 2, 1)
	nodes.append(age_node)
	tags = [ [tag[0]] for tag in tags ]
	variance_node, prob_p = kmeans(variance, tags, 2, 1)
	nodes.append(variance_node)
	tags = [ [tag[0]] for tag in tags ]
	range_node, prob_p = kmeans(gps_range, tags, 2, 1)
	nodes.append(range_node)
	tags = [ [tag[0]] for tag in tags ]
	traveled_node, prob_p = kmeans(gps_traveled, tags, 2, 1)
	nodes.append(traveled_node)
	# and add more on...

	# Create the cursor object
	cursor = connection.cursor()

	# Create the fields string and query
	columns = ",".join(fields)
	query = getColumnFromTable(columns, table)

	try:
		cursor.execute(query)
	except ProgrammingError as e:
		print("Query Error --- {0}::{1}".format(e.errno, e))
		print(query)
		closeConnection()
		sys.exit()

	# Get the query results
	results = cursor.fetchall()

	# Get a random sampling of ~20% to test with
	results = random.sample(results, round(len(results)*.2))

	# Set Variables
	num_fields = len(fields)

	points, tags = rows_to_points_and_tags(results,num_fields)

	test_points = []

	age = [[x[0]] for x in points]
	test_points.append(age)
	variance = [ [x[1],x[2]] for x in points]
	test_points.append(variance)
	gps_range = [ [x[3]] for x in points]
	test_points.append(gps_range)
	gps_traveled = [ [x[4]] for x in points]
	test_points.append(gps_traveled)

	P = [1] * len(points)
	NP = [1] * len(points)

	for i in range(len(points)):
		for j in range(len(nodes)):
			partial_parkinsons, partial_not_parkinsons = naive_node(nodes[j], test_points[j][i])
			P[i] *= partial_parkinsons
			NP[i] *= partial_not_parkinsons
		P[i] *= prob_p
		NP[i] *= (1 - prob_p)
		if P[i] > NP[i]:
			tags[i].append(1)
		else:
			tags[i].append(0)

	probs = [];
	correct = 0
	for i in range(len(points)):
		probs.append([P,NP])
		if tags[i][0] == tags[i][1]:
			correct += 1

	print(prob_p)
	print(array(nodes))
	print(correct/len(points))
	