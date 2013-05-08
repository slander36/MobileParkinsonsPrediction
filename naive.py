# Default Python Libraries
import sys, math

# Libraries for MySql Connection
import mysql.connector
from mysql.connector import errorcode

# Libraries for SciPy
from stat import *
from numpy import array
from scipy.cluster.vq import vq, kmeans, whiten


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
	query = "SELECT parkinsons, {0} FROM {1} WHERE lat_var < 1 AND long_var < 1 {2}".format(column, agg, group_by_column)
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

	parkinsons_query = "SELECT * FROM {0} WHERE parkinsons = 1 AND lat_var < 1 AND long_var < 1".format(from_table)

	parkinsons_query = "SELECT COUNT(*) FROM ({0}) AS A {1}".format(parkinsons_query, group_by_statement)
	
	total_query = "SELECT * FROM {0} WHERE lat_var < 1 AND long_var < 1".format(from_table)

	total_query = "SELECT COUNT(*) FROM({0}) AS A {1}".format(total_query, group_by_statement)

	cursor = connection.cursor()
	
	cursor.execute(parkinsons_query)
	parkinsons_count = cursor.fetchall()

	cursor.execute(total_query)
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
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_var['mu'].append(result[1])
		lat_var['sig'].append(result[2])
	
	# print(lat_var['mu'], lat_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_var", agg)
	cursor.execute(query)
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
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_var['mu'].append(result[1])
		long_var['sig'].append(result[2])
	
	# print(long_var['mu'], long_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_var", agg)
	cursor.execute(query)
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

	lat_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, lat_traveled_mu, lat_traveled_sig "
		"FROM lat_traveled_per_day_overall_view "
		"WHERE lat_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_traveled['mu'].append(result[1])
		lat_traveled['sig'].append(result[2])
	
	# print(lat_traveled['mu'], lat_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_traveled", agg)
	cursor.execute(query)
	results = cursor.fetchall()

	lat_traveled_right = 0
	lat_traveled_count = 0
	for result in results:
		lat_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_traveled['mu'][0], lat_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_traveled['mu'][1], lat_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Traveled
	'''

	long_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Traveled trait
	query = ("SELECT "
		"parkinsons, long_traveled_mu, long_traveled_sig "
		"FROM long_traveled_per_day_overall_view "
		"WHERE long_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_traveled['mu'].append(result[1])
		long_traveled['sig'].append(result[2])
	
	# print(long_traveled['mu'], long_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_traveled", agg)
	cursor.execute(query)
	results = cursor.fetchall()

	long_traveled_right = 0
	long_traveled_count = 0
	for result in results:
		long_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_traveled['mu'][0], long_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_traveled['mu'][1], long_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			long_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Latitude Range
	'''

	lat_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, lat_range_mu, lat_range_sig "
		"FROM lat_range_per_day_overall_view "
		"WHERE lat_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_range['mu'].append(result[1])
		lat_range['sig'].append(result[2])
	
	# print(lat_range['mu'], lat_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_range", agg)
	cursor.execute(query)
	results = cursor.fetchall()

	lat_range_right = 0
	lat_range_count = 0
	for result in results:
		lat_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_range['mu'][0], lat_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_range['mu'][1], lat_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Range
	'''

	long_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Range trait
	query = ("SELECT "
		"parkinsons, long_range_mu, long_range_sig "
		"FROM long_range_per_day_overall_view "
		"WHERE long_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_range['mu'].append(result[1])
		long_range['sig'].append(result[2])
	
	# print(long_range['mu'], long_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_range", agg)
	cursor.execute(query)
	results = cursor.fetchall()

	long_range_right = 0
	long_range_count = 0
	for result in results:
		long_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_range['mu'][0], long_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_range['mu'][1], long_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			long_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Results
	'''

	print("Latitude Variance Classification Success Rate: {0}".format(lat_var_right/lat_var_count))

	print("Longitude Variance Classification Success Rate: {0}".format(long_var_right/long_var_count))
	
	print("Latitude Traveled Classification Success Rate: {0}".format(lat_traveled_right/lat_traveled_count))
	
	print("Longitude Traveled Classification Success Rate: {0}".format(long_traveled_right/long_traveled_count))
	
	print("Latitude Range Classification Success Rate: {0}".format(lat_range_right/lat_range_count))
	
	print("Longitude Range Classification Success Rate: {0}".format(long_range_right/long_range_count))

	# K-Means query
	query = getColumnFromTable("age, lat_var, long_var, lat_traveled, long_traveled, lat_range, long_range", 'agg_per_day')
	cursor.execute(query)
	results = cursor.fetchall()
	# (parkinsons, lat, long_var)

	# Store for k-means sampling
	point_array = []
	for row in results:
		point_array.append([row[1],row[2],row[3],row[4],row[5],row[6],row[7]])

	# Make into k-means array
	features = array(point_array)
	whitened = whiten(features)
	clusters = kmeans(whitened, 4)
	points = clusters[0]
	samples = []
	num_classes = len(points)
	count_class = [0] * num_classes
	count_class_given_p = [0] * num_classes
	count_P = 0
	count = 0
	has_park = False
	values = [0] * len(point_array)
	for row in results:
		count += 1
		has_park = False
		if row[0] == 1:
			has_park = True
			count_P +=1
		# Get squared error
		values = [row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
		se = (points-values)**2
		sse = [0] * num_classes
		for i in range(num_classes):
			sse[i] = sum(se[i])
		smallest = [-1,1000]
		for i in range(len(sse)):
			if sse[i] < smallest[1]:
				smallest[0] = i
				smallest[1] = sse[i]

		count_class[smallest[0]] += 1
		if has_park:
			count_class_given_p[smallest[0]] += 1
		
		samples.append([smallest[0],row[0]])

	# Calculate the probabilities using Laplacian Smoothing
	k = 1
	prob_class = [(x + k) / (count + num_classes * k) for x in count_class]
	prob_class_given_p = [(x + k) / (count_P + num_classes * k) for x in count_class_given_p]
	prob_P = (count_P + k) / (count + 2 * k)

	# Print CPDs
	print("Probability for each class:\n{0}\nSums to {1}".format(prob_class,sum(prob_class)))
	print("Probability to be in class X given Parkinsons:\n{0}\nSums to {1}".format(prob_class_given_p,sum(prob_class_given_p)))
	print("Probability to have Parkinsons:\n{0}".format(prob_P))

	num_right = 0
	num_total = 0
	for row in results:
		values = [row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
		sse = sum((values-points)**2)
		smallest = [-1,1000]
		for i in range(num_classes):
			if sse[i] < smallest[1]:
				smallest[0] = i
				smallest[1] = sse[i]
		prob_p_given_class = prob_class_given_p[smallest[0]]*prob_P/prob_class[smallest[0]]
		if prob_p_given_class > 0.5:
			if row[0] == 1:
				num_right += 1
		num_total += 1

	print("K-Means Accuracy: {0}".format(num_right / num_total))


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
	cursor.execute(query)
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
	cursor.execute(query)
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
	cursor.execute(query)
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
	cursor.execute(query)
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
	Latitude Traveled
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	lat_traveled = {
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
		"FROM lat_traveled_per_day_gb_dayofweek_view "
		"WHERE lat_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			lat_traveled['mu'][result[0]][0] = result[2]
			lat_traveled['sig'][result[0]][0] = result[3]
		else:
			lat_traveled['mu'][result[0]][1] = result[2]
			lat_traveled['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), lat_traveled", agg)
	cursor.execute(query)
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, lat_traveled)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = lat_traveled['mu'][dayofweek][0]
		npmu = lat_traveled['mu'][dayofweek][1]
		psig = lat_traveled['sig'][dayofweek][0]
		npsig = lat_traveled['sig'][dayofweek][1]

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
	print("Lat_Traveled :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Longitude Traveled
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	long_traveled = {
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
		"FROM long_traveled_per_day_gb_dayofweek_view "
		"WHERE long_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			long_traveled['mu'][result[0]][0] = result[2]
			long_traveled['sig'][result[0]][0] = result[3]
		else:
			long_traveled['mu'][result[0]][1] = result[2]
			long_traveled['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), long_traveled", agg)
	cursor.execute(query)
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, long_traveled)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = long_traveled['mu'][dayofweek][0]
		npmu = long_traveled['mu'][dayofweek][1]
		psig = long_traveled['sig'][dayofweek][0]
		npsig = long_traveled['sig'][dayofweek][1]

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
	print("Long_Traveled :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Latitude Range
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	lat_range = {
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
		"FROM lat_range_per_day_gb_dayofweek_view "
		"WHERE lat_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			lat_range['mu'][result[0]][0] = result[2]
			lat_range['sig'][result[0]][0] = result[3]
		else:
			lat_range['mu'][result[0]][1] = result[2]
			lat_range['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), lat_range", agg)
	cursor.execute(query)
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, lat_range)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = lat_range['mu'][dayofweek][0]
		npmu = lat_range['mu'][dayofweek][1]
		psig = lat_range['sig'][dayofweek][0]
		npsig = lat_range['sig'][dayofweek][1]

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
	print("Lat_Range :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))

	'''
	Longitude Range
	'''

	# mu 	: {dayOfWeek: [P,!P]...}
	# sig 	: {dayOfWeek: [P,!P]...}
	long_range = {
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
		"FROM long_range_per_day_gb_dayofweek_view "
		"WHERE long_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		# Add value to appropriate section
		if result[1] == 1:
			long_range['mu'][result[0]][0] = result[2]
			long_range['sig'][result[0]][0] = result[3]
		else:
			long_range['mu'][result[0]][1] = result[2]
			long_range['sig'][result[0]][1] = result[3]

	# Get query
	query = getColumnFromTable("name, DAYOFWEEK(record_day), long_range", agg)
	cursor.execute(query)
	results = cursor.fetchall()
	# (parkinsonsname, name, dayofweek, long_range)

	subjects = {}

	for row in results:
		parkinsons = row[0]
		name = row[1]
		dayofweek = row[2]
		v = row[3]

		pmu = long_range['mu'][dayofweek][0]
		npmu = long_range['mu'][dayofweek][1]
		psig = long_range['sig'][dayofweek][0]
		npsig = long_range['sig'][dayofweek][1]

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
	print("Long_Range :: {0} out of {1} correctly classified. {2}%".format(correct, counter, 100*correct/counter))



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
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_var['mu'].append(result[1])
		lat_var['sig'].append(result[2])
	
	# print(lat_var['mu'], lat_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_var", agg, group_by)
	cursor.execute(query)
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
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_var['mu'].append(result[1])
		long_var['sig'].append(result[2])
	
	# print(long_var['mu'], long_var['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_var", agg, group_by)
	cursor.execute(query)
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

	lat_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, lat_traveled_mu, lat_traveled_sig "
		"FROM lat_traveled_per_hour_overall_view "
		"WHERE lat_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_traveled['mu'].append(result[1])
		lat_traveled['sig'].append(result[2])
	
	# print(lat_traveled['mu'], lat_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_traveled", agg, group_by)
	cursor.execute(query)
	results = cursor.fetchall()

	lat_traveled_right = 0
	lat_traveled_count = 0
	for result in results:
		lat_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_traveled['mu'][0], lat_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_traveled['mu'][1], lat_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Traveled
	'''

	long_traveled = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Traveled trait
	query = ("SELECT "
		"parkinsons, long_traveled_mu, long_traveled_sig "
		"FROM long_traveled_per_hour_overall_view "
		"WHERE long_traveled_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_traveled['mu'].append(result[1])
		long_traveled['sig'].append(result[2])
	
	# print(long_traveled['mu'], long_traveled['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_traveled", agg, group_by)
	cursor.execute(query)
	results = cursor.fetchall()

	long_traveled_right = 0
	long_traveled_count = 0
	for result in results:
		long_traveled_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_traveled['mu'][0], long_traveled['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_traveled['mu'][1], long_traveled['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			long_traveled_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Latitude Range
	'''

	lat_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Latitude Traveled trait
	query = ("SELECT "
		"parkinsons, lat_range_mu, lat_range_sig "
		"FROM lat_range_per_hour_overall_view "
		"WHERE lat_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		lat_range['mu'].append(result[1])
		lat_range['sig'].append(result[2])
	
	# print(lat_range['mu'], lat_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("lat_range", agg, group_by)
	cursor.execute(query)
	results = cursor.fetchall()

	lat_range_right = 0
	lat_range_count = 0
	for result in results:
		lat_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], lat_range['mu'][0], lat_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], lat_range['mu'][1], lat_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			lat_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Longitude Range
	'''

	long_range = {'mu' : [], 'sig' : []}

	# Get the mean and std dev for the Longitude Range trait
	query = ("SELECT "
		"parkinsons, long_range_mu, long_range_sig "
		"FROM long_range_per_hour_overall_view "
		"WHERE long_range_sig <> 0"
	)
	cursor.execute(query)
	results = cursor.fetchall()

	# Store mean and std dev into their respective areas
	for result in results:
		long_range['mu'].append(result[1])
		long_range['sig'].append(result[2])
	
	# print(long_range['mu'], long_range['sig'])

	# Get sample data (all data)
	query = getColumnFromTable("long_range", agg, group_by)
	cursor.execute(query)
	results = cursor.fetchall()

	long_range_right = 0
	long_range_count = 0
	for result in results:
		long_range_count += 1

		# Calculate the partial probability that this is parkinsons, given the data
		p_prob = (prob_parkinsons) * (prob(result[1], long_range['mu'][0], long_range['sig'][0]))
		# Calculate the partial probability that this is NOT parkinsons, given the data
		not_p_prob = (1 - prob_parkinsons) * (prob(result[1], long_range['mu'][1], long_range['sig'][1]))
		evidence = p_prob + not_p_prob
		if result[0] is 1 and p_prob > not_p_prob:
			long_range_right += 1
		# print("Probability Parkinsons: {0}".format(p_prob/evidence))
		# print("Probability Not Parkinsons: {0}".format(not_p_prob/evidence))
		# print("Probability Sum: {0}".format(p_prob/evidence+not_p_prob/evidence))

	'''
	Results
	'''

	print("Latitude Variance Classification Success Rate: {0}".format(lat_var_right/lat_var_count))

	print("Longitude Variance Classification Success Rate: {0}".format(long_var_right/long_var_count))
	
	print("Latitude Traveled Classification Success Rate: {0}".format(lat_traveled_right/lat_traveled_count))
	
	print("Longitude Traveled Classification Success Rate: {0}".format(long_traveled_right/long_traveled_count))
	
	print("Latitude Range Classification Success Rate: {0}".format(lat_range_right/lat_range_count))
	
	print("Longitude Range Classification Success Rate: {0}".format(long_range_right/long_range_count))


# Main Function
if __name__ == '__main__':
	# Connect to the DB
	makeConnection()
	
	print("Parkinsons Prediction Results:")

	# Get the stats for agg_per_hour overall
	statsAggPerHourOverall()

	# Get the stats for agg_per_day overall
	statsAggPerDayOverall()

	# Get the stats for agg_per_day gb_dayofweek
	statsAggPerDayGroupByDayOfWeek()
	