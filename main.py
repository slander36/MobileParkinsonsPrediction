# Default Python Libraries
import os, sys, re, csv

# Libraries for MySql Connection
import mysql.connector
from mysql.connector import errorcode

# Libraries for SciPy
from stat import *


'''
LOAD DATA FROM FILES TO DATABASE
'''


# Initialize the connection
connection = None

# Test the DB Connection
def testConnection():
	# Load in the connection global
	global connection
	connection = mysql.connector.connect(user='root',
		password='',
		host='127.0.0.1',
		database='parkinsons')
	print("Connected!")
	connection.close()


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


# Check the connection. Make it if missing.
def checkConnection():
	# Load in the connection global
	global connection
	if connection is None or not connection.is_connected():
		makeConnection()


# List of subject names for alternate usage
# Name: [hasParkinsons, isMale, currentAge, diagnosedAge]
subjects = {
	'APPLE':	[0,	1,	77,	'NULL'],
	'CHERRY':	[1,	0,	55,	55],
	'CROCUS':	[1,	1,	46, 41],
	'DAFODIL':	[0,	1,	42,	'NULL'],
	'DAISY':	[1,	1,	54,	52],
	'FLOX':		[1,	1,	57,	47],
	'IRIS':		[1,	1,	65,	45],
	'LILY':		[0,	0,	53,	'NULL'],
	'MAPLE':	[1,	1,	55,	46],
	'ORANGE':	[0,	1,	57,	'NULL'],
	'ORCHID':	[1,	1,	69,	65],
	'PEONY':	[1,	1,	80,	67],
	'ROSE':		[0,	1,	55,	'NULL'],
	'SUNFLOWER':[0,	0,	67,	'NULL'],
	'SWEETPEA':	[0,	0,	77,	'NULL'],
	'VIOLET':	[1,	0,	55,	43]
	}

# Creates the Subjects if they don't exist
def checkSubjects():
	# Load in the connection global
	global connection
	checkConnection()

	# Create the cursor object
	cursor = connection.cursor()

	# Get Subject Names
	global subjects

	checkSubject = ("SELECT * FROM Subject WHERE name=%(name)s")

	addSubject = ("INSERT INTO Subject "
		"(name, parkinsons, male, age, age_diagnosed) "
		"VALUES (%s, %s, %s, %s, %s)")

	for (name,data) in subjects.items():
		# Erases any existing files
		csvInfile = open("{0}gps.csv".format(name),'w')
		csvInfile.close()

		# Check to make sure Subject isn't already in DB
		cursor.execute(checkSubject,{'name': name})
		rows = cursor.fetchall()
		if len(rows) is not 0:
			continue

		# Add Subject to DB
		parkinsons = data[0]
		male = data[1]
		age = data[2]
		ageDiagnosed = data[3]
		subject = (name, parkinsons, male, age, ageDiagnosed)
		cursor.execute(addSubject, subject)

	connection.commit()


# Walk through the current directory tree
def walk(top):
	# Load in the connection global
	global connection
	checkConnection()

	for f in os.listdir(top):
		# Get the pathname
		pathname = os.path.join(top,f)

		# Get the file mode (directory or file)
		mode = os.stat(pathname).st_mode

		if S_ISDIR(mode):
			# It's a directory, recurse into it
			walk(pathname)
		elif S_ISREG(mode):
			# It's a file, call the callback function
			if re.search("hdl_gps_[A-Z]+_201\d+_\d+\.csv",pathname) is not None:
				createGpsInfile(pathname)
		else:
			# Unknown file type, print a message
			print("Skipping %s" % pathname)


# Counter for the GPS Data Insertion
gpsCounter = 0

# Load the current GPS CSV file
def createGpsInfile(file):
	# Load in the GPS Data Counter
	global gpsCounter

	# Load in the subjects dictionary
	global subjects

	# Get just the basename from the full path
	basename = os.path.basename(file)
	# Show the current file
	# print(basename)
	# Then split it into name and extension
	list = re.split('[.]', basename)
	# Grab only the filename
	filename = list[0]
	# Then split it into its parts based on the underscore
	list = re.split('_', filename)

	# Get the name of the subject form this
	name = list[2] # get the subject name
	if re.search('DAISY|DAISEY',name) is not None:
		name = 'DAISY'

	if re.search('LILY|LILLY',name) is not None:
		name = 'LILY'

	if name not in subjects.keys():
		return

	# Trying something new
	# Write the queries to a sql file and run it instead
	csvInfile = open('{0}gps.csv'.format(name),'a')

	# Open the file
	csvfile = open(file)
	# Load it into a csv reader using default parameters for delimiter and quotechar
	csvreader = csv.reader(csvfile)

	# Skip the header
	next(csvreader)

	# Get the static information
	parkinsons = subjects[name][0]
	male = subjects[name][1]
	age = subjects[name][2]
	ageDiagnosed = subjects[name][3]
	if ageDiagnosed is 'NULL':
		yearsDiagnosed = str(0)
	else:
		yearsDiagnosed = age - ageDiagnosed

	for row in csvreader:
		# Get the data from the row
		diffSec = row[0]
		latitude = row[1]
		longitude = row[2]
		altitude = row[3]
		time = row[4]

		# Write data to a list and use it to generate a sql statement
		data = (name,
			str(parkinsons),
			str(male),
			str(age),
			str(ageDiagnosed),
			str(yearsDiagnosed),
			diffSec,
			latitude,
			longitude,
			altitude,
			time)
		csvInfile.write("{}\n".format(','.join(data)))

	# Close the file
	csvInfile.close()

	# Inc and modulos the gpsCounter
	gpsCounter += 1
	if gpsCounter%60 is 0:
		print('.', end="")
		sys.stdout.flush()
		gpsCounter = 0


def loadGpsDataInfile():
	# Load in the connection global
	global connection

	# Create MySql Cursor
	cursor = connection.cursor()

	# Load in the subjectNames global
	global subjects

	loadDataLocalInfile = open('LoadDataLocalInfile.sql','w')

	for subjectName in subjects.keys():
		query = ("LOAD DATA LOCAL INFILE '{0}gps.csv' "
			"INTO TABLE GPS "
			"FIELDS TERMINATED BY ',' "
			"ENCLOSED BY '\"' "
			"LINES TERMINATED BY '\\n';\n")
		loadDataLocalInfile.write(query.format(subjectName));


'''
MAIN
'''


# Main Function
if __name__ == '__main__':
	# Make sure a DB exists
	testConnection()

	# Connect to the DB
	makeConnection()

	# Get the current directory to run the search on
	here = sys.argv[0].split('\\')
	here = here[0:-1]
	here = '\\'.join(here)
	print(here)
	here = '{0}\\Data'.format(here)

	# Finished Compiling GPS Data
	
	# Make sure the Subject table has people in it
	checkSubjects()
	print("Created All Subjects")

	# Walk the directory structure, creating the CSVs
	walk(here)
	print("Created All Subject's GPS CSVs")
	
	# Create the sql files that will load the CSVs using
	# LOAD DATA LOCAL INFILE
	# which the MySqlConnector doesn't have
	loadGpsDataInfile()
	print("Created SQL File to Load GPS CSVs")
	