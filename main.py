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
subjectNames = ['APPLE',
	'CHERRY',
	'CROCUS',
	'DAFODIL',
	'DAISY',
	'FLOX',
	'IRIS',
	'LILY',
	'MAPLE',
	'ORANGE',
	'ORCHID',
	'PEONY',
	'ROSE',
	'SUNFLOWER',
	'SWEETPEA',
	'VIOLET']

# Creates the Subjects if they don't exist
def checkSubjects():
	# Load in the connection global
	global connection
	checkConnection()

	# Create the cursor object
	cursor = connection.cursor()

	# Get Subject Names
	global subjectNames

	subjects = []
	subjects.append((subjectNames[0],0,1,77))
	subjects.append((subjectNames[1],1,0,55,51))
	subjects.append((subjectNames[2],1,1,46,41))
	subjects.append((subjectNames[3],0,1,42))
	subjects.append((subjectNames[4],1,1,54,52))
	subjects.append((subjectNames[5],1,1,57,47))
	subjects.append((subjectNames[6],1,1,65,45))
	subjects.append((subjectNames[7],0,0,53))
	subjects.append((subjectNames[8],1,1,55,46))
	subjects.append((subjectNames[9],0,1,57))
	subjects.append((subjectNames[10],1,1,69,65))
	subjects.append((subjectNames[11],1,1,80,67))
	subjects.append((subjectNames[12],0,1,55))
	subjects.append((subjectNames[13],0,1,67))
	subjects.append((subjectNames[14],0,0,77))
	subjects.append((subjectNames[15],1,0,55,43))

	checkSubject = ("SELECT * FROM Subject WHERE name=%(name)s")

	addSubjectWith = ("INSERT INTO Subject "
		"(name, parkinsons, male, age, ageDiagnosed) "
		"VALUES (%s, %s, %s, %s, %s)")

	addSubjectWithout = ("INSERT INTO Subject "
		"(name, parkinsons, male, age) "
		"VALUES (%s, %s, %s, %s)")

	for subject in subjects:
		# Erases any existing files
		csvInfile = open("{0}gps.csv".format(subject[0]),'w')
		csvInfile.close()

		cursor.execute(checkSubject,{'name': subject[0]})
		rows = cursor.fetchall()
		if len(rows) is not 0:
			continue
		if len(subject) is 5:
			# Add all fields
			cursor.execute(addSubjectWith, subject)
		else:
			# Add all but ageDiagnosed
			cursor.execute(addSubjectWithout, subject)

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

# Create default insert/select statements
insertGps = ("INSERT IGNORE INTO GPS "
	"(name, diffSecs, latitude, longitude, altitude, time) VALUES "
	"(\"%s\", %s, %s, %s, %s, \"%s\");")
selectGpsByDate = ("SELECT * FROM GPS "
	"WHERE date=\"%s\";")

# Load the current GPS CSV file
def createGpsInfile(file):
	# Load in the GPS Data Counter
	global gpsCounter

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

	# Trying something new
	# Write the queries to a sql file and run it instead
	csvInfile = open('{0}gps.csv'.format(name),'a')

	# Open the file
	csvfile = open(file)
	# Load it into a csv reader using default parameters for delimiter and quotechar
	csvreader = csv.reader(csvfile)

	# Skip the header
	next(csvreader)

	# Initialize the data
	data = []
	for row in csvreader:
		# Get the different 
		diffSec = row[0]
		latitude = row[1]
		longitude = row[2]
		altitude = row[3]
		time = row[4]

		# Write data to a list and use it to generate a sql statement
		data = (name, diffSec, latitude, longitude, altitude, time)
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
	global subjectNames

	loadDataLocalInfile = open('loadDataLocalInfile.sql','w')

	for subjectName in subjectNames:
		query = ("LOAD DATA LOCAL INFILE '{0}gps.csv' "
			"INTO TABLE GPS "
			"FIELDS TERMINATED BY ',' "
			"ENCLOSED BY '\"' "
			"LINES TERMINATED BY '\n';")
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
	'''
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
	'''