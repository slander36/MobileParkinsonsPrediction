'''
Import libraries
'''
import os, sys, re
from stat import *

def walktree(top, callback):
	'''
	Descend through the Data directory pulling out all
	files and acting on them using callback
	'''

	count = 1;
	for f in os.listdir(top):
		# Remove if not testing
		if count > 100:
			break
		pathname = os.path.join(top,f)
		mode = os.stat(pathname).st_mode
		if S_ISDIR(mode):
			# It's a directory, recurse into it
			walktree(pathname, callback)
		elif S_ISREG(mode):
			# It's a file, call the callback function
			callback(pathname)
		else:
			# Unknown file type, print a message
			print("Skipping %s" % pathname)
		count += 1

def visitfile(file):
	if re.search("hdl_gps_[A-Z]+_201\d+_\d+\.csv",file) is not None:
		print ('visiting', file)