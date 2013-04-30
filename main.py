import os, sys
from stat import *
import loadData

# Run Function -- Just checking to make sure things work
def run():
	print("Hello World")

# Main Function
if __name__ == '__main__':
	run()
	here = sys.argv[0].split('\\')
	here = here[0:-1]
	here = '\\'.join(here)
	print(here)
	here = '{0}\\Data'.format(here)
	loadData.walktree(here,loadData.visitfile)