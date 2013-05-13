# Mobile Parkinson's Prediction
#### Using mobile phone data to help predict and monitor Parkinson's Disease

## About
This project was brought about because of the Michael J. Fox Parkinson's Challenge in Spring 2013. I was unable to complete an entry in time but kept the data. Using what was learned from contest submissions and other research, I hope to build a competitive model to those already found.

## How To
Before anything else, the Parkinson's dataset must be acquired from the Michael J. Fox Parkinson's Challenge site. The folder layout after being unzipped should be all patient folders in a single folder named Data. This is used by main.py to parse through and compile all GPS data into single files to be used by MySQL.  
This project requires Python 3.2, 32 bit in order to work with SciPy, and an installation of MySQL as well as a database "parkinsons".  
Once the tools and libraries have been installed, navigate your console to the project folder and run
```sql
source prepdb.sql
```
in order to create the correct tables.  
After the tables have been created, open main.py and uncomment the lines containing any function calls (anything that isn't plain text).  
Once the lines have been uncommented, run main.py, which will build your database and the composite files from the Data folder containing the Parkinson's data.  
Once main.py has completed running, return to mysql and run
```sql
source loadDataLocalInfile.sql
```
This will take a while, as it is loading the ~900MB of GPS data into your database.  
After this is complete, it is time to build the aggregation functions dll/so for MySQL. The MySQL Functions folder has a CMake file which needs to point to your MySQL includes folder in order to create the project successfully. Once CMake has been installed and the list file configured, run the CMake command appropriate to your Visual Studio version.  
NOTE: If you are not on Windows, please refer to MySQL's User Defined Functions guide on how to build the aggregates.c file.  
Once the project has been built, use the MSBuild command below to build the dll. Take the new dll from the Releases folder and drop it in your MySQL's lib/plugin foler.  
Once the dll has been placed, run the CREATE AGGREGATE FUNCTION commands below for `sumEuclidianDistance` and `covariance`.  
After creating those two functions in MySQL, feel free to run
```sql
source AggPerHour.sql
```
to build up the agg_per_hour table and helper views.  
After all of this is done, scroll to the bottom of navie.py and edit it as you will to build new versions of the naive bayes net, or just run it as is to get the last network run.

## Author
@slander36  
Graduate Student  
University of Missouri - Columbia  
Computer Science Department

## Tools Used
[MySQL Database][3]  
[Sublime Text 2][4]  
[Github][5]  
[CMake][6]  
[Visual Studio 11][7]  

## Libraries Used
[SciPy][1]  
[NumPy][9]  
[MySQL Connector/Python][2]  
[MatPlotLib][8]  

## Tools/Libraries Created
#### **aggregates** MySQL Library - Found in MySQL_Functions
New aggregation functions created specifically for this project. They were created using CMake and Visual Studio 2012, but should be compilable on any system.  
They include:
+ **sumOfDifferences:**
	+ Description: Sums the differences between double values in a sequence
	+ Usage: In this case it is used to find the total distance traveled via GPS positions recorded in 1 second intervals
+ **covariance:**
	+ Description: Calculates the covariance between two selected columns
	+ Usage: Used to find covariance and correlation between different random variables of the patients such as {age, distance traveled}, {gps variance, parkinsons}, etc

## Useful Commands
```batchfile
cmake -G "Visual Studio 11 Win64"  
MSBuild xxx.sln /p:Configuration=Release  
```  
```sql
CREATE AGGREGATE FUNCTION function RETURNS REAL SONAME 'myfunctionlibrary.dll';  
DROP FUNCTION function;  
CREATE VIEW viewName (col1, col2, ...) AS SELECT colA, colB ... FROM ... ;
```

[1]: http://www.scipy.org/
[2]: http://dev.mysql.com/doc/refman/5.7/en/connector-python.html
[3]: http://www.mysql.com/
[4]: http://www.sublimetext.com/2
[5]: https://www.github.com
[6]: http://www.cmake.org/
[7]: http://www.microsoft.com/visualstudio/eng
[8]: http://www.matplotlib.org
[9]: http://www.scipy.org/