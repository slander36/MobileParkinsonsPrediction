# Mobile Parkinson's Prediction
#### Using mobile phone data to help predict and monitor Parkinson's Disease

## About
This project was brought about because of the Michael J. Fox Parkinson's Challenge in Spring 2013. I was unable to complete an entry in time but kept the data. Using what was learned from contest submissions and other research, I hope to build a competitive model to those already found.

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
[MySQL Connector/Python][2]  

## Tools/Libraries Created
####aggregates MySQL Library
New aggregation functions created specifically for this project including:
+ **sumOfDifferences:**
	+ Description: Sums the differences between double values in a sequence
	+ Usage: In this case it is used to find the total distance traveled via GPS positions recorded in 1 second intervals

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