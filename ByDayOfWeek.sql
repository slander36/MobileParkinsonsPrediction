/*
Data Aggregation By Day Of Week
*/
USE parkinsons;

# Create Group By Unique Day Of Week View/Table

CREATE OR REPLACE VIEW GroupByDayOfWeekView
(
	Name,
	Age,
	Parkinsons,
	LatVar,
	LongVar,
	LatTraveled,
	LongTraveled,
	LatRange,
	LongRange
)
AS SELECT
	S.name,
	S.age,
	S.parkinsons,
	VARIANCE(G.latitude),
	VARIANCE(G.longitude),
	sumOfDifferences(G.latitude),
	sumOfDifferences(G.longitude),
	MAX(G.latitude)-MIN(G.latitude),
	MAX(G.longitude)-MIN(G.longitude)
FROM GPS AS G
JOIN Subject AS S ON G.name=S.name
GROUP BY S.name, DAYOFWEEK(G.time);

CREATE TABLE NewGroupByDayOfWeek SELECT * FROM GroupByDayOfWeekView;
CREATE TABLE IF NOT EXISTS GroupByDayOfWeek (foo INT);
RENAME TABLE GroupByDayOfWeek TO OldGroupByDayOfWeek, NewGroupByDayOfWeek TO GroupByDayOfWeek;
DROP TABLE IF EXISTS OldGroupByDayOfWeek;

# Generate Covariance By Day Of Week

CREATE OR REPLACE VIEW CovByDayOfWeekView
(
	CovAge,
	CovLatVar,
	CovLongVar,
	CovLatTraveled,
	CovLongTraveled,
	CovLatRange,
	CovLongRange
)
AS SELECT
	covariance(Parkinsons, Age),
	covariance(Parkinsons, LatVar),
	covariance(Parkinsons, LongVar),
	covariance(Parkinsons, LatTraveled),
	covariance(Parkinsons, LongTraveled),
	covariance(Parkinsons, LatRange),
	covariance(Parkinsons, LongRange)
FROM GroupByDayOfWeek;

CREATE TABLE NewCovByDayOfWeek SELECT * FROM CovByDayOfWeekView;
CREATE TABLE IF NOT EXISTS CovBydayOfWeek (foo INT);
RENAME TABLE CovByDayOfWeek TO OldCovByDayOfWeek, NewCovByDayOfWeek TO CovByDayOfWeek;
DROP TABLE IF EXISTS OldCovByDayOfWeek;

# Generate Correlation By Day Of Week

CREATE OR REPLACE VIEW CorByDayOfWeekView
(
	CorAge,
	CorLatVar,
	CorLongVar,
	CorLatTraveled,
	CorLongTraveled,
	CorLatRange,
	CorLongRange
)
AS SELECT
	covariance(Parkinsons, Age) / (STDDEV(Parkinsons)*STDDEV(Age)),
	covariance(Parkinsons, LatVar) / (STDDEV(Parkinsons)*STDDEV(LatVar)),
	covariance(Parkinsons, LongVar) / (STDDEV(Parkinsons)*STDDEV(LongVar)),
	covariance(Parkinsons, LatTraveled) / (STDDEV(Parkinsons)*STDDEV(LatTraveled)),
	covariance(Parkinsons, LongTraveled) / (STDDEV(Parkinsons)*STDDEV(LongTraveled)),
	covariance(Parkinsons, LatRange) / (STDDEV(Parkinsons)*STDDEV(LatRange)),
	covariance(Parkinsons, LongRange) / (STDDEV(Parkinsons)*STDDEV(LongRange))
FROM GroupByDayOfWeek;

CREATE TABLE NewCorByDayOfWeek SELECT * FROM CorByDayOfWeekView;
CREATE TABLE IF NOT EXISTS CorByDayOfWeek (foo INT);
RENAME TABLE CorByDayOfWeek TO OldCorByDayOfWeek, NewCorByDayOfWeek TO CorByDayOfWeek;
DROP TABLE IF EXISTS OldCorByDayOfWeek;