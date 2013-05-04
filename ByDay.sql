/*
Data Aggregation By Day
*/
USE parkinsons;

# Create Group By Unique Day View/Table

CREATE OR REPLACE VIEW GroupByDayView
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
GROUP BY S.name, YEAR(G.time), DAYOFYEAR(G.time);

CREATE TABLE NewGroupByDay SELECT * FROM GroupByDayView;
CREATE TABLE IF NOT EXISTS GroupByDay (foo INT);
RENAME TABLE GroupByDay TO OldGroupByDay, NewGroupByDay TO GroupByDay;
DROP TABLE IF EXISTS OldGroupByDay;

# Generate Covariance By Day

CREATE OR REPLACE VIEW CovByDayView
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
FROM GroupByDay;

CREATE TABLE NewCovByDay SELECT * FROM CovByDayView;
CREATE TABLE IF NOT EXISTS CovByDay (foo INT);
RENAME TABLE CovByDay TO OldCovByDay, NewCovByDay TO CovByDay;
DROP TABLE IF EXISTS OldCovByDay;

# Generate Correlation By Day

CREATE OR REPLACE VIEW CorByDayView
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
FROM GroupByDay;

CREATE TABLE NewCorByDay SELECT * FROM CorByDayView;
CREATE TABLE IF NOT EXISTS CorByDay (foo INT);
RENAME TABLE CorByDay TO OldCorByDay, NewCorByDay TO CorByDay;
DROP TABLE IF EXISTS OldCorByDay;