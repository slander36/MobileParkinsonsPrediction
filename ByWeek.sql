/*
Data Aggregation By Week
*/
USE parkinsons;

# Create Group By Unique Week View/Table

CREATE OR REPLACE VIEW GroupByWeekView
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
GROUP BY S.name, YEAR(G.time), WEEKOFYEAR(G.time);

CREATE TABLE NewGroupByWeek SELECT * FROM GroupByWeekView;
CREATE TABLE IF NOT EXISTS GroupByWeek (foo INT);
RENAME TABLE GroupByWeek TO OldGroupByWeek, NewGroupByWeek TO GroupByWeek;
DROP TABLE IF EXISTS OldGroupByWeek;

# Generate Covariance By Week

CREATE OR REPLACE VIEW CovByWeekView
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
FROM GroupByWeek;

CREATE TABLE NewCovByWeek SELECT * FROM CovByWeekView;
CREATE TABLE IF NOT EXISTS CovByWeek (foo INT);
RENAME TABLE CovByWeek TO OldCovByWeek, NewCovByWeek TO CovByWeek;
DROP TABLE IF EXISTS OldCovByWeek;

# Generate Correlation By Week

CREATE OR REPLACE VIEW CorByWeekView
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
FROM GroupByWeek;

CREATE TABLE NewCorByWeek SELECT * FROM CorByWeekView;
CREATE TABLE IF NOT EXISTS CorByWeek (foo INT);
RENAME TABLE CorByWeek TO OldCorByWeek, NewCorByWeek TO CorByWeek;
DROP TABLE IF EXISTS OldCorByWeek;