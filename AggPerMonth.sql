/*
Data Aggregation By Month
*/
USE parkinsons;

# Create Group By Month View/Table

CREATE OR REPLACE VIEW agg_per_month_view
(
	record_month,
	name,
	age,
	parkinsons,
	lat_var,
	long_var,
	lat_traveled,
	long_traveled,
	lat_range,
	long_range
)
AS SELECT
	DATE_FORMAT(G.time, '%Y-%m'),
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
GROUP BY S.name, DATE_FORMAT(G.time, '%Y-%m');

CREATE TABLE new_agg_per_month SELECT * FROM agg_per_month_view;

# Generate Covariance By Month

CREATE OR REPLACE VIEW cov_per_month_overall_view
(
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_lat_traveled,
	cov_long_traveled,
	cov_lat_range,
	cov_long_range
)
AS SELECT
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, lat_traveled),
	covariance(parkinsons, long_traveled),
	covariance(parkinsons, lat_range),
	covariance(parkinsons, long_range)
FROM agg_per_month
WHERE lat_range < 1 AND long_range < 1;

# Generate Correlation By Month - Overall

CREATE OR REPLACE VIEW cor_per_month_overall_view
(
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_lat_traveled,
	cor_long_traveled,
	cor_lat_range,
	cor_long_range
)
AS SELECT
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, lat_traveled) / (STDDEV(parkinsons)*STDDEV(lat_traveled)),
	covariance(parkinsons, long_traveled) / (STDDEV(parkinsons)*STDDEV(long_traveled)),
	covariance(parkinsons, lat_range) / (STDDEV(parkinsons)*STDDEV(lat_range)),
	covariance(parkinsons, long_range) / (STDDEV(parkinsons)*STDDEV(long_range))
FROM agg_per_month
WHERE lat_range < 1 AND long_range < 1;