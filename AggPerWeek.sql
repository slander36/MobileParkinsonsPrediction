/*
Data Aggregation By Week
*/
USE parkinsons;

# Create Group By Unique Week View/Table

CREATE OR REPLACE VIEW agg_per_week_view
(
	record_week,
	name,
	age,
	parkinsons,
	lat_var,
	long_var,
	gps_traveled,
	gps_range
)
AS SELECT
	DATE_FORMAT(G.time, '%Y-%u'),
	S.name,
	S.age,
	S.parkinsons,
	VARIANCE(G.latitude),
	VARIANCE(G.longitude),
	sumEuclidianDistance(G.latitude, G.longitude) As gps_traveled,
	SQRT(POW(MAX(G.latitude)-MIN(G.latitude),2) + POW(MAX(G.longitude)-MIN(G.longitude),2)) AS gps_range
FROM GPS AS G
JOIN Subject AS S ON G.name=S.name
GROUP BY S.name, DATE_FORMAT(G.time, '%Y-%u');

CREATE TABLE IF NOT EXISTS agg_per_week SELECT * FROM agg_per_week_view;

# Generate Covariance By Week - Overall

CREATE OR REPLACE VIEW cov_per_week_overall_view
(
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_gps_traveled,
	cov_gps_range
)
AS SELECT
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, gps_traveled),
	covariance(parkinsons, gps_range)
FROM agg_per_week;

# Generate Correlation By Week - Overall

CREATE OR REPLACE VIEW cor_per_week_overall_view
(
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_gps_traveled,
	cor_gps_range
)
AS SELECT
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, gps_traveled) / (STDDEV(parkinsons)*STDDEV(gps_traveled)),
	covariance(parkinsons, gps_range) / (STDDEV(parkinsons)*STDDEV(gps_range))
FROM agg_per_week;