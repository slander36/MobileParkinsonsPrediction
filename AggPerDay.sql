/*
Data Aggregation By Day
*/
USE parkinsons;

# Create Group By Unique Day View/Table

CREATE OR REPLACE VIEW agg_per_day_view
(
	record_day,
	name,
	age,
	parkinsons,
	lat_var,
	long_var,
	gps_traveled,
	gps_range
)
AS SELECT
	DATE(G.time) AS record_day,
	S.name AS name,
	S.age AS age,
	S.parkinsons AS parkinsons,
	VARIANCE(G.latitude) AS lat_var,
	VARIANCE(G.longitude) AS log_var,
	sumEuclidianDistance(G.latitude, G.longitude) As gps_traveled,
	SQRT(POW(MAX(G.latitude)-MIN(G.latitude),2) + POW(MAX(G.longitude)-MIN(G.longitude),2)) AS gps_range
FROM GPS AS G
JOIN Subject AS S ON G.name=S.name
GROUP BY S.name, DATE(G.time);

DROP TABLE IF EXISTS agg_per_day;
CREATE TABLE IF NOT EXISTS agg_per_day SELECT * FROM agg_per_day_view;

# Generate Covariance By Day - Overall

CREATE OR REPLACE VIEW cov_per_day_overall_view
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
FROM agg_per_day;

# Generate Correlation By Day - Overall

CREATE OR REPLACE VIEW cor_per_day_overall_view
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
FROM agg_per_day;

# Generate Mean and Std Dev View for each variable - Overall

CREATE OR REPLACE VIEW lat_var_per_day_overall_view
(
	parkinsons,
	lat_var_mu,
	lat_var_sig
)
AS SELECT
	parkinsons,
	avg(lat_var),
	VARIANCE(lat_var)
FROM agg_per_day
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_var_per_day_overall_view
(
	parkinsons,
	long_var_mu,
	long_var_sig
)
AS SELECT
	parkinsons,
	avg(long_var),
	VARIANCE(long_var)
FROM agg_per_day
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_traveled_per_day_overall_view
(
	parkinsons,
	gps_traveled_mu,
	gps_traveled_sig
)
AS SELECT
	parkinsons,
	avg(gps_traveled),
	VARIANCE(gps_traveled)
FROM agg_per_day
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_range_per_day_overall_view
(
	parkinsons,
	gps_range_mu,
	gps_range_sig
)
AS SELECT
	parkinsons,
	avg(gps_range),
	VARIANCE(gps_range)
FROM agg_per_day
GROUP BY parkinsons
ORDER BY parkinsons DESC;

# Generate Covariance By Day - Grouped By Day Of Week

CREATE OR REPLACE VIEW cov_per_day_gb_dayofweek_view
(
	day_of_week,
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_gps_traveled,
	cov_gps_range
)
AS SELECT
	DAYOFWEEK(record_day),
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, gps_traveled),
	covariance(parkinsons, gps_range)
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day);

# Generate Correlation By Day - Grouped By Day Of Week

CREATE OR REPLACE VIEW cor_per_day_gb_dayofweek_view
(
	day_of_week,
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_gps_traveled,
	cor_gps_range
)
AS SELECT
	DAYOFWEEK(record_day),
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, gps_traveled) / (STDDEV(parkinsons)*STDDEV(gps_traveled)),
	covariance(parkinsons, gps_range) / (STDDEV(parkinsons)*STDDEV(gps_range))
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day);

# Generate Mean and Std Dev View for each variable - Grouped By Day Of Week

CREATE OR REPLACE VIEW lat_var_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	lat_var_mu,
	lat_var_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(lat_var),
	VARIANCE(lat_var)
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_var_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	long_var_mu,
	long_var_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(long_var),
	VARIANCE(long_var)
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_traveled_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	gps_traveled_mu,
	gps_traveled_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(gps_traveled),
	VARIANCE(gps_traveled)
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_range_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	gps_range_mu,
	gps_range_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(gps_range),
	VARIANCE(gps_range)
FROM agg_per_day
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;


# Generate Covariance By Day - Grouped By Week

CREATE OR REPLACE VIEW cov_per_day_gb_week_view
(
	week,
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_gps_traveled,
	cov_gps_range
)
AS SELECT
	DATE_FORMAT(record_day, '%Y %u'),
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, gps_traveled),
	covariance(parkinsons, gps_range)
FROM agg_per_day
GROUP BY DATE_FORMAT(record_day, '%Y %u');

# Generate Correlation By Day - Grouped By Week

CREATE OR REPLACE VIEW cor_per_day_gb_week_view
(
	week,
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_gps_traveled,
	cor_gps_range
)
AS SELECT
	DATE_FORMAT(record_day, '%Y %u'),
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, gps_traveled) / (STDDEV(parkinsons)*STDDEV(gps_traveled)),
	covariance(parkinsons, gps_range) / (STDDEV(parkinsons)*STDDEV(gps_range))
FROM agg_per_day
GROUP BY DATE_FORMAT(record_day, '%Y %u');

# Generate Mean and Std Dev View for each variable - Grouped By Week

CREATE OR REPLACE VIEW lat_var_per_day_gb_week_view
(
	parkinsons,
	lat_var_mu,
	lat_var_sig
)
AS SELECT
	parkinsons,
	avg(lat_var),
	VARIANCE(lat_var)
FROM agg_per_day
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_var_per_day_gb_week_view
(
	parkinsons,
	long_var_mu,
	long_var_sig
)
AS SELECT
	parkinsons,
	avg(long_var),
	VARIANCE(long_var)
FROM agg_per_day
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_traveled_per_day_gb_week_view
(
	parkinsons,
	gps_traveled_mu,
	gps_traveled_sig
)
AS SELECT
	parkinsons,
	avg(gps_traveled),
	VARIANCE(gps_traveled)
FROM agg_per_day
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW gps_range_per_day_gb_week_view
(
	parkinsons,
	gps_range_mu,
	gps_range_sig
)
AS SELECT
	parkinsons,
	avg(gps_range),
	VARIANCE(gps_range)
FROM agg_per_day
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;