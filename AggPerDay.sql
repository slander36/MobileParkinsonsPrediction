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
	lat_traveled,
	long_traveled,
	lat_range,
	long_range
)
AS SELECT
	DATE(G.time),
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
GROUP BY S.name, DATE(G.time);

CREATE TABLE IF NOT EXISTS agg_per_day SELECT * FROM agg_per_day_view;

# Generate Covariance By Day - Overall

CREATE OR REPLACE VIEW cov_per_day_overall_view
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
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1;

# Generate Correlation By Day - Overall

CREATE OR REPLACE VIEW cor_per_day_overall_view
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
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1;

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
WHERE lat_var < 1 AND long_var < 1
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
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_traveled_per_day_overall_view
(
	parkinsons,
	lat_traveled_mu,
	lat_traveled_sig
)
AS SELECT
	parkinsons,
	avg(lat_traveled),
	VARIANCE(lat_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_traveled_per_day_overall_view
(
	parkinsons,
	long_traveled_mu,
	long_traveled_sig
)
AS SELECT
	parkinsons,
	avg(long_traveled),
	VARIANCE(long_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_range_per_day_overall_view
(
	parkinsons,
	lat_range_mu,
	lat_range_sig
)
AS SELECT
	parkinsons,
	avg(lat_range),
	VARIANCE(lat_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_range_per_day_overall_view
(
	parkinsons,
	long_range_mu,
	long_range_sig
)
AS SELECT
	parkinsons,
	avg(long_range),
	VARIANCE(long_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons
ORDER BY parkinsons DESC;

# Generate Covariance By Day - Grouped By Day Of Week

CREATE OR REPLACE VIEW cov_per_day_gb_dayofweek_view
(
	day_of_week,
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_lat_traveled,
	cov_long_traveled,
	cov_lat_range,
	cov_long_range
)
AS SELECT
	DAYOFWEEK(record_day),
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, lat_traveled),
	covariance(parkinsons, long_traveled),
	covariance(parkinsons, lat_range),
	covariance(parkinsons, long_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day);

# Generate Correlation By Day - Grouped By Day Of Week

CREATE OR REPLACE VIEW cor_per_day_gb_dayofweek_view
(
	day_of_week,
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_lat_traveled,
	cor_long_traveled,
	cor_lat_range,
	cor_long_range
)
AS SELECT
	DAYOFWEEK(record_day),
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, lat_traveled) / (STDDEV(parkinsons)*STDDEV(lat_traveled)),
	covariance(parkinsons, long_traveled) / (STDDEV(parkinsons)*STDDEV(long_traveled)),
	covariance(parkinsons, lat_range) / (STDDEV(parkinsons)*STDDEV(lat_range)),
	covariance(parkinsons, long_range) / (STDDEV(parkinsons)*STDDEV(long_range))
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
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
WHERE lat_var < 1 AND long_var < 1
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
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_traveled_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	lat_traveled_mu,
	lat_traveled_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(lat_traveled),
	VARIANCE(lat_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_traveled_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	long_traveled_mu,
	long_traveled_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(long_traveled),
	VARIANCE(long_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_range_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	lat_range_mu,
	lat_range_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(lat_range),
	VARIANCE(lat_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_range_per_day_gb_dayofweek_view
(
	dayofweek,
	parkinsons,
	long_range_mu,
	long_range_sig
)
AS SELECT
	DAYOFWEEK(record_day),
	parkinsons,
	avg(long_range),
	VARIANCE(long_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DAYOFWEEK(record_day), parkinsons
ORDER BY parkinsons DESC;


# Generate Covariance By Day - Grouped By Week

CREATE OR REPLACE VIEW cov_per_day_gb_week_view
(
	week,
	cov_age,
	cov_lat_var,
	cov_long_var,
	cov_lat_traveled,
	cov_long_traveled,
	cov_lat_range,
	cov_long_range
)
AS SELECT
	DATE_FORMAT(record_day, '%Y %u'),
	covariance(parkinsons, age),
	covariance(parkinsons, lat_var),
	covariance(parkinsons, long_var),
	covariance(parkinsons, lat_traveled),
	covariance(parkinsons, long_traveled),
	covariance(parkinsons, lat_range),
	covariance(parkinsons, long_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY DATE_FORMAT(record_day, '%Y %u');

# Generate Correlation By Day - Grouped By Week

CREATE OR REPLACE VIEW cor_per_day_gb_week_view
(
	week,
	cor_age,
	cor_lat_var,
	cor_long_var,
	cor_lat_traveled,
	cor_long_traveled,
	cor_lat_range,
	cor_long_range
)
AS SELECT
	DATE_FORMAT(record_day, '%Y %u'),
	covariance(parkinsons, age) / (STDDEV(parkinsons)*STDDEV(age)),
	covariance(parkinsons, lat_var) / (STDDEV(parkinsons)*STDDEV(lat_var)),
	covariance(parkinsons, long_var) / (STDDEV(parkinsons)*STDDEV(long_var)),
	covariance(parkinsons, lat_traveled) / (STDDEV(parkinsons)*STDDEV(lat_traveled)),
	covariance(parkinsons, long_traveled) / (STDDEV(parkinsons)*STDDEV(long_traveled)),
	covariance(parkinsons, lat_range) / (STDDEV(parkinsons)*STDDEV(lat_range)),
	covariance(parkinsons, long_range) / (STDDEV(parkinsons)*STDDEV(long_range))
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
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
WHERE lat_var < 1 AND long_var < 1
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
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_traveled_per_day_gb_week_view
(
	parkinsons,
	lat_traveled_mu,
	lat_traveled_sig
)
AS SELECT
	parkinsons,
	avg(lat_traveled),
	VARIANCE(lat_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_traveled_per_day_gb_week_view
(
	parkinsons,
	long_traveled_mu,
	long_traveled_sig
)
AS SELECT
	parkinsons,
	avg(long_traveled),
	VARIANCE(long_traveled)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW lat_range_per_day_gb_week_view
(
	parkinsons,
	lat_range_mu,
	lat_range_sig
)
AS SELECT
	parkinsons,
	avg(lat_range),
	VARIANCE(lat_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;

CREATE OR REPLACE VIEW long_range_per_day_gb_week_view
(
	parkinsons,
	long_range_mu,
	long_range_sig
)
AS SELECT
	parkinsons,
	avg(long_range),
	VARIANCE(long_range)
FROM agg_per_day
WHERE lat_var < 1 AND long_var < 1
GROUP BY parkinsons, DATE_FORMAT(record_day, '%Y %u')
ORDER BY parkinsons DESC;