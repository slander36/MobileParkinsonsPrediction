USE parkinsons;

DROP TABLE IF EXISTS GPS;
DROP TABLE IF EXISTS Subject;

CREATE TABLE Subject (
	name VARCHAR(16),
	parkinsons TINYINT(1) NOT NULL,
	male TINYINT(1) NOT NULL,
	age INT(3) NOT NULL,
	age_diagnosed INT(3) DEFAULT 0,
	PRIMARY KEY (name)
) ENGINE=InnoDB;

CREATE TABLE GPS (
	name VARCHAR(16),
	parkinsons TINYINT(1) NOT NULL,
	male TINYINT(1) NOT NULL,
	age INT(3) NOT NULL,
	age_diagnosed INT(3) DEFAULT NULL,
	years_diagnosed INT(2) DEFAULT 0,
	diff_secs DOUBLE NOT NULL,
	latitude DOUBLE NOT NULL,
	longitude DOUBLE NOT NULL,
	altitude DOUBLE NOT NULL,
	time DATETIME NOT NULL,
	PRIMARY KEY (name, time),
	FOREIGN KEY (name) REFERENCES Subject (name) ON DELETE CASCADE
) ENGINE=InnoDB;