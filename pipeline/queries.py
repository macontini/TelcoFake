from conf import config

tbl_name = config['sql']['tbl_name']
columns = [
    'train_id', 'timestamp', 'temp_celsius', 'status',
    'speed_kmh', 'latitude', 'longitude', 'signal_strength_dbm',
    'cell_tower_id', 'power_consumption_kw', 'network_type', 'line_segment'
]

# ----- DDL: Creazione tabelle -----
CREATE_TBL_TELEMETRY = f"""
CREATE TABLE IF NOT EXISTS {tbl_name} (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    train_id                TEXT NOT NULL,
    timestamp               DATETIME NOT NULL,
    temp_celsius            REAL,
    status                  TEXT,
    speed_kmh               REAL,
    latitude                REAL,
    longitude               REAL,
    signal_strength_dbm     INTEGER,
    cell_tower_id           TEXT,
    power_consumption_kw    REAL,
    network_type            TEXT,
    line_segment            TEXT,

    UNIQUE(train_id, timestamp) -- Evita duplicati
);
"""

# ----- DML: Inserimento -----
INSERT_TELEMETRY = f"""
INSERT OR IGNORE INTO {tbl_name} (
    train_id,
    timestamp,
    temp_celsius,
    status,
    speed_kmh,
    latitude,
    longitude,
    signal_strength_dbm,
    cell_tower_id,
    power_consumption_kw,
    network_type,
    line_segment
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

# --- ANALYTICS: monitoraggio ---
# 1
HOURLY_SPEED_ANALYSIS = """
SELECT
    STRFTIME('%H', timestamp) AS hour,
    AVG(speed_kmh)
FROM train_telemetry
GROUP BY hour;
"""

# 2
CRITICAL_TRAINS_REPORT = """
SELECT
    train_id,
    COUNT(status) AS critical_count
FROM train_telemetry
WHERE status = 'CRITICAL'
GROUP BY train_id
ORDER BY critical_count DESC
LIMIT 10;
"""

# 3
SIGNAL_PERFORMANCE_ANALYSIS = """
SELECT
    CASE
        WHEN speed_kmh <= 50  THEN '01: 0-50'
        WHEN speed_kmh <= 100 THEN '02: 50-100'
        WHEN speed_kmh <= 150 THEN '03: 100-150'
        WHEN speed_kmh <= 200 THEN '04: 150-200'
        WHEN speed_kmh <= 250 THEN '05: 200-250'
        ELSE '06: 250-300'
    END AS speed_ranges,
    AVG(signal_strength_dbm) AS avg_signal_str
FROM train_telemetry
GROUP BY speed_ranges
ORDER BY speed_ranges ASC;
"""

# 4
ANOMALY_WINDOW_DETECTION = """
WITH previous_values AS (
    SELECT
        train_id,
        timestamp,
        temp_celsius,
        LAG(timestamp) OVER (PARTITION BY train_id ORDER BY timestamp ASC) AS previous_timestamp,
        LAG(temp_celsius) OVER (PARTITION BY train_id ORDER BY timestamp ASC) AS previous_temp
    FROM train_telemetry
)
SELECT
    train_id,
    ABS(timestamp - previous_timestamp) AS delta_timestamp,
    ABS(temp_celsius - previous_temp) AS delta_temp
FROM previous_values
WHERE previous_temp IS NOT NULL
    AND delta_temp > 30
ORDER BY delta_temp DESC
LIMIT 30;
"""

# 5
WORST_SIGNAL_SEGMENT = """
SELECT
	line_segment,
	ROUND(SUM(
        CASE
		  WHEN signal_strength_dbm < -100 OR network_type = 'OFFLINE'
		  THEN 1 ELSE 0
		END
	) * 1.0 / COUNT(*), 4) AS blackout_ratio
FROM train_telemetry
GROUP BY line_segment;
"""

# 6
TOP_POWER_SPEED_RATIO = """
SELECT
	train_id,
	SUM(power_consumption_kw) AS power_sum,
	SUM(speed_kmh) AS speed_sum,
    ROUND(SUM(power_consumption_kw) * 1.0 / NULLIF(SUM(speed_kmh), 0), 2) AS ratio
FROM train_telemetry
WHERE speed_kmh > 10
GROUP BY train_id
ORDER BY ratio DESC
LIMIT 15;
"""

# 7
HIGHEST_SIGNAL_DROP = """
WITH previous_values AS (
	SELECT
		cell_tower_id,
		timestamp,
		signal_strength_dbm,
		LAG(timestamp) OVER (PARTITION BY cell_tower_id ORDER BY timestamp) AS previous_timestamp,
		LAG(signal_strength_dbm) OVER (PARTITION BY cell_tower_id ORDER BY timestamp) AS previous_signal
	FROM train_telemetry
)
SELECT
	cell_tower_id,
	ROUND(
		(JULIANDAY(timestamp) - JULIANDAY(previous_timestamp)) * 1440, 0
	) AS delta_minutes,
	ABS(previous_signal - signal_strength_dbm) AS signal_drop
FROM previous_values
WHERE
	delta_minutes < 5		-- Sbalzi entro 5 minuti
	AND signal_drop > 50	-- Soglia critica (max: 70 by def)
ORDER BY signal_drop DESC
LIMIT 15;
"""

# 8
RECURRENTLY_CRITICAL_TRAINS = """
WITH cte AS (
	SELECT
		train_id,
		COUNT(*) AS row_count,
		SUM(CASE
				WHEN status = 'CRITICAL'
				THEN 1 ELSE 0
			END) AS critical_count
	FROM train_telemetry
	GROUP BY train_id
)
SELECT 
	train_id,
	row_count,
	critical_count,
	critical_count * 1.0 / row_count AS critical_percentage
FROM cte
WHERE critical_percentage > 0.1
ORDER BY critical_percentage DESC
LIMIT 15;
"""

# 9
SIGNAL_SPEED_CORRELATION = """
WITH low_speed AS (
	SELECT
		train_id,
		AVG(signal_strength_dbm) AS signal_low
	FROM train_telemetry
	WHERE speed_kmh BETWEEN 0 and 100
	GROUP BY train_id
),
high_speed AS (
	SELECT
		train_id,
		AVG(signal_strength_dbm) AS signal_high
	FROM train_telemetry
	WHERE speed_kmh > 200
	GROUP BY train_id
)
SELECT
	l.train_id,
	ROUND(l.signal_low, 2) AS signal_low_speed,
	ROUND(h.signal_high, 2) AS signal_high_speed,
	ROUND(h.signal_high - l.signal_low, 2) AS delta_signal
FROM low_speed l
JOIN high_speed h
	ON l.train_id = h.train_id
WHERE l.signal_low IS NOT NULL AND h.signal_high IS NOT NULL
ORDER BY delta_signal DESC
LIMIT 15;
"""

# 10
MOST_CONSUMING_TRAINS = """
WITH per_train AS (
	SELECT
		train_id,
		AVG(power_consumption_kw) AS avg_eng,
		AVG(speed_kmh) AS avg_speed,
		AVG(power_consumption_kw) / AVG(speed_kmh) AS ratio
	FROM train_telemetry
	WHERE speed_kmh > 0
	GROUP BY train_id
),
global AS (
	SELECT
		AVG(power_consumption_kw) / AVG(speed_kmh) AS baseline
	FROM train_telemetry
	WHERE speed_kmh > 0
)
SELECT
	pt.train_id,
	pt.ratio - g.baseline AS delta_from_baseline,
	CASE WHEN pt.ratio - g.baseline <= 0 THEN 'Efficient'
		ELSE 'Inefficient'
		END AS efficiency_status
FROM per_train pt
CROSS JOIN global g
ORDER BY pt.avg_eng DESC
LIMIT 15;
"""

# 11
NETWORK_RELIABILITY = """
WITH signal AS (
	SELECT
		STRFTIME('%H', timestamp) AS hour,
		COUNT(*) AS total_cnt,
		SUM(
			CASE WHEN network_type = 'OFFLINE' OR signal_strength_dbm < -100
				THEN 1 ELSE 0
			END) AS bad_cnt
	FROM train_telemetry
	GROUP BY hour
)
SELECT
	hour,
	ROUND(bad_cnt * 1.0 / total_cnt, 4) AS bad_signal_percentage
FROM signal
ORDER BY bad_signal_percentage DESC;
"""
