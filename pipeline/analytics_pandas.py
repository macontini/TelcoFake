from datetime import datetime
from pathlib import Path
from typing import Callable
import pandas as pd
import numpy as np
from functools import cached_property

from conf import config, logger
from conf import DATA_DIR, REPORTS_DIR

CHUNKSIZE = config['simulation']['chunksize']

class PandasAnalytics:
    def __init__(self, data_dir: Path, chunksize: int = 0) -> None:
        self.data_dir = data_dir
        self.input_file = next(data_dir.glob('*.parquet'), None)
        self.chunksize = chunksize or CHUNKSIZE
        self.report_catalog: dict[Callable, list] = {
            # --- REPORT 1: ANALISI VELOCITÀ PER FASCIA ORARIA ---
            self.hourly_speed_analysis: ['hour', 'speed_kmh'],
            # --- REPORT 2: TOP 5 TRENI CRITICI ---
            self.critical_trains_report: ['train_id', 'status'],
            # --- REPORT 3: SEGNALE VS VELOCITÀ ---
            self.signal_performance_analysis: ['speed_ranges', 'signal_strength_dbm'],
            # --- REPORT 4: WINDOW FUNCTIONS ---
            self.anomaly_window_detection: ['train_id', 'timestamp', 'temp_celsius'],
            # --- REPORT 5: ANALISI DEI BLACKOUT ---
            self.worst_signal_segment: ['line_segment', 'signal_strength_dbm', 'network_type'],
            # --- REPORT 6: EFFICIENZA RELATIVA ---
            self.top_power_speed_ratio: ['train_id', 'power_consumption_kw', 'speed_kmh'],
            # --- REPORT 7: NETWORK ---
            self.highest_signal_drop: ['cell_tower_id', 'timestamp', 'signal_strength_dbm'],
            # ----- REPORT 8: TRENI CON PIÙ CRITICITÀ -----
            self.recurrently_critical_trains: ['train_id', 'status'],
            # ----- REPORT 9: CORRELAZIONE SEGNALE VELOCITÀ -----
            self.signal_speed_correlation: ['train_id', 'signal_strength_dbm', 'speed_kmh'],
            # ----- REPORT 10: TRENI A MINOR EFFICIENZA -----
            self.most_consuming_trains: ['train_id', 'power_consumption_kw', 'speed_kmh'],
            # ----- REPORT 11: AFFIDABILITÀ SEGNALE PER ORARIO -----
            self.network_reliability: ['hour', 'network_type', 'signal_strength_dbm'],
        }

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        if self.input_file is None:
            logger.error(f"Nessun file parquet trovato in {self.data_dir}.")
            raise FileNotFoundError
        df = pd.read_parquet(self.input_file).astype(
            'float32', errors='ignore'
        )

        # ----- Colonne calcolate -----
        df['hour'] = df['timestamp'].dt.hour
        df['speed_ranges'] = pd.cut(
            df['speed_kmh'],
            bins=[-np.inf, 50, 100, 150, 200, 250, np.inf],
            labels=['01: 0-50', '02: 50-100', '03: 100-150',
                    '04: 150-200', '05: 200-250', '06: 250-300']
        )

        return df

    def generate_all_reports(self) -> None:
        """Esegue sequenzialmente tutti i report, esportando in CSV."""
        output_dir = REPORTS_DIR / 'by_pandas'
        output_dir.mkdir(exist_ok=True, parents=True)

        for i, (method, columns) in enumerate(self.report_catalog.items()):
            report_name = method.__name__            
            logger.info(f"Esecuzione report (Pandas): {report_name}")
            output_path = output_dir / f"{i+1}_{report_name}.csv"
            try:
                # Selezione colonne
                df = self.dataframe[columns].copy()
                res = method(df)
                # Salvataggio
                res.to_csv(output_path, index=False)
                logger.info(f"Report (Pandas) '{report_name}' riprodotto con successo.")
            except Exception as e:
                logger.error(f"Errore durante il report (Pandas) '{report_name}': {e}")

    def hourly_speed_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        SELECT
            STRFTIME('%H', timestamp) AS hour,
            AVG(speed_kmh)
        FROM train_telemetry
        GROUP BY hour;
        """
        return df\
            .groupby(by='hour')['speed_kmh']\
            .mean()\
            .reset_index()

    def critical_trains_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        SELECT
            train_id,
            COUNT(status) AS critical_count
        FROM train_telemetry
        WHERE status = 'CRITICAL'
        GROUP BY train_id
        ORDER BY critical_count DESC
        LIMIT 10;
        """
        return df\
            .loc[df['status'].eq('CRITICAL'), ['train_id']]\
            .value_counts()\
            .reset_index(name='critical_count')\
            .head(10)

    def signal_performance_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        # observed=False mostra etichette anche se prive di valori
        return df\
            .groupby(by='speed_ranges', observed=False)['signal_strength_dbm']\
            .mean()\
            .to_frame()\
            .rename(columns={'signal_strength_dbm': 'avg_signal_str'})\
            .reset_index()

    def anomaly_window_detection(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        df = df.sort_values(by=['train_id', 'timestamp'])
        df['previous_timestamp'] = df\
            .groupby('train_id')['timestamp']\
            .shift(1)
        df['previous_temp'] = df\
            .groupby('train_id')['temp_celsius']\
            .shift(1)
        df['delta_timestamp'] = (df['timestamp'] - df['previous_timestamp']).abs()
        df['delta_temp'] = (df['temp_celsius'] - df['previous_temp']).abs()

        return df.loc[
                df['previous_temp'].notna() & df['delta_temp'].ge(30),
                ['train_id', 'delta_timestamp', 'delta_temp']
            ].sort_values(by='delta_temp', ascending=False)\
            .head(30)

    def worst_signal_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        is_blackout = df['signal_strength_dbm'].lt(-100) | df['network_type'].eq('OFFLINE')

        return df\
            .assign(blackout_ratio=is_blackout)\
            .groupby(by='line_segment')['blackout_ratio']\
            .mean()\
            .round(4)\
            .reset_index()

    def top_power_speed_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        return df[df['speed_kmh'].gt(10)]\
            .groupby(by='train_id')[['power_consumption_kw', 'speed_kmh']]\
            .sum()\
            .assign(ratio=lambda row: row['power_consumption_kw'] / row['speed_kmh'])\
            .reset_index()\
            .rename(columns={'power_consumption_kw': 'power_sum', 'speed_kmh': 'speed_sum'})\
            .round({'ratio': 2})\
            .sort_values(by='ratio', ascending=False)\
            .head(15)

    def highest_signal_drop(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
            delta_minutes < 5       -- Sbalzi entro 5 minuti
            AND signal_drop > 50	-- Soglia critica (max: 70 by def)
        ORDER BY signal_drop DESC
        LIMIT 15;
        """
        df = df.sort_values(by=['cell_tower_id', 'timestamp'])
        df['previous_timestamp'] = df\
            .groupby('cell_tower_id')['timestamp']\
            .shift(1)
        df['previous_signal'] = df\
            .groupby('cell_tower_id')['signal_strength_dbm']\
            .shift(1)
        df['delta_minutes'] = (df['timestamp'] - df['previous_timestamp']).dt.total_seconds() / 60
        df['signal_drop'] = (df['signal_strength_dbm'] - df['previous_signal']).abs()

        return df.loc[
            df['delta_minutes'].lt(5) & df['signal_drop'].gt(50),
            ['cell_tower_id', 'delta_minutes', 'signal_drop']
        ].sort_values(by='signal_drop', ascending=False)\
        .head(15)

    def recurrently_critical_trains(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        return df\
            .assign(critical_count=df['status'].eq('CRITICAL'))\
            .groupby(by='train_id')['critical_count']\
            .aggregate(['count', 'sum'])\
            .rename(columns={'count': 'row_count', 'sum': 'critical_count'})\
            .assign(critical_percentage=lambda row: row['critical_count'] / row['row_count'])\
            .query('`critical_percentage` > 0.1')\
            .sort_values(by='critical_percentage', ascending=False)\
            .head(15)\
            .reset_index()

    def signal_speed_correlation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        low_speed = df[df['speed_kmh'].between(0, 100)]\
            .pivot_table(values='signal_strength_dbm', index='train_id', aggfunc='mean')\
            .round({'signal_strength_dbm': 2})\
            .rename(columns={'signal_strength_dbm': 'signal_low_speed'})
        high_speed = df[df['speed_kmh'].gt(200)]\
            .pivot_table(values='signal_strength_dbm', index='train_id', aggfunc='mean')\
            .round({'signal_strength_dbm': 2})\
            .rename(columns={'signal_strength_dbm': 'signal_high_speed'})

        return low_speed\
            .join(high_speed)\
            .dropna(axis=0, how='any')\
            .assign(delta_signal=lambda row: row['signal_high_speed'] - row['signal_low_speed'])\
            .sort_values(by='delta_signal', ascending=False)\
            .head(15)

    def most_consuming_trains(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        df = df[df['speed_kmh'].gt(0)]
        _global = df[['power_consumption_kw', 'speed_kmh']].mean()

        baseline = _global['power_consumption_kw'] / _global['speed_kmh']
        return df\
            .groupby(by='train_id')\
            .mean()\
            .rename(columns={'power_consumption_kw': 'avg_eng', 'speed_kmh': 'avg_speed'})\
            .assign(
                ratio=lambda row: row['avg_eng'] / row['avg_speed'],
                delta_from_baseline=lambda row: row['ratio'] - baseline,
                efficiency_status=lambda row: np.where(row['delta_from_baseline'].gt(0), 'Inefficient', 'Efficient')
            )\
            .sort_values(by='avg_eng', ascending=False)\
            .head(15)\
            .reset_index()\
            [['train_id', 'delta_from_baseline', 'efficiency_status']]

    def network_reliability(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        is_blackout =  df['signal_strength_dbm'].lt(-100) | df['network_type'].eq('OFFLINE')

        return df\
            .assign(bad_signal_percentage=is_blackout)\
            .groupby('hour')['bad_signal_percentage']\
            .mean()\
            .round(4)\
            .reset_index(name='bad_signal_percentage')\
            .sort_values(by='bad_signal_percentage', ascending=False)

if __name__ == "__main__":

    start_time = datetime.now()

    analytics = PandasAnalytics(DATA_DIR, CHUNKSIZE)

    logger.info("Generazione report CSV (via Pandas) in corso...")
    analytics.generate_all_reports()
    logger.info(f"Tempo impiegato per la creazione dei report (Pandas): {(datetime.now() - start_time).total_seconds():.2f} secondi.")
