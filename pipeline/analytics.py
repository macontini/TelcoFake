from datetime import datetime
from pathlib import Path
import sqlite3
import pandas as pd

import queries
from conf import logger
from conf import REPORTS_DIR, DB_PATH, CWD

REPORTS_DIR.mkdir(exist_ok=True)

class RailwayAnalytics:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.report_catalog: dict[str, str] = {
            # --- REPORT 1: ANALISI VELOCITÀ PER FASCIA ORARIA ---
            "1 Hourly Speed Profile": queries.HOURLY_SPEED_ANALYSIS,
            # --- REPORT 2: TOP 5 TRENI CRITICI ---
            "2 Top Critical Trains": queries.CRITICAL_TRAINS_REPORT,
            # --- REPORT 3: SEGNALE VS VELOCITÀ ---
            "3 Signal By Speed Range": queries.SIGNAL_PERFORMANCE_ANALYSIS,
            # --- REPORT 4: WINDOW FUNCTIONS ---
            "4 Temperature Anomalies": queries.ANOMALY_WINDOW_DETECTION,
            # --- REPORT 5: ANALISI DEI BLACKOUT ---
            "5 Segment Blackout Ratio": queries.WORST_SIGNAL_SEGMENT,
            # --- REPORT 6: EFFICIENZA RELATIVA ---
            "6 Power Efficiency Ranking": queries.TOP_POWER_SPEED_RATIO,
            # --- REPORT 7: NETWORK ---
            "7 Critical Signal Drops": queries.HIGHEST_SIGNAL_DROP,
            # ----- REPORT 8: TRENI CON PIÙ CRITICITÀ -----
            "8 Chronic Failure Trains": queries.RECURRENTLY_CRITICAL_TRAINS,
            # ----- REPORT 9: CORRELAZIONE SEGNALE VELOCITÀ -----
            "9 Speed Signal Degradation": queries.SIGNAL_SPEED_CORRELATION,
            # ----- REPORT 10: TRENI A MINOR EFFICIENZA -----
            "10 Power Consumption Outliers": queries.MOST_CONSUMING_TRAINS,
            # ----- REPORT 11: AFFIDABILITÀ SEGNALE PER ORARIO -----
            "11 Network Reliability By Hour": queries.NETWORK_RELIABILITY
        }

    def generate_all_reports(self) -> None:
        """Esegue sequenzialmente tutti i report."""
        try:
            with self.conn:
                for report_name, query in self.report_catalog.items():
                    self._execute_single_report(query, report_name)
        except Exception as e:
            logger.error(f"Errore critico di connessione al DB: {e}")

    def _execute_single_report(self, query: str, report_name: str) -> None:
        """Metodo helper per eseguire una query e salvare il risultato in CSV."""
        try:
            logger.info(f"Esecuzione report (SQL): {report_name}")
            df_result = pd.read_sql_query(query, self.conn)

            if df_result.empty:
                logger.warning(f"Il report '{report_name}' non ha prodotto risultati.")
                return

            output_path = REPORTS_DIR / f"{report_name.replace(' ', '_').lower()}.csv"
            df_result.to_csv(output_path, index=False)

            logger.info(f"Report (SQL) salvato in: {output_path.relative_to(CWD)}")

        except Exception as e:
            logger.error(f"Errore durante il report (SQL) '{report_name}': {e}")

if __name__ == "__main__":

    start_time = datetime.now()

    analytics = RailwayAnalytics(DB_PATH)

    logger.info("Generazione report CSV (via SQLite) in corso...")
    analytics.generate_all_reports()
    logger.info(f"Tempo impiegato per la creazione dei report (SQLite): {(datetime.now() - start_time).total_seconds():.2f} secondi.")
