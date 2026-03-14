from datetime import datetime
from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np

import queries
from conf import config, logger
from conf import DATA_DIR, DB_PATH

CHUNKSIZE = config['simulation']['chunksize']
indexes_on = [('cell_tower_id', 'timestamp'), ('line_segment', ), ('status', ), ('timestamp', )]

class Database:
    def __init__(self, db_path: Path, data_dir: Path, chunksize: int = 0) -> None:
        self.conn = sqlite3.connect(db_path)
        self.data_dir = data_dir
        self.chunksize = chunksize or CHUNKSIZE

        self.latest_file = self._get_latest_file('.parquet') or self._get_latest_file('.csv')

    def __len__(self) -> int:
        try:
            with self.conn:
                cursor = self.conn.cursor()
                res = cursor.execute(
                    "SELECT COUNT(*) FROM %s" % queries.tbl_name
                ).fetchone()
                return res[0]
        except Exception as e:
            logger.error(f"{e.__class__.__name__}: {e}", exc_info=True)
            return 0

    def init_table(self) -> None:
        try:
            with self.conn:
                cursor = self.conn.cursor()

                logger.info(queries.CREATE_TBL_TELEMETRY.strip())
                cursor.execute(queries.CREATE_TBL_TELEMETRY)
                logger.info("Success!")
        except Exception as e:
            logger.error(f"{e.__class__.__name__}: {e}", exc_info=True)

    def init_indexes(self, tbl_name: str, indexes_on: list[tuple[str, ...]]) -> None:
        with self.conn:
            for idxs in indexes_on:
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(
                        f"CREATE INDEX idx_{'_'.join(idxs)} ON {tbl_name}({', '.join(idxs)})"
                    )
                except Exception as e:
                    logger.error(f"{e.__class__.__name__}: {e}", exc_info=True)

    def _get_latest_file(self, suffix: str) -> Path | None:
        """Trova il file più recente con una specifica estensione."""
        try:
            files = self.data_dir.glob(f"*{suffix}")
            latest_file = max(
                files,
                key=lambda f: f.stat().st_mtime
            )
            logger.info(f"Caricamento dati da: {latest_file.name}")
            return latest_file
        except ValueError as e:
            logger.error(f"{e.__class__.__name__} - Errore: Nessun file caricato dalla cartella {self.data_dir}", exc_info=True)
            return None

    def dataframe_to_sql(self) -> None:
        """Inserisce in database i i dati del parquet/csv più recente"""
        if self.latest_file is None:
            return

        logger.info(f"Inizio elaborazione massiva per {self.latest_file.resolve()}")

        if self.latest_file.suffix == '.csv':
            chunks = pd.read_csv(self.latest_file, encoding='utf-8', chunksize=self.chunksize)
            for chunk in chunks:
                self._push(chunk) # type: ignore

        elif self.latest_file.suffix == '.parquet':
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(self.latest_file)
            for batch in pf.iter_batches(self.chunksize):
                self._push(batch.to_pandas())

    def _push(self, chunk: pd.DataFrame) -> None:

        # datetime64[ns] -> str
        chunk['timestamp'] = chunk['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
        # float -> None
        chunk = chunk.replace({np.nan: None})

        values = chunk[queries.columns].to_numpy().tolist()

        with self.conn:
            try:
                cursor = self.conn.cursor()

                cursor.executemany(queries.INSERT_TELEMETRY, values)
                logger.info(f"Inserito blocco da {len(chunk):,} righe.")

            except Exception as e:
                logger.error(f"{e.__class__.__name__} - Errore durante l'inserimento del chunk: {e}", exc_info=True)

    def optimize_sqlite(self) -> None:
        with self.conn:
            cursor = self.conn.cursor()
            # Non aspetta che il disco confermi la scrittura (veloce, ma rischioso se salta la corrente)
            cursor.execute("PRAGMA synchronous = OFF;")
            # Usa la memoria invece del disco per i log temporanei
            cursor.execute("PRAGMA journal_mode = MEMORY;")
            # Aumenta la dimensione della cache
            cursor.execute("PRAGMA cache_size = 1000000;")

def run() -> None:

    db = Database(DB_PATH, DATA_DIR, CHUNKSIZE)
    if db.latest_file is None:
        return

    db.init_table()
    db.optimize_sqlite()
    db.dataframe_to_sql()
    # Costruzione indici DOPO inserimento dati
    db.init_indexes(queries.tbl_name, indexes_on)

if __name__ == '__main__':

    start_time = datetime.now()
    run()
    logger.info(f"Tempo impiegato per la creazione del database: {(datetime.now() - start_time).total_seconds():.2f} secondi.")
