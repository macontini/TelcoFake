import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from conf import config, logger
from conf import DATA_DIR

UNPROCESSED_PARQUET = next(DATA_DIR.glob('*.parquet'))
PROCESSED_PARQUET = DATA_DIR / 'data_for_powerbi.parquet'

def run(input_file: Path, output_file: Path) -> None:

    logger.info(f"Lettura file {input_file}")
    df = pd.read_parquet(input_file)

    # --------------------
    # ROUNDING
    # --------------------
    df = df.round({
        'temp_celsius': 2,
        'speed_kmh': 2,
        'latitude': 3,
        'longitude': 3,
        'signal_strength_dbm': 2,
        'power_consumption_kw': 2,
        'speed_modifier': 2,
        'hour_noise': 2,
        'signal_malus': 2,
    })

    # Potenzialmente ridondante, ma per sicurezza si tiene
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --------------------
    # CHIAVI DIMENSIONALI
    # granularità al minuti
    # --------------------
    logger.info("Generazione chiavi dimensionali `DateKey` e `TimeKey`")
    df['DateKey'] = df['timestamp'].dt.strftime("%Y%m%d").astype('int32')
    df['TimeKey'] = (
        df['timestamp'].dt.hour * 60
        + df['timestamp'].dt.minute
    ).astype('int32')

    # --------------------
    # DEDUPLICAZIONE
    # --------------------
    logger.info("Deduplicazione per `train_id` e `timestamp`")
    df.sort_values(by=['train_id', 'timestamp'], ascending=[True, True], inplace=True)
    df.drop_duplicates(subset=['train_id', 'timestamp'], inplace=True)

    # --------------------
    # DELTA (Sostituisce misure in DAX, pesanti)
    # --------------------
    logger.info("Calcolo Delta Temperatura e Delta Timestamp (minuti) per ciascun treno.")
    df['delta_temp_celsius'] = df\
        .groupby(by='train_id')['temp_celsius']\
        .diff()\
        .round(2)
    df['delta_time_min'] = (df\
        .groupby(by='train_id')['timestamp']\
        .diff()\
        .dt.total_seconds() / 60.0
    ).round(2)

    logger.info("Calcolo Delta Segnale (assoluto) per ciascuna torre.")
    df.sort_values(by=['cell_tower_id', 'timestamp'], inplace=True)
    df['delta_signal_dbm'] = df\
        .groupby(by='cell_tower_id')['signal_strength_dbm']\
        .diff()\
        .abs()\
        .round(2)

    # --------------------
    # QUALITY
    # --------------------
    logger.info("Costruzione colonne ausiliarie di Data Quality e suddivisione velocità in categorie.")
    df['IsMissingTemp'] = np.where(df['temp_celsius'].isna(), 1, 0).astype('int8')

    MAX_CELSIUS = config['thresholds']['temp']['max']
    df['IsExtremeTemp'] = np.where((df['IsMissingTemp'].eq(0)) & (df['temp_celsius'] > MAX_CELSIUS), 1, 0).astype('int8')

    df['SpeedRange'] = pd.cut(
        df['speed_kmh'],
        bins=[-np.inf, 50, 100, 150, 200, 250, np.inf],
        labels=['01: 0-50', '02: 50-100', '03: 100-150',
                    '04: 150-200', '05: 200-250', '06: 250-300']
    ).astype('str')

    # --------------------
    # EPURAZIONE COLONNE PESANTI
    # utili solo per costruzione dati random
    # `timestamp` ha cardinalità assurda e deve scomparire
    # --------------------
    cols_to_drop = [
        "timestamp", "latitude", "longitude",
        "speed_modifier", "hour_noise", "signal_malus"
    ]
    logger.info(f"Rimozione colonne non necessarie: {', '.join(cols_to_drop)}")
    df.drop(columns=cols_to_drop, errors='ignore', inplace=True)

    # --------------------
    # SAVING
    # --------------------
    logger.info("Salvataggio dataset ordinato per `train_id` e `timestamp` (ottimizzato per compressione Power BI)")
    df.sort_values(by=['DateKey', 'train_id', 'TimeKey'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_parquet(output_file, index=False)
    logger.info(f"Parquet per importazione in Power BI salvato in {output_file}")

if __name__ == '__main__':
    start_time = datetime.now()

    logger.info("Preparazione dati per importazione in Power BI")
    run(UNPROCESSED_PARQUET, PROCESSED_PARQUET)
    logger.info(f"Tempo impiegato per il preprocessing dei dati: {(datetime.now() - start_time).total_seconds():.2f} secondi.")
