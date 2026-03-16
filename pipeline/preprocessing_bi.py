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

    # Potenzialmente ridondante, ma per sicurezza si tiene
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    logger.info("Generazione chiavi dimensionali `DateKey` e `TimeKey`")
    df['DateKey'] = df['timestamp'].dt.strftime("%Y%m%d").astype('int32')
    df['TimeKey'] = (
        df['timestamp'].dt.hour * 3600
        + df['timestamp'].dt.minute * 60
        + df['timestamp'].dt.second
    ).astype('int32')

    logger.info("Calcolo tempo assoluto (Epoch) per logiche DAX.")
    epoch_start = pd.Timestamp(config['epoch_start'])
    df['EpochSeconds'] = (df['timestamp'] - epoch_start).dt.total_seconds().astype('int64')

    logger.info("Deduplicazione per `train_id` e `timestamp`")
    df.drop_duplicates(subset=['train_id', 'timestamp'], inplace=True)

    logger.info("Costruzione indice riga `RowIndexTrain`")
    df.sort_values(by=['train_id', 'timestamp'], ascending=[True, True], inplace=True)
    df['RowIndexTrain'] = df.groupby('train_id').cumcount() + 1

    logger.info("Costruzione indice riga `RowIndexTower`")
    df.sort_values(by=['cell_tower_id', 'timestamp'], ascending=[True, True], inplace=True)
    df['RowIndexTower'] = df.groupby('cell_tower_id').cumcount() + 1

    logger.info("Costruzione colonne ausiliarie di Data Quality e suddivisione velocità in categorie.")
    df['IsMissingTemp'] = np.where(
        df['temp_celsius'].isna(),
        1,
        0
    )
    MAX_CELSIUS = config['thresholds']['temp']['max']
    df['IsExtremeTemp'] = np.where(
        (df['IsMissingTemp'].eq(0)) & (df['temp_celsius'] > MAX_CELSIUS),
        1,
        0
    )
    df[['IsMissingTemp', 'IsExtremeTemp']] = df[['IsMissingTemp', 'IsExtremeTemp']].astype('int8')

    df['SpeedRange'] = pd.cut(
        df['speed_kmh'],
        bins=[-np.inf, 50, 100, 150, 200, 250, np.inf],
        labels=['01: 0-50', '02: 50-100', '03: 100-150',
                    '04: 150-200', '05: 200-250', '06: 250-300']
    ).astype('str')

    logger.info("Salvataggio dataset ordinato per `train_id` e `timestamp` (ottimizzato per Power BI)")
    df.sort_values(by=['DateKey', 'train_id', 'TimeKey'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_parquet(output_file, index=False)
    logger.info(f"Parquet per importazione in Power BI salvato in  {output_file}")

if __name__ == '__main__':
    start_time = datetime.now()

    logger.info("Preparazione dati per importazione in Power BI")
    run(UNPROCESSED_PARQUET, PROCESSED_PARQUET)
    logger.info(f"Tempo impiegato per il preprocessing dei dati: {(datetime.now() - start_time).total_seconds():.2f} secondi.")
