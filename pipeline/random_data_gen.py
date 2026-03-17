import sys
from datetime import datetime
from faker import Faker
import pandas as pd
import numpy as np

from conf import config, logger
from conf import DATA_DIR, LOG_FILE_PATH

# Se True, produce il 10% delle righe per ridurre tempi di elaborazione e impatto su disco
QUICK_TEST = '--test' in sys.argv

logger.info(f"File di log: {LOG_FILE_PATH.resolve()}")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def run(now: str) -> None:

    # ----- CONFIGURAZIONE -----
    try:
        SAMPLES_NUM = config['simulation']['samples_num']
        if QUICK_TEST:
            SAMPLES_NUM  //= 10 

        OUTPUT_CSV, OUTPUT_PARQUET = (
            DATA_DIR / f'{now}_random_data.csv',
            DATA_DIR / f'{now}_random_data.parquet'
        )

        TRAIN_ID = config['fake_id_schema']['train']
        TOWER_ID = config['fake_id_schema']['tower']

        START_DATE = config['simulation']['start_date']

        AVG_CELSIUS, DEVSTD_CELSIUS, MIN_CELSIUS, MAX_CELSIUS = (
            config['thresholds']['temp']['avg'],
            config['thresholds']['temp']['devstd'],
            config['thresholds']['temp']['min'],
            config['thresholds']['temp']['max']
        )
        MIN_TRN_SPEED_KMH, MAX_TRN_SPEED_KMH = (
            config['thresholds']['speed']['min_kmh'],
            config['thresholds']['speed']['max_kmh']
        )
        MIN_SIGN_STREN, MAX_SIGN_STREN = (
            config['thresholds']['signal']['min_dbm'],
            config['thresholds']['signal']['max_dbm']
        )

        MILAN_MIN_LAT, MILAN_MAX_LAT = (
            config['geocoordinates']['milan_min_lat'],
            config['geocoordinates']['milan_max_lat']
        )
        MILAN_MIN_LON, MILAN_MAX_LON = (
            config['geocoordinates']['milan_min_lon'],
            config['geocoordinates']['milan_max_lon']
        )

        # Divisione del percorso in tratte
        SEGMENTS = config['segments']
        assert len(SEGMENTS) > 1, f"Il numero di tratte (segments) dev'essere maggiore di 1."

        statuses = list(config['probabilities']['status'].keys())
        networks = list(config['probabilities']['network'].keys())

        STATUS_PRBLTY = list(config['probabilities']['status'].values())
        BROKEN_STATUS_PRBLTY = list(config['probabilities']['broken_status'].values())
        NETWORK_PRBLTY = list(config['probabilities']['network'].values())

        assert sum(STATUS_PRBLTY) == 1, f"Rivedere config: {config['probabilities']['status']}"
        assert sum(NETWORK_PRBLTY) == 1, f"Rivedere config: {config['probabilities']['network']}"

        # Treni che si guasteranno
        UNFORTUNATE_TRAINS = config['unfortunate_trains']
        assert UNFORTUNATE_TRAINS < SAMPLES_NUM, f"I treni sfortunati devono essere meno dei treni totali ({UNFORTUNATE_TRAINS:,} vs {SAMPLES_NUM:,})"

        fake = Faker(config['simulation']['locale'])

        DIRTY_FRACTION_MISSING = config['data_quality']['dirty_fraction_missing']       # % Valori mancanti
        DIRTY_FRACTION_ANOMALIES = config['data_quality']['dirty_fraction_anomalies']   # % Anomalie di sensore
        DIRTY_FRACTION_DUPLICATES = config['data_quality']['dirty_fraction_duplicates'] # % Righe duplicate

        STOPS_NUM_FRACTION = config['data_quality']['stops_num_fraction'] # % Fermate (speed=0)

    except (KeyError, AssertionError) as e:
        logger.error(f"{e.__class__.__name__}: {e}", exc_info=True)
        sys.exit(1)

    # ----- GENERAZIONE -----
    logger.info(f"Generazione dati in corso (Obiettivo: {SAMPLES_NUM:,} righe).")

    # .bothify trasforma '?' in lettere casuali da 'ABCDE', '#' in numeri casuali (0-9), '%' in numeri casuali non nulli (1-9)
    train_pool = list(set(fake.bothify(text=TRAIN_ID, letters='XYZ').upper() for _ in range(1000)))
    tower_pool = list(set(fake.bothify(text=TOWER_ID).upper() for _ in range(1000)))

    data = {
        'train_id': np.random.choice(train_pool, SAMPLES_NUM),
        'cell_tower_id': np.random.choice(tower_pool, SAMPLES_NUM),

        # Crea una sequenza temporale fissa. freq='min' aggiungendo un record ogni minuto
        'timestamp': pd.date_range(start=START_DATE, periods=SAMPLES_NUM, freq='min'),

        # np.random.normal genera una distribuzione Gaussiana
        # Media, dev std, numero di campioni
        'temp_celsius': np.random.normal(AVG_CELSIUS, DEVSTD_CELSIUS, SAMPLES_NUM),

        # Estrae valori casuali basandosi sulle probabilità 'p'.
        'status': np.random.choice(
            a=statuses,
            p=STATUS_PRBLTY,
            size=SAMPLES_NUM
        ),

        # Alta velocità con variazioni casuali
        'speed_kmh': np.random.uniform(MIN_TRN_SPEED_KMH, MAX_TRN_SPEED_KMH, SAMPLES_NUM),

        # Coordinate vicine a Milano
        'latitude': np.random.uniform(MILAN_MIN_LAT, MILAN_MAX_LAT, SAMPLES_NUM),
        'longitude': np.random.uniform(MILAN_MIN_LON, MILAN_MAX_LON, SAMPLES_NUM),

        # Magnitudo del segnale
        'signal_strength_dbm': np.random.randint(MIN_SIGN_STREN, MAX_SIGN_STREN, SAMPLES_NUM),

        # Dominio Ferroviario
        'line_segment': np.random.choice(SEGMENTS, SAMPLES_NUM),

        # Protocolli di rete
        'network_type': np.random.choice(
            a=networks,
            p=NETWORK_PRBLTY,
            size=SAMPLES_NUM
        )
    }

    # --------------------
    # pandas.DataFrame
    # --------------------

    df = pd.DataFrame(data)
    logger.info(f"Dataframe creato: {len(df):,} elementi.")

    #  Treni sfortunati
    bad_trains = df['train_id'].unique()[:UNFORTUNATE_TRAINS]
    logger.info(f"Treni sfortunati selezionati per maggior probabilità di `status` WARNING/CRITICAL: {UNFORTUNATE_TRAINS}.")
    print(*bad_trains, sep='\n')

    # Per questi treni, aumenta drasticamente la probabilità di WARNING e CRITICAL
    mask = df['train_id'].isin(bad_trains)
    df.loc[mask, 'status'] = np.random.choice(
        a=statuses,
        p=BROKEN_STATUS_PRBLTY,
        size=mask.sum()
    )

    # --------------------
    # SANITIZZAZIONE
    # --------------------
    # Seppur precoce e potenzialmente innecessaria, a scopo illustrativo

    # TEMPERATURA
    mask = ~df['temp_celsius'].between(MIN_CELSIUS, MAX_CELSIUS, inclusive='both')
    df = df[~mask]
    logger.info(f"Rimosse {mask.sum():,} righe in cui `temp_celsius` è fuori dall'intervallo [{MIN_CELSIUS}, {MAX_CELSIUS}]")

    # RIGHE DUPLICATE
    mask = df.duplicated(subset=['timestamp', 'train_id'], keep='first')
    df = df[~mask].copy()
    logger.info(f"Rimosse {mask.sum():,} righe duplicate per 'timestamp' e 'train_id'.")

    # float32 invece di float64 per risparmiare memoria 
    df['temp_celsius'] = df['temp_celsius'].astype('float32')
    logger.info("Colonna `temp_celsius` convertita in float32.")

    # --------------------
    # RUMORE
    # --------------------

    # POTENZA
    # Scala di grandezza (nessun significato fisico di `forza` o `resistenza`)
    power_factor = 5
    # con aggiunta di rumore il cui valore centrale è al centro del range di velocità
    POWER_NOISE_MEAN = (MAX_TRN_SPEED_KMH - MIN_TRN_SPEED_KMH) / 2
    # la cui deviazione standard è 1/3 dell'ampiezza del range di velocità
    POWER_NOISE_DEVSTD = (MAX_TRN_SPEED_KMH - MIN_TRN_SPEED_KMH) / 3
    df['power_consumption_kw'] = power_factor * (
        df['speed_kmh'] + np.random.normal(
            POWER_NOISE_MEAN,
            POWER_NOISE_DEVSTD,
            len(df)
        )
    )
    logger.info("Costruita colonna `power_consumption_kw`, "
                "proporzionale a `speed_kmh` a meno di un disturbo normale "
                f"con media {POWER_NOISE_MEAN} e deviazione {POWER_NOISE_DEVSTD}.")

    # VELOCITÀ
    SPEED_LOWER_GAIN, SPEED_UPPER_GAIN = (
        config['speed_noise']['on_train']['lower_gain'],
        config['speed_noise']['on_train']['upper_gain']
    )
    assert SPEED_LOWER_GAIN < SPEED_UPPER_GAIN, f"Rivedere config: guagagno di velocità tra {SPEED_LOWER_GAIN} e {SPEED_UPPER_GAIN}."
    train_factors = {
        tid: np.random.uniform(SPEED_LOWER_GAIN, SPEED_UPPER_GAIN) for tid in df['train_id'].unique()
    }
    # Ad ogni treno il suo moltiplicatore di velocità, estratto randomo da una distribuzione uniforme
    df['speed_modifier'] = df['train_id'].map(train_factors)
    df['speed_kmh'] = df['speed_kmh'] * df['speed_modifier']
    logger.info("Alterata colonna `speed_kmh`, "
                "associando ad ogni treno un moltiplicatore di velocità "
                f"compreso tra {SPEED_LOWER_GAIN} e {SPEED_UPPER_GAIN}.")

    # Ad ogni fascia oraria associamo ulteriore moltiplicatore sulla velocità
    SPEED_LOWER_GAIN, SPEED_UPPER_GAIN = (
        config['speed_noise']['on_hour']['lower_gain'],
        config['speed_noise']['on_hour']['upper_gain']
    )
    hour_noise = {h: np.random.uniform(SPEED_LOWER_GAIN, SPEED_UPPER_GAIN) for h in range(24)}
    df['hour_noise'] = df['timestamp'].dt.hour.map(hour_noise)
    df['speed_kmh'] = df['speed_kmh'] * df['hour_noise']
    logger.info("Alterata colonna `speed_kmh`, "
                "associando ad ogni fascia oraria un moltiplicatore di velocità "
                f"compreso tra {SPEED_LOWER_GAIN} e {SPEED_UPPER_GAIN}.")

    df['speed_kmh'] = df['speed_kmh'].clip(MIN_TRN_SPEED_KMH, MAX_TRN_SPEED_KMH)
    logger.info(f"Dominio delle velocità clippato tra {MIN_TRN_SPEED_KMH} e {MAX_TRN_SPEED_KMH}")

    # Fermate
    df.loc[df.sample(frac=STOPS_NUM_FRACTION).index, 'speed_kmh'] = 0
    logger.info(f"Fermate introdotte: frazione {STOPS_NUM_FRACTION} del totale ({STOPS_NUM_FRACTION*len(df)} velocità nulle)")

    # SEGNALE
    # Associamo un malus per ogni tratta: successione di negativi (es. 0, -10, -20, -30, -40)
    MAX_MALUS = -40
    # il malus diminuisce gradualmente per ciascuna tratta secondo la funzione sottostante
    step = MAX_MALUS / (len(SEGMENTS) - 1)
    segment_malus = {SEGMENTS[i]: round(i * step, 2) for i in range(len(SEGMENTS))}
    df['signal_malus'] = df['line_segment'].map(segment_malus)
    # da sommare ad ogni segnale della corrispondente tratta
    df['signal_strength_dbm'] = df['signal_strength_dbm'] + df['signal_malus']

    # Ulteriore rumore sul segnale, come numero intero casuale fra due estremi configurati
    SIGNAL_NOISE_MAX = config['signal_noise']['max']
    SIGNAL_NOISE_MIN = config['signal_noise']['min']
    df['signal_strength_dbm'] = df['signal_strength_dbm'] + np.random.randint(
        SIGNAL_NOISE_MIN,
        SIGNAL_NOISE_MAX,
        size=len(df)
    )
    df['signal_strength_dbm'] = df['signal_strength_dbm'].clip(MIN_SIGN_STREN, MAX_SIGN_STREN)

    # Si sottrae un valore proporzionale alla velocità
    network_speed_correlation = 10
    df['signal_strength_dbm'] = df['signal_strength_dbm'] - (df['speed_kmh'] / network_speed_correlation)
    logger.info("Alterata colonna `signal_strength_dbm`, "
                "sottraendo all'ampiezza di ogni segnale "
                f"1/{network_speed_correlation} della velocità istantanea.")

    df['signal_strength_dbm'] = df['signal_strength_dbm'].clip(MIN_SIGN_STREN, MAX_SIGN_STREN)
    logger.info(f"Dominio dei segnali clippato tra {MIN_SIGN_STREN} e {MAX_SIGN_STREN}")

    logger.info("Trasformazioni sui dati effettuate!")

    # --------------------
    # RIGHE ROTTE
    # --------------------

    # Missing values (NaN) per simulare un termometro che si rompe
    df.loc[df.sample(frac=DIRTY_FRACTION_MISSING).index, 'temp_celsius'] = np.nan
    logger.info(f"Missing values (`temp_celsius`) inseriti. {DIRTY_FRACTION_MISSING*100}% del totale è NaN.")

    # Anomalie di temperatura (valori impossibili)
    df.loc[df.sample(frac=DIRTY_FRACTION_ANOMALIES).index, 'temp_celsius'] = 999.0
    logger.info(f"Valori anomali (`temp_celsius`) inseriti. {DIRTY_FRACTION_ANOMALIES*100}% del totale è 999.0.")

    # Errore di invio/ricezione dati che genera righe doppie
    df = pd.concat([df, df.sample(frac=DIRTY_FRACTION_DUPLICATES)], ignore_index=True)
    logger.info(f"Righe duplicate inserite: {DIRTY_FRACTION_DUPLICATES*100}% del totale aggiunto in coda al dataframe.")

    # Eliminare la sezione RIGHE ROTTE o spostarla prima di SANITIZZAZIONE
    # Altrimenti i successivi steps della pipeline rischiano di rompersi o produrre report sporchi
    # --------------------

    df = df.sort_values(by=['train_id', 'timestamp']).reset_index(drop=True)
    logger.info("Dati ordinati per `train_id` e `timestamp`.")

    # --------------------
    # SALVATAGGIO
    # --------------------

    # CSV
    df.to_csv(OUTPUT_CSV, sep=',', encoding='utf-8', index=False)
    logger.info(f"Salvataggio CSV: ({len(df)} righe) {OUTPUT_CSV.resolve()} ({OUTPUT_CSV.stat().st_size // 1024 ** 2}MB).")

    # Parquet
    df.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info(f"Salvataggio Parquet: ({len(df):,} righe) {OUTPUT_PARQUET.resolve()} ({OUTPUT_PARQUET.stat().st_size // 1024 ** 2}MB).")

    logger.info(f"Spazio su disco risparmiato (Parquet vs CSV): {(OUTPUT_PARQUET.stat().st_size / OUTPUT_CSV.stat().st_size - 1 )* 100:.2f}%.")


if __name__ == '__main__':

    start_time = datetime.now()
    run(start_time.strftime("%Y_%m_%d_%H_%M_%S"))
    logger.info(f"Tempo impiegato per la creazione dei dati: {(datetime.now() - start_time).total_seconds():.2f} secondi.")
