# Railway IoT Telemetry Analytics

Questo progetto implementa una pipeline di data engineering e data analysis per l'elaborazione di dati telemetrici ferroviari.  
Simula l'ingestione, l'analisi e la visualizzazione dello stato di una flotta di treni ad alta velocità, valutando performance di rete, efficienza energetica e anomalie hardware.

- [Railway IoT Telemetry Analytics](#railway-iot-telemetry-analytics)
  - [Tecnologie utilizzate](#tecnologie-utilizzate)
  - [Architettura e Pipeline](#architettura-e-pipeline)
  - [Dataset](#dataset)
  - [Punti di forza e limiti](#punti-di-forza-e-limiti)
  - [Conclusioni e insights](#conclusioni-e-insights)

## Tecnologie utilizzate

- **Linguaggio:** Python 3.14
- **Data Processing:** Pandas, Numpy, SQLite3
- **Data Visualization:** Matplotlib, Seaborn
- **Data Generation:** Faker, Random
- **Formati di archiviazione:** Parquet, CSV, SQLite DB

## Architettura e Pipeline

Il progetto è strutturato come una pipeline batch composta da tre fasi principali:

1. **Generazione (`random_data_gen.py`):** creazione del dataset sintetico e salvataggio in formato Parquet/CSV e database SQLite.
2. **Analisi:** elaborazione di 11 report analitici implementati in due versioni:
   - `analytics.py`: basato su query SQL (CTE, Window Functions) eseguite tramite SQLite.
   - `analytics_pandas.py`: basato su calcolo vettoriale in memoria tramite Pandas.
3. **Visualizzazione (`visualization.py`):** generazione automatica di grafici tramite un pattern a catalogo (`@auto_plot`), che mappa ogni report esportato alla relativa visualizzazione ottimale.
4. **Preparation (`preprocessing_bi.py`):** script di trasformazione (*Transform*) finale che ottimizza il dataset per il motore colonnare di Power BI, per ottenere un *parquet* pronto per l'importazione (*Load*) in Power BI.

## Dataset

Il dataset ammette *fine-tuning* tramite apposito file di configurazione `config.yaml`.  
Consiste di default in circa **2.5 milioni di record** generati randomicamente, progettati per simulare serie temporali di sensori IoT a bordo treno.

- **Feature principali:** ID treno (es. `ETR-X-123-45`), timestamp, tratta ferroviaria, velocità (km/h), temperatura motori, potenza del segnale di rete (dBm) e cella telefonica agganciata.
- **Logica di generazione:** Non è puramente casuale. Sono stati implementati vincoli di dominio, come malus del segnale associati a specifiche tratte (simulando zone d'ombra) e l'iniezione volontaria di "treni sfortunati" con tassi di guasto maggiore (scarso segnale, anomalia di misura) per testare il rilevamento.

## Punti di forza e limiti

**Punti di Forza:**

- **Versatilità del codice:** l'implementazione duale (SQL nativo vs Method Chaining di Pandas) garantisce robustezza e vuole dimostrare confidenza sia nell'elaborazione su disco che in memoria RAM.
- **Efficienza Pandas:** uso estensivo di operazioni vettorializzate e allineamento degli indici, evitando copie intermedie e loop, gestendo milioni di righe in pochi secondi.
- **Scalabilità della UI:** l'aggiunta di nuovi grafici non richiede modifiche alla logica di orchestrazione grazie all'uso di decoratori e mapping dinamico.
- **Architettura "Shift-Left":** la scelta di delegare l'ordinamento cronologico, la deduplicazione e la generazione di indici a Python garantisce massima efficienza in Power BI, riducendo la cardinalità del modello e abbattendo i tempi di caricamento e calcolo misure.

**Limiti del Modello:**

- **Natura dei dati:** essendo sintetico, il dataset non modella perfettamente l'inerzia termica o le curve di accelerazione reali e coerenti con la meccanica.
- **Architettura Batch:** attualmente la pipeline processa file statici. In uno scenario di produzione reale, l'ingestione dovrebbe essere migrata su un'architettura particolare (es. Kafka) per la telemetria in tempo reale e su sistemi cloud anziché file locali.

## Conclusioni e insights

I report e i grafici prodotti permettono di estrarre tre tipologie di insight operativi:

1. **Manutenzione Predittiva:** l'analisi degli sbalzi termici (calcolati tramite window functions temporali su finestre ristrette) e l'identificazione di guasti cronici isolano rapidamente i treni che necessitano di revisione immediata.
2. **Network Reliability:** l'incrocio tra coordinate, tratte e potenza del segnale (dBm) individua sia le antenne con il peggior *handoff* (cambio di antenna in breve tempo), sia i segmenti ferroviari critici per la connettività.
3. **Sostenibilità e Performance:** mappando il consumo energetico contro la velocità media e la media globale dell'intera flotta, è possibile stilare un ranking di efficienza, distinguendo cali "fisiologici" da inefficienze proprie del veicolo.
