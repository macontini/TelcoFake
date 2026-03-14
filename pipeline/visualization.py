from datetime import datetime
from pathlib import Path
from typing import Callable
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from functools import wraps

from conf import logger
from conf import REPORTS_DIR, CHARTS_DIR, CWD

CHARTS_DIR.mkdir(exist_ok=True)

# Stile per i grafici
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

def auto_plot(func: Callable) -> Callable:
    """
    Decoratore per gestione:
    1. Caricamento CSV -> pandas.DataFrame
    2. Interruzione se DataFrame vuoto
    3. Inizializzazione figura matplotlib
    4. Funzione di plottinf
    5. Salvataggio e chiusura
    """
    @wraps(func)
    def wrapper(self: RailwayVisualizer, csv_name: str, *args, **kwargs) -> None:

        # SETUP
        file_path = self.reports_dir / csv_name
        if not file_path.exists():
            logger.warning(f"File {file_path.name} inesistente. Grafico saltato.")
            return

        df = pd.read_csv(file_path)
        if df.empty:
            logger.warning(f"File {file_path.name} vuoto. Grafico saltato.")
            return

        plt.figure()

        # CHIAMATA
        func(self, df, *args, **kwargs)

        # TEARDOWN
        output = self.output_dir / csv_name
        plt.savefig(output.with_suffix('.png'), bbox_inches='tight')
        logger.info(f"Grafico salvato in {output.with_suffix('.png').relative_to(CWD)}")
        plt.close()

    return wrapper

class RailwayVisualizer:
    def __init__(self, reports_dir: Path, output_dir: Path):
        self.reports_dir = reports_dir
        self.output_dir = output_dir
        self.chart_catalog: dict[str, Callable] = {
            "1_hourly_speed_profile.csv": self._plot_hourly_speed,
            "2_top_critical_trains.csv": self._plot_critical_trains,
            "3_signal_by_speed_range.csv": self._plot_signal_vs_speed,
            "4_temperature_anomalies.csv": self._plot_temperature_anomalies,
            "5_segment_blackout_ratio.csv": self._plot_blackout_segments,
            "6_power_efficiency_ranking.csv": self._plot_efficiency_ratio,
            "7_critical_signal_drops.csv": self._plot_signal_drops,
            "8_chronic_failure_trains.csv": self._plot_chronic_failure_trains,
            "9_speed_signal_degradation.csv": self._plot_speed_signal_degradation,
            "10_power_consumption_outliers.csv": self._plot_power_consumption_outliers,
            "11_network_reliability_by_hour.csv": self._plot_network_reliability_by_hour
        }

    def generate_all_charts(self) -> None:
        """Itera sul catalogo e genera tutti i grafici automaticamente."""
        for csv_name, plot_func in self.chart_catalog.items():
            try:
                plot_func(csv_name)
            except Exception as e:
                logger.error(f"Errore durante la generazione del grafico per {csv_name}: {e}")

    @auto_plot
    def _plot_hourly_speed(self, df: pd.DataFrame) -> None:
        """Genera un grafico a linee per la velocità media oraria."""
        sns.lineplot(data=df, x='hour', y='AVG(speed_kmh)', marker='o', color='royalblue', linewidth=2.5)

        plt.title('Andamento Velocità Media dei Treni nelle 24 Ore', fontsize=15, pad=20)
        plt.xlabel('Ora del Giorno', fontsize=12)
        plt.ylabel('Velocità Media (km/h)', fontsize=12)
        plt.xticks(range(0, 24))

    @auto_plot
    def _plot_critical_trains(self, df: pd.DataFrame) -> None:
        """Genera un grafico a barre per i treni con più criticità."""
        sns.barplot(data=df, x='train_id', y='critical_count', hue='train_id', palette='Reds_r', legend=False)        
        plt.title('Top Treni per Numero di Eventi Critici', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Conteggio Stati CRITICAL', fontsize=12)
        plt.xticks(rotation=45) # Ruotiamo le label se ci sono molti treni

    @auto_plot
    def _plot_signal_vs_speed(self, df: pd.DataFrame) -> None:
        """Genera un grafico per mostrare la correlazione segnale/velocità."""
        sns.barplot(data=df, x='speed_ranges', y='avg_signal_str', hue='speed_ranges', palette='viridis', legend=False)
        plt.title('Qualità del Segnale (dBm) in base alla Velocità', fontsize=15, pad=20)
        plt.xlabel('Range Velocità (km/h)', fontsize=12)
        plt.ylabel('Segnale Medio (dBm)', fontsize=12)

    @auto_plot
    def _plot_temperature_anomalies(self, df: pd.DataFrame) -> None:
        """Strip plot per mostrare la distribuzione degli sbalzi termici per ogni treno"""
        sns.stripplot(data=df, x='train_id', y='delta_temp', hue='train_id', palette='flare', size=8, jitter=True, legend=False)
        plt.title('Sbalzi Termici Anomali per Treno (Delta > 20°C)', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Variazione di Temperatura (°C)', fontsize=12)
        plt.xticks(rotation=45)

    @auto_plot
    def _plot_blackout_segments(self, df: pd.DataFrame) -> None:
        """Genera un grafico a barre orizzontali per i blackout di rete per tratta."""
        df = df.sort_values('blackout_ratio', ascending=False).head(10)
        sns.barplot(data=df, x='blackout_ratio', y='line_segment', hue='line_segment', palette='rocket', legend=False)
        plt.title('Copertura Rete: Tratte con più Blackout / Segnale Scarso', fontsize=15, pad=20)
        plt.xlabel('Tasso di Blackout (0 = Mai, 1 = Sempre)', fontsize=12)
        plt.ylabel('Segmento di Linea', fontsize=12)

    @auto_plot
    def _plot_efficiency_ratio(self, df: pd.DataFrame) -> None:
        """Genera un grafico a barre per l'efficienza energetica dei treni."""
        y_col = 'ratio' if 'ratio' in df.columns else df.columns[-1] 

        # Ordiniamo per far risaltare i peggiori a sinistra o a destra
        # Mostriamo solo i 10 peggiori per chiarezza
        df = df.sort_values(y_col, ascending=False).head(10)
        sns.barplot(data=df, x='train_id', y=y_col, hue='train_id', palette='RdYlGn', legend=False)
        plt.title('Top 10 Treni Meno Efficienti (Power/Speed Ratio)', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Indice di Inefficienza (kW / km/h)', fontsize=12)
        plt.xticks(rotation=45) # Ruotiamo le label se ci sono molti treni

    @auto_plot
    def _plot_signal_drops(self, df: pd.DataFrame) -> None:
        """Genera un grafico a dispersione (scatter) per evidenziare i drop di segnale critici."""
        sns.barplot(data=df, x='cell_tower_id', y='signal_drop', hue='cell_tower_id', palette='Reds_r', legend=False)
        plt.title('Top 15 Antenne più Instabili (Frequenza di Drop > 50 dBm)', fontsize=15, pad=20)
        plt.xlabel('ID Antenna Cella', fontsize=12)
        plt.ylabel('Ampiezza Sbalzo Segnale (dBm)', fontsize=12)
        plt.xticks(rotation=45) # Inclina le etichette per non farle toccare

    @auto_plot
    def _plot_chronic_failure_trains(self, df: pd.DataFrame) -> None:
        sns.barplot(data=df, x='train_id', y='critical_percentage', hue='train_id', palette='magma', legend=False)
        plt.title('Treni con Guasti Cronici (% Stati Critical)', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Percentuale Criticità (> 10%)', fontsize=12)
        plt.xticks(rotation=45)

    @auto_plot
    def _plot_speed_signal_degradation(self, df: pd.DataFrame) -> None:
        # delta_signal è la differenza tra segnale ad alta velocità e bassa velocità
        sns.barplot(data=df, x='train_id', y='delta_signal', hue='train_id', palette='coolwarm', legend=False)
        plt.title('Degrado Segnale ad Alta Velocità (Delta dBm)', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Peggioramento Segnale (dBm)', fontsize=12)
        plt.xticks(rotation=45)

    @auto_plot
    def _plot_power_consumption_outliers(self, df: pd.DataFrame) -> None:
        # Coloriamo in verde gli efficienti e in rosso gli inefficienti usando la colonna 'efficiency_status'
        sns.barplot(data=df, x='train_id', y='delta_from_baseline', hue='efficiency_status', palette={'Efficient': 'mediumseagreen', 'Inefficient': 'tomato'})
        plt.title('Scostamento Consumo Energetico dalla Media (Baseline)', fontsize=15, pad=20)
        plt.xlabel('ID Treno', fontsize=12)
        plt.ylabel('Delta Consumo/Velocità', fontsize=12)
        plt.xticks(rotation=45)

    @auto_plot
    def _plot_network_reliability_by_hour(self, df: pd.DataFrame) -> None:
        sns.lineplot(data=df, x='hour', y='bad_signal_percentage', marker='s', color='darkorange', linewidth=2.5)
        plt.title('Affidabilità Rete per Fascia Oraria (% Segnale Scarso/Offline)', fontsize=15, pad=20)
        plt.xlabel('Ora del Giorno', fontsize=12)
        plt.ylabel('% Tempo con Problemi di Rete', fontsize=12)
        plt.xticks(range(0, 24))

if __name__ == "__main__":

    start_time = datetime.now()

    visualizer = RailwayVisualizer(REPORTS_DIR, CHARTS_DIR)

    logger.info("Generazione grafici in corso...")
    visualizer.generate_all_charts()
    logger.info(f"Tempo impiegato per la creazione dei grafici: {(datetime.now() - start_time).total_seconds():.2f} secondi.")
