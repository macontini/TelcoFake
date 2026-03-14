from pathlib import Path
import yaml
import logging

# ----- CONFIG -----
CWD = Path(__file__).parent.parent.resolve()
CONFIG_YAML = CWD / 'config.yaml'
with open(CONFIG_YAML, 'r') as f:
    config = yaml.safe_load(f)

# ----- LOGGING -----
LOG_DIR: Path = CWD / config['paths']['log_dir']
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_DIR / 'app.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----- CONSTANTS -----
DATA_DIR: Path = CWD / config['paths']['data_dir']
DB_PATH: Path = CWD / config['sql']['db_fname']
REPORTS_DIR: Path = CWD / config['paths']['reports']
CHARTS_DIR: Path = CWD / config['paths']["charts"]
