import shutil
import logging
from typing import Dict, Any
from common.utils import get_param

logger = logging.getLogger(__name__)


def clean_up(**context: Dict[str, Any]) -> None:
    logger.info('Cleanup')

    folder_name = get_param('local_folder_name', **context)
    shutil.rmtree(folder_name)
