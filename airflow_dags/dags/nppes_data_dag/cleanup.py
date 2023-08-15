import logging
import shutil
from pathlib import Path

from common.utils.xcom import get_return_value_from_xcom


logger = logging.getLogger(__name__)


def clean_up_tmp_dir(**context) -> None:
    """
    Clean up temporary folder
    """
    tmp_dir_path = get_return_value_from_xcom(task_id='set_up_tmp_folder_for_', **context)
    logger.info(f'Clean up temporary folder: {tmp_dir_path}')
    path = Path(tmp_dir_path)

    if path.exists():
        shutil.rmtree(path)
