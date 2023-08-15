from typing import Generator
from typing import Optional
from typing import Tuple


def get_return_value_from_xcom(task_id: str, **context) -> Optional[str]:
    """

    :param task_id:
    :param context:
    :return:

    """

    dag_id = context["dag"].dag_id

    if '_for_' in task_id:
        task_id = f'{task_id}{dag_id}'

    try:
        return context['ti'].xcom_pull(
            key='return_value',
            task_ids=(task_id,),
            dag_id=dag_id)[0]
    except KeyError:
        return None


def get_range(file_size: int, chunk_size: int) -> Generator[Tuple[int, int], None, None]:
    """

    :param file_size:
    :param chunk_size:
    :return:

    """

    start_range = 0
    end_range = min(chunk_size, file_size)

    while start_range < file_size:
        yield start_range, end_range
        start_range = end_range
        end_range = end_range + min(chunk_size, file_size - end_range)
