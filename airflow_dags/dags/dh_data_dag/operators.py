"""
Operator for DH which downloads files over sftp and if failed - over https.
"""
import logging
import shutil
from pathlib import Path
from typing import Any
import requests
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.utils.xcom import get_return_value_from_xcom
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class DHDownloadOperator(SFTPOperator):

    """
    Download ZIP file from SFTP server.
    Paramiko produces different errors (EOFError etc) while downloading huge
    files. If this case occured we download the archive with HTTPS.
    """

    def execute(self, context: Any) -> str:
        """Download archive with SFTP or HTTPS if failed."""
        try:
            res = super().execute(context)
        except (AirflowException, EOFError) as exc:
            logger.warning(
                "Failed to download a file %s with SFTP. "
                "Trying to download with HTTPS: %s", self.local_filepath, exc)
            res = self.download_https_file()
        self.upload_to_s3(res, context)
        return res

    def upload_to_s3(self, fname: str, context: Any) -> None:
        """Upload file to S3 bucket."""
        s3_url = get_return_value_from_xcom('set_up_s3_bucket_for_', **context)
        name = fname.split("/")[-1]
        s3_key = f'{s3_url}/{name}'
        S3Hook().load_file(
            fname,
            s3_key,
            replace=True)
        logger.info("Upload to S3 completed: %s", s3_key)

    def download_https_file(self) -> None:
        """Download ZIP archive with HTTPS.
        Definitive Healthcare are using the Cerberus FTP Server which
        has an additional Web UI. We log in as a SFTP user and download
        self.remote_filepath over HTTPS.
        """
        con = Connection.get_connection_from_secrets(self.ssh_conn_id)
        login_url = f"https://{con.host}/login"
        file_url = f"https://{con.host}/file/d{self.remote_filepath}"
        with requests.Session() as client:
            resp = client.get(login_url)
            soup = BeautifulSoup(resp.text, 'lxml')
            csrf_token = soup.select_one('meta[name="csrftoken"]')['content']

            client.post(
                login_url + '/user',
                data={'username': con.login,
                      'password': con.password,
                      'csrftoken': csrf_token},
                headers=dict(Referer=login_url))
            return self.download_file(client, file_url)

    def download_file(self, client: requests.Session, file_url: str) -> str:
        """
        Download file_url content with client and save it in the
        self.local_filepath.
        """
        data_dir = Path(self.local_filepath).parent
        if not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
        with client.get(file_url, stream=True) as resp:
            with open(self.local_filepath, 'wb') as out_f:
                shutil.copyfileobj(resp.raw, out_f)
        return self.local_filepath
