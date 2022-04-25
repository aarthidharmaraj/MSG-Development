"""This module has the script for moving files from sftp folder to s3 folder"""
import configparser
import re
from datetime import datetime
import logging
from sre_constants import SUCCESS
from S3.s3_client import S3Details
from sftp_connection import SftpConnection

config = configparser.ConfigParser()
config.read("details.ini")

logging.basicConfig(
    filename="sftptos3_logfile.log", format="%(asctime)s %(message)s", filemode="w"
)
logger = logging.getLogger()


class MoveSftpToS3:
    """This is the class for moving file sftp to s3"""

    def __init__(self):
        """This is the init method for the class of MoveFileSftpToS3"""
        self.sftp_conn = SftpConnection()
        self.s3_client = S3Details()
        self.sftp_path = config["SFTP"]["sftp_path"]

    def move_file_to_s3(self):
        """This method moves the file from sftp to s3"""
        sftp_file_list = self.sftp_conn.list_files()
        for file_name in sftp_file_list:
            copy_source = self.sftp_path + file_name
            self.put_path_partition(file_name, copy_source)
        return 'success'
    def put_path_partition(self, file_name, sftp_source):
        """This method uses partioning of path and upload the file to S3"""
        try:
            if re.search("[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4}.(csv)", file_name):

                date_object = datetime.strptime(file_name, "%d.%m.%Y.csv")
                partition_path = (
                    "pt_year="
                    + date_object.strftime("%Y")
                    + "/pt_month="
                    + date_object.strftime("%m")
                    + "/pt_day="
                    + date_object.strftime("%d")
                )
                # self.sftp_conn.get_file(file_name, partition_path,sftp_source) or
                self.s3_client.upload_file(file_name, partition_path, sftp_source)
                rename = self.sftp_conn.rename_file(file_name)
                print(
                    "The file has been uploaded to s3 and rename in sftp path", rename
                )
                logger.info("The file has been uploaded to s3 and rename in sftp path")
                return 'success'
            else:
                print("The", file_name, "is not in the prescribed format")
                logger.error("The file is not in the prescribed format")
                return 'failure'
        except Exception as err:
            print("Cannot be uploaded in S3 in the parttioned path", err)
            logger.error("The file cannot be uploaded in the given path in s3")
            return 'failure'

def main():
    """This is the main method for the module name move_file_sftp_to_s3"""
    move_sftp_to_s3 = MoveSftpToS3()
    move_sftp_to_s3.move_file_to_s3()


if __name__ == "__main__":
    main()
