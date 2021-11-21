"""
@title: Arangdb Backup
@description: create a arangodb backup and store in s3 bucket
"""
# import required modules
import argparse
import logging
from colorlog import ColoredFormatter
from pyArango.connection import *
from arango import ArangoClient


class ArangoBackup(object):
    log = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    formatter = ColoredFormatter(
        '%(log_color)s %(process)d-%(levelname)s-%(message)s', '%c')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)
    log.propagate = False

    def initConnect(self, host):
        # Initialize the ArangoDB client.
        self.log.info("[+] INITIALIZING ARANGODB CONNECTION")
        self.log.debug(f"[+] connecting to the {host}")
        client = ArangoClient(
            hosts=host
        )
        self.log.debug(f'[+] successfully connected to {client.hosts}')
        return client

    def dbBackup(self, host, username, password):
        try:
            client = self.initConnect(host)
            self.log.info("[+] CONNECTING TO DATABASE")
            sys_db = client.db(
                "_system",
                username=username,
                password=password
            )
            self.log.debug(f"[+]successfully connected to {sys_db.db_name}")
            self.log.info("[+] INITIALIZATION ARANGODB BACKUP PROCESS")
            # Get the backup API wrapper.
            bakupObj = sys_db.backup
            # create backup
            backupResult = bakupObj.create(
                label="ArangoBackup",
                allow_inconsistent=True,
                force=False,
                timeout=1000
            )
            backup_id = backupResult["backup_id"]
            self.log.debug(f"[+] arangodb backup process done.")
            # retrieve details of backup
            backupDetails = backup_id.get()
            self.log.debug(f'[+] backup details \n '
                           f'{backupDetails}')

        except Exception as exc:
            self.log.error(exc)


backupObj = ArangoBackup()
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, required=True, help="please enter arangodb host")
    parser.add_argument("--username", type=str, required=True, help="please enter arangodb username")
    parser.add_argument("--password", type=str, required=True, help="please enter arangodb password")
    args = parser.parse_args()
    backupObj.dbBackup(args.host, args.username, args.password)
    pass
