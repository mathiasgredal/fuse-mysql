from argparse import ArgumentParser
import traceback
import llfuse
import mysql.connector
from mysql.connector.connection import MySQLConnection

from FS import FS

# Database connection info
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "fuse"

# Mount location
MOUNT_DIR = "./mnt"


def setup_db() -> MySQLConnection:
    global DB_HOST, DB_USER, DB_PASS, DB_NAME
    # First connection is used to create the database
    db = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS)

    with db.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS fuse")

    db.close()

    # Second connection sets up the tables
    db = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=True
    )

    with db.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS inodes (
                id        INTEGER PRIMARY KEY AUTO_INCREMENT,
                content   TEXT NOT NULL
            ) AUTO_INCREMENT = 2
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id              INTEGER PRIMARY KEY AUTO_INCREMENT,
                name            VARCHAR(256) NOT NULL,
                inode           INT NOT NULL REFERENCES inodes(id),
                parent_inode    INT NOT NULL REFERENCES inodes(id),
                UNIQUE (name, parent_inode)
            )
        """
        )

    return db


def seed_db(db: MySQLConnection):
    with db.cursor() as cursor:
        cursor.execute(
            """
            TRUNCATE TABLE inodes;
            """
        )
        cursor.execute(
            """
            TRUNCATE TABLE files;
            """
        )
        cursor.execute(
            """
            INSERT INTO inodes (id, content) VALUES 
            (2, 'world');
            """
        )
        cursor.execute(
            """
            INSERT INTO files (name, inode, parent_inode) VALUES 
            ("hello.txt", 2, 1);
            """
        )


def parse_args():
    parser = ArgumentParser()

    parser.add_argument(
        "mountpoint", type=str, default="mnt", help="Where to mount the file system"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Enable debugging output"
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        default=False,
        help="Clears and reseeds database with test data",
    )

    return parser.parse_args()


if __name__ == "__main__":
    print("Starting MySQL FUSE...")
    options = parse_args()
    db = setup_db()

    if options.seed:
        seed_db(db)

    fs = FS(db)
    fuse_options = set(llfuse.default_options)
    fuse_options.add("fsname=mysql")
    fuse_options.discard("default_permissions")
    if options.debug:
        fuse_options.add("debug")
    llfuse.init(fs, options.mountpoint, fuse_options)

    try:
        llfuse.main()
    except Exception:
        traceback.print_exc()
    finally:
        llfuse.close(unmount=True)
        db.close()
        print("Shutting down MySQL FUSE...")
