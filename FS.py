import stat
import os
import llfuse
import errno
from mysql.connector.connection import MySQLConnection


class FS(llfuse.Operations):
    def __init__(self, db: MySQLConnection):
        super(FS, self).__init__()
        self.db = db

    def create_entry(self, inode, size: int, st_mode: int):
        entry = llfuse.EntryAttributes()
        entry.st_mode = st_mode
        entry.st_size = size
        stamp = int(0)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode
        return entry

    def getattr(self, inode, ctx=None):
        if inode == llfuse.ROOT_INODE:
            return self.create_entry(inode, llfuse.ROOT_INODE, stat.S_IFDIR | 0o755)

        inode_data = None
        with self.db.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM inodes WHERE id = %s;", (inode,))
            inode_data = cursor.fetchone()

        if inode_data is None:
            raise llfuse.FUSEError(errno.ENOENT)

        return self.create_entry(
            inode, len(inode_data["content"]), stat.S_IFREG | 0o644
        )

    def readdir(self, inode, start_id):
        if start_id == 0:
            start_id = -1

        rows = []
        with self.db.cursor(dictionary=True) as cursor:
            cursor.execute(
                """SELECT * FROM files 
                    WHERE parent_inode=%s AND id > %s 
                    ORDER BY id""",
                (inode, start_id),
            )
            rows = cursor.fetchall()

        for row in rows:
            yield (bytes(row["name"], "utf-8"), self.getattr(row["inode"]), row["id"])

    def opendir(self, inode, ctx):
        return inode

    def access(self, inode, mode, ctx):
        return True

    def releasedir(self, inode):
        return True

    def lookup(self, inode_parent, name, ctx=None):
        if name == ".":
            return self.getattr(inode_parent)

        if name == "..":
            inode = None
            with self.db.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM files WHERE inode=%s", (inode_parent,))
                inode = cursor.fetchone()["parent_inode"]
            return self.getattr(inode)

        results = []
        with self.db.cursor(dictionary=True) as cursor:
            cursor.execute(
                "SELECT * FROM files WHERE name=%s AND parent_inode=%s",
                (name, inode_parent),
            )
            results = cursor.fetchall()

        if results == []:
            raise (llfuse.FUSEError(errno.ENOENT))
        else:
            return self.getattr(results[0]["inode"])

    def open(self, inode, flags, ctx):
        return inode

    def create(self, inode_parent, name, mode, flags, ctx):
        if (self.getattr(inode_parent)).st_nlink == 0:
            raise llfuse.FUSEError(errno.EINVAL)
        with self.db.cursor() as cursor:
            cursor.execute(
                'INSERT INTO inodes (content) VALUES("")',
            )
            inode = cursor.lastrowid

            cursor.execute(
                "INSERT INTO files (name, inode, parent_inode) VALUES(%s,%s,%s)",
                (name, inode, inode_parent),
            )
            entry = self.getattr(inode)
            return (entry.st_ino, entry)

    def read(self, inode, offset, length):
        inode_data = None
        with self.db.cursor() as cursor:
            cursor.execute("SELECT content FROM inodes WHERE id = %s;", (inode,))
            inode_data = cursor.fetchone()

        if inode_data is None:
            return b""

        return bytes(inode_data[0], "utf-8")[offset : offset + length]

    def write(self, file_descriptor, offset, buf):
        content = None
        with self.db.cursor() as cursor:
            cursor.execute("SELECT content FROM inodes WHERE id=%s", (file_descriptor,))
            content = bytes(cursor.fetchone()[0], "utf-8")
        if content is None:
            content = b""
        content = content[:offset] + buf + content[offset + len(buf) :]

        with self.db.cursor() as cursor:
            cursor.execute(
                "UPDATE inodes SET content=%s WHERE id=%s", (content, file_descriptor)
            )
        return len(buf)

    def release(self, file_descriptor):
        return True

    # TODO: Actually set the attributes
    def setattr(self, inode, attr, fields, file_descriptor, ctx):
        return self.getattr(inode)
