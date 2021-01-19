#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backup BTRFS subvolumes

The script makes backups of a subvolume on a BTRFS volume.
The snapshots could be copied to other BTRFS volumes, FAT filesystem or other
filesystem. A history of old snapshots is stored on BTRFS volumes only.

The snapshots will be named "NAME-Snapshot-YYYY-MM-DD" where NAME is the name
of the subvolume, e.g. @Data, and YYYY-MM-DD is the creation date.

On the source volume each backup starts with a snapshot creation. But only one
snapshot per day will be created. A second call of the script at the same day
will reuse the existing snapshot on the source volume.

To BTRFS volumes a send - receive of snapshots is done.

To other volumes a rsync is done. To FAT volumes the simple "--archive" flag
is not used because user information could not stored on FAT volumes. To all
other filesystems a "rsync --archive" call is used. To FAT volumes a time
tolerance of 3700s (= 1h + 100s) is used to prevent a copy of all files after
DST change because FAT uses the local time.
Attention: Files changed only maximal 1 hour after backup will be not copied
to a FAT backup volume. The old file content will be remain. I decide a one
hour old copy of such a very rarely changed file is acceptable.

Each snapshot or rsync copied subvolume is checked by comparing some test
files. If the content of the test files are not equal, then an error message
will be logged. The backup process will be continued also if invalid backup
files are found.

On BTRFS volumes the number of old backups is defined by the parameter keep
in the configuration. The last n backups, the last backup of the last n month
and the last backup of the last n years will be stored. Other older backups
will be deleted.

On other volumes the number of backups is constant. A new bachup will replace
the oldes backup on the volume.

The program return code is 0 on success and 1 on a non recoverable error.

The default configuration is ubackup.conf located  beside the script file,
in the current work directory or in ~/.config.

The configuration file has json format. The subvolumes to backup and the
logging is configured in the file.

The top level element in the configuration is a dictionary with the two keys
backup and logging.

The configuration of the subvolumes to backup is in a dictionary with the key
backup. In this sub dictionary the key is the name of the volume, the
value is a dictionary with details to the volume. The details are:
- subvolume: Name of the BTRFS subvolume. For backups: the name without
  "-Snapshot-YYYY-MM-DD". This extension is added internal by the script.
  For not-BTRFS volumes: a directory in the root is used like a subvolume.
- uuid: UUID of the volume used to mount the volume.
- type: "source" or "destination".
- last-snapshot: null or the date of the last stored snapshot.
- keep: A dictionary with three entries: day, month and year. The number of
  old backups to keep on the volume. day: the latest n backups are kept.
  month: the latest backup per month of the last n month is kept. year: the
  latest backup per year of the last n years are kept. (The simplifications
  30 days = 1 month and 365 days = 1 year are used.)
  The keep entry is used on BTFS volumes only.
- comment: Comment text to the subvolume.

The configuration of the logging is a dictionary with the key logging.
The dictionary is passed throw the Python logging.config.dictConfig function.
A -v or -vv switch overwrites the logging levels set in the config file.
"""

###############################################################################
#
# Author: Ulrich Berntien
# Initial date: 2019-03-26
# Language: Python 3.6.7
#
# MIT License
# Copyright (c) 2019 Ulrich Berntien
#
###############################################################################


import argparse
import datetime
import filecmp
import glob
import json
import logging
import logging.config
import os
import subprocess
import sys
import tempfile
import time
from typing import *

###############################################################################

# Version
BACKUP_VERSION = "2021-01-19"

# Default configuration file name.
# Default location is beside the script.
CONFIG_FILE = "ubackup.conf"

# The date of the backup is a constant during running the backup script.
# The date at the script start is used.
# Because the snapshots containing the date only one snapshot per day is
# possible with this tool. More snapshots are possible if e.g. the hour
# is added to the backup date stamp.
BACKUP_DATE = datetime.date.today().isoformat()

# Number of files to compare after a snapshot or a copy.
CHECK_FILE_COUNT = 30

# The name part of all snapshot subvolumes.
SNAPSHOT_NAME_MIDDLE = "-Snapshot-"

# The name of the snapshots and the copied snapshots on the backup volumes
# is "subvolume" + SNAPSHOT_APPENDIX.
SNAPSHOT_NAME_APPENDIX = SNAPSHOT_NAME_MIDDLE + BACKUP_DATE


###############################################################################


def precondition(condition: bool) -> None:
    """
    Raises a exception on condition is false.
    Bugs in a backup program are fatal. Hence not the assert is used but this
    strict exception throwing function is used.
    If a condition is False, then a program bug must exists causing this
    failure. Only program errors are handled by the precondition calls.
    Configuration errors, user errors, etc. are handled soft.
    :param condition: True if the program works correct.
    """
    if not condition:
        raise RuntimeError("Precondition failed. Program bug.")


def not_empty_str(string: str) -> bool:
    """
    Checks string.
    :param string: Check this string.
    :return: True if and only if string is a not-empty string.
    """
    return isinstance(string, str) and string


def error_raise(message: str) -> None:
    """
    Logs an error message and raise a RuntimeError exception.
    :param message: The error message.
    """
    logging.error(message)
    raise RuntimeError(message)


###############################################################################


class UTCFormatter(logging.Formatter):
    """
    Logging Formatter class to set UTC timestamps in log file.
    The local time is difficult to analyse during DST time changes.
    Therefore the log file uses UTC.
    """
    converter = time.gmtime


###############################################################################


class Config:
    """
    Handling the configuration file.
    The class is used like a singleton object: The methods are class-methods.
    The members are class-variables.
    """

    # The configuration in a dict.
    # This dict is read from and written to the configuration file.
    _raw: dict = None

    # The backup configuration.
    # The dictionary is a value of the _raw_config dictionary.
    _backup: dict = None

    # Status of the configuration data in _raw_config.
    # 0 = no config, 1 = config loaded, 2 = config changed
    _status: int = 0

    # Verbose level.
    # 0: no info message output
    # 1: info message output from this script
    # 2: level 1 + verbose pass through to all called tools
    _verbose: int = 0

    @staticmethod
    def get_default() -> str:
        """
        Get default configuration file name.
        Configuration file searched .config or beside the script.
        :return: File name with absolute path.
        """
        # Search configuration file
        for base in (os.path.dirname(os.path.realpath(__file__)),
                     os.getcwd(),
                     os.path.expanduser("~/.config")):
            test = os.path.join(base, CONFIG_FILE)
            if os.path.exists(test):
                return test
        # Config file not found at default locations
        return ""

    @classmethod
    def set_verbose(cls, level) -> None:
        """
        Stores the verbose level.
        Sets the ERROR or INFO logging level on verbose level 0 or >0.
        :param level: integer (0,1 or 2) or other type (False => 0, True => 1)
        """
        if isinstance(level, int):
            precondition(0 <= level <= 3)
            cls._verbose = level
            # Sets level to all logging steps
            for item in logging.getLogger().handlers + [logging.getLogger()]:
                item.setLevel(logging.INFO if cls._verbose > 0 else logging.ERROR)
        # else:
        #   ignore level==None, do not overwrite current logging configuration
        return

    @classmethod
    def get_verbose(cls) -> int:
        """
        Get the current verbose level.
        :return: 0, 1, or 2.
        """
        return cls._verbose

    @classmethod
    def check_item(cls, name: str) -> bool:
        """
        Checks if a valid backup configuration item is given.
        Log errors if the configuration item is not valid.
        :param name: Name of the backup configuration item.
        :return: True if and only if the configuration item is valid.
        """
        precondition(not_empty_str(name))
        precondition(cls._status > 0)
        if name not in cls._backup:
            logging.error(f"item {name} not defined.")
            return False
        cfg = cls._backup[name]
        if "type" not in cfg or cfg["type"] not in ("destination", "source"):
            logging.error(f"item {name} has no valid type")
            return False
        if "uuid" not in cfg or not cfg["uuid"]:
            logging.error(f"item {name} has no uuid")
            return False
        if "subvolume" not in cfg:
            logging.error(f"item {name} contains no subvolume name")
            return False
        if "keep" in cfg:
            # The keep item is only needed for BTRFS volumes and a default
            # value exists. So the keep item is optional. If the item exists
            # the values must be correct.
            keep = cfg["keep"]
            for part, lowest in (("day", 1), ("month", 0), ("year", 0)):
                if part not in keep:
                    logging.error(f"item {name} contains no '{part}' value in the 'keep' item")
                    return False
                if not isinstance(keep[part], int) or keep[part] < lowest:
                    logging.error(f"item {name} 'keep.{part}' value is no integer or less than {lowest}")
                    return False
        return True

    @classmethod
    def check_destination(cls, name: str) -> bool:
        """
        Checks if a valid destination is given.
        Log errors if the item is not a destination.
        :param name: Name of the backup destination.
        :return: True if and only if the destination is valid.
        """
        precondition(not_empty_str(name))
        precondition(cls._status > 0)
        if not cls.check_item(name):
            return False
        cfg = cls._backup[name]
        if cfg["type"] != "destination":
            logging.error(f"backup destination {name} has not type destination")
            return False
        subvolume = cls._backup[name]["subvolume"]
        number_sources = 0
        for value in cls._backup.values():
            if value["subvolume"] == subvolume and value["type"] == "source":
                number_sources += 1
        if number_sources == 0:
            logging.error(f"no source defined for backup destination {name}")
            return False
        if number_sources > 1:
            logging.error(f"more than one source defined for backup destination {name}")
            return False
        return True

    @classmethod
    def check_source(cls, name: str) -> bool:
        """
        Checks if a valid source subvolume configuration is given.
        Log errors if the item is not a source subvolume.
        :param name: Name of the backup source subvolume.
        :return: True if and only if the source subvolume configuration is valid.
        """
        precondition(not_empty_str(name))
        precondition(cls._status > 0)
        if not cls.check_item(name):
            return False
        cfg = cls._backup[name]
        if cfg["type"] != "source":
            logging.error(f"backup source {name} has not type source")
            return False
        return True

    @classmethod
    def is_destination(cls, name: str) -> str:
        """
        Checks if the configuration has type destination.
        :param name:  The name of the configuration item.
        :return: True if and only if the item has type destination.
        """
        precondition(cls.check_item(name))
        return cls._backup[name]["type"] == "destination"

    @classmethod
    def list_name(cls, uuid: List[str]) -> List[str]:
        """
        Lists the names of all subvolumes on volumes with the given uuid.
        Destination and source subvolumes will be included in the list.
        The returned list could be empty.
        :param uuid: list of UUIDs
        :return: List of names.
        """
        precondition(cls._status > 0)
        return [name
                for name in cls._backup.keys()
                if cls._backup[name]["uuid"] in uuid]

    @classmethod
    def list_last_snapshots(cls, subvolume: str) -> List[str]:
        """
        Lists the last snapshot dates of all backups for one subvolume.
        :param subvolume: The name of the subvolume.
        :return: List of the last snapshot dates. The dates are stored as str.
        """
        precondition(cls._status > 0)
        return [cls._backup[name]["last-snapshot"]
                for name in cls._backup.keys()
                if cls._backup[name]["subvolume"] == subvolume]

    @classmethod
    def get_source(cls, destination: str) -> str:
        """
        Gets the name of the backup source volume.
        :param destination:  The name of the destination volume.
        :return: Name of the source volume.
        """
        precondition(cls.check_destination(destination))
        subvolume = cls._backup[destination]["subvolume"]
        for key, value in cls._backup.items():
            if value["subvolume"] == subvolume and value["type"] == "source":
                return key
        return ""

    @classmethod
    def get_subvolume(cls, name: str) -> str:
        """
        Gets the subvolume name of the backup volume.
        :param name: The name of the source or destination volume.
        :return: The uuid.
        """
        precondition(cls.check_item(name))
        return cls._backup[name]["subvolume"]

    @classmethod
    def get_uuid(cls, name: str) -> str:
        """
        Gets the uuid of the backup volume.
        :param name: The name of the source or destination volume.
        :return: The uuid.
        """
        precondition(cls.check_item(name))
        return cls._backup[name]["uuid"]

    @classmethod
    def get_keep(cls, name: str) -> Dict:
        """
        Gets the number backups/snapshots to keep on the volume.
        :param name: The name of the source or destination volume.
        :return: The numbers to keep with time distance of a day, month and year.
        """
        precondition(cls.check_item(name))
        item: Dict = cls._backup[name]
        if "keep" in item.keys():
            return item["keep"]
        else:
            # The default number of snapshots/backups to keep
            return {"day": 7, "month": 6, "year": 5}

    @classmethod
    def set_last_snapshot(cls, name: str, date: str) -> None:
        """
        Set the date of the last snapshot.
        :param name: Name of the backup volume.
        :param date: The date of the snapshot.
        """
        precondition(cls.check_item(name))
        precondition(not_empty_str(date))
        if "last-snapshot" in cls._backup[name].keys():
            old_value = cls._backup[name]["last-snapshot"]
        else:
            old_value = None
        if old_value != date:
            cls._backup[name]["last-snapshot"] = date
            cls._status = 2

    @classmethod
    def load(cls, file) -> None:
        """
        Load the backup configuration file.
        :param file: The open configuration file.
        :return: The loaded configuration.
        """
        precondition(file.readable())
        logging.info(f"load configuration {file.name}")
        if cls._status == 1:
            logging.info("Reload configuration")
        if cls._status == 2:
            logging.warning("Reload configuration, revert changes")
        # Reset the status
        cls._status = 0
        try:
            cls._raw = json.load(file)
            if "backup" not in cls._raw:
                error_raise("no backup entry in configuration")
            if not isinstance(cls._raw["backup"], dict):
                error_raise("invalid backup configuration")
            cls._backup = cls._raw["backup"]
            if "logging" in cls._raw:
                logging.config.dictConfig(cls._raw["logging"])
            else:
                logging.warning("no logging configuration")
            cls._status = 1
        except json.JSONDecodeError as error:
            error_raise(f"Configuration file error {error}")

    @classmethod
    def update(cls, file) -> None:
        """
        Updates the configuration files.
        Write the file only if the configuration was changed.
        :param file: Writes into this file.
        """
        precondition(file.readable())
        if cls._status == 2:
            if file.writable():
                file.seek(0)
                file.truncate()
            else:
                logging.info("Reopen configuration file for write")
                file.close()
                file = open(file.name, "wt")
            logging.info("write configuration file")
            if cls._raw["backup"] is not cls._backup:
                cls._raw["backup"] = cls._backup
            logging.warning("internal backup config link was fixed")
            json.dump(cls._raw, file, indent=4, sort_keys=True)


###############################################################################


class BlockDeviceList:
    """
    Access block device list created by lsblk.
    The block device list is NOT cached.
    For a backup program the correct and up-to-date date is more important
    than a small a shorter runtime.
    """

    @staticmethod
    def _lsblk() -> Dict:
        """
        Call lsblk and return block device list in a dict.
        :return: The lsblk output in a dict.
        """
        sub = subprocess.Popen(
            ["lsblk", "--json", "--output", "uuid,fstype,mountpoint"],
            stdout=subprocess.PIPE)
        stdout, stderr = sub.communicate()
        if sub.returncode:
            error_raise("lsblk failed")
        return json.loads(stdout)

    @staticmethod
    def _get(lsblk: Dict, uuid: str, entry: str) -> str:
        """
        Gets entry for uuid from the block device list.
        :param lsblk: The output of a lsblk call.
        :param uuid: The uuid of the block device.
        :param entry: This entry should be read.
        :return: The value of the entry for the volume uuid.
        """
        precondition(not_empty_str(uuid))
        precondition(not_empty_str(entry))
        if "blockdevices" not in lsblk.keys():
            error_raise("lsblk json structure unknown")
        for item in lsblk["blockdevices"]:
            if item["uuid"] == uuid:
                return item[entry]
        error_raise(f"volume {uuid} is not present")

    @staticmethod
    def is_fat(uuid: str) -> bool:
        """
        Check if the volume has a FAT file system.
        :param uuid: The uuid of the volume.
        :return: True if and only if the volume has a FAT file system.
        """
        precondition(not_empty_str(uuid))
        info = BlockDeviceList._lsblk()
        return BlockDeviceList._get(info, uuid, "fstype") in ("exfat", "msdos", "vfat")

    @staticmethod
    def is_btrfs(uuid: str) -> bool:
        """
        Checks if the volume has a BTRFS file system.
        :param uuid: The uuid of the volume.
        :return: True if and only if the volume has a BTRFS file system.
        """
        precondition(not_empty_str(uuid))
        info = BlockDeviceList._lsblk()
        return BlockDeviceList._get(info, uuid, "fstype") == "btrfs"

    @staticmethod
    def mount_point(uuid: str) -> str:
        """
        Gets the mount point of the volume.
        :param uuid: The uuid of the volume.
        :return: Mount point or None if not mounted.
        """
        precondition(not_empty_str(uuid))
        info = BlockDeviceList._lsblk()
        return BlockDeviceList._get(info, uuid, "mountpoint")

    @staticmethod
    def is_mounted(uuid: str) -> bool:
        """
        Checks if the volume is mounted.
        :param uuid: The uuid of the volume.
        :return: True if and only if the volume is mounted.
        """
        precondition(not_empty_str(uuid))
        return BlockDeviceList.mount_point(uuid) is not None

    @staticmethod
    def list_uuid() -> List[str]:
        """
        Gets list of the UUIDs of all connected block devices.
        The returned list could be empty.
        :return: List of UUIDs
        """
        devices = BlockDeviceList._lsblk()
        return [x["uuid"] for x in devices["blockdevices"] if "uuid" in x and x["uuid"]]


###############################################################################


class Run:
    """
    Runs system programs.
    """

    @staticmethod
    def _verbose() -> List[str]:
        """
        Returns optional the verbose switch.
        The verbose switch is passed through to the called tools if
        verbose level is 2 or higher.
        :return: List with verbose switch or empty list.
        """
        return ["--verbose"] if Config.get_verbose() >= 2 else []

    @staticmethod
    def _verbose_short() -> List[str]:
        """
        Returns optional the short (-v) verbose switch.
        The verbose switch is passed through to the called tools if
        verbose level is 2 or higher.
        :return: List with short verbose switch (-v) or empty list.
        """
        return ["-v"] if Config.get_verbose() >= 2 else []

    @staticmethod
    def mount(uuid: str, mount_point: str, readonly: bool) -> None:
        """
        Mounts a volume.
        If not BTRFS volume is already mounted, then a bind mount will be
        done. BTRFS volumes could be mounted several times.
        On an error: logs an error message and raises a Runtime exception.
        :param uuid: The uuid of the volume to mount.
        :param mount_point: Prepared mount point
        :param readonly: True if the volume should be mounted as readonly
        """
        precondition(not_empty_str(uuid))
        precondition(os.path.exists(mount_point))
        logging.info(f"mount {uuid}{' as readonly' if readonly else ''}")
        options = f"noatime,nodev,lazytime"
        if readonly:
            options += ",ro"
        current_mount_point = BlockDeviceList.mount_point(uuid)
        is_btrfs = BlockDeviceList.is_btrfs(uuid)
        if current_mount_point and not is_btrfs:
            options += ",bind"
            source = ["--bind", current_mount_point]
        else:
            source = ["--uuid", uuid]
        error = subprocess.call(
            ["mount"] + Run._verbose() + ["--no-mtab", "--options", options,
                                          "--target", mount_point] + source)
        if error:
            error_raise(f"mount of {uuid} failed")
        return

    @staticmethod
    def sync() -> None:
        """
        Synchronize caches with the storage devices.
        The sync could be take a while after backup or remove operations
        with a lot of involved files.
        """
        error = subprocess.call(["sync"])
        if error:
            error_raise("sync failed")
        return

    @staticmethod
    def umount(mount_point: str) -> None:
        """
        Unmount a file system.
        On an error: logs an error message and raises a Runtime exception.
        :param mount_point: The mount point of the file system.
        """
        precondition(os.path.exists(mount_point))
        error = subprocess.call(["umount"] + Run._verbose() +
                                ["--lazy", "--no-mtab", mount_point])
        if error:
            error_raise("umount failed")
        return

    @staticmethod
    def snapshot(source: str, destination: str) -> None:
        """
        Create readonly snapshot on BTRFS volumes.
        :param source: Name of a subvolume to copy.
        :param destination: Name of snapshot to create.
        """
        precondition(os.path.exists(source))
        precondition(not_empty_str(destination))
        precondition(not os.path.exists(destination))
        error = subprocess.call(["btrfs", "subvolume",
                                 "snapshot", "-r",
                                 source, destination])
        if error or not os.path.exists(destination):
            error_raise(f"make snapshot {source} to {destination} failed")
        return

    @staticmethod
    def copy_snapshot(source: str, destination: str, common_snapshot) -> None:
        """
        Copies snapshot from BTRFS volume to other BTRFS volume.
        Uses incremental update if a common snapshot exists.
        On an error: logs an error message and raises a Runtime exception.
        :param source: Path to snapshot to copy.
        :param destination: Path to destination. Path without the snapshot name.
        :param common_snapshot: None or a common snapshot in source.
        """
        precondition(os.path.exists(source))
        precondition(os.path.exists(destination))
        precondition(common_snapshot is None or os.path.exists(common_snapshot))
        parent = ["-p", common_snapshot] if common_snapshot else []
        sender = subprocess.Popen(
            ["btrfs", "send"] + Run._verbose() + parent + [source],
            stdout=subprocess.PIPE)
        receiver = subprocess.Popen(
            ["btrfs", "receive"] + Run._verbose_short() + [destination],
            stdin=sender.stdout)
        sender.stdout.close()
        receiver.communicate()
        if sender.returncode or receiver.returncode or not os.path.exists(destination):
            error_raise(f"copy snapshot {source} to {destination} failed")
        return

    @staticmethod
    def delete_snapshot(path: str) -> None:
        """
        Deletes snapshot from BTRFS volume.
        :param path: Path to the snapshot to delete.
        """
        precondition(path.find(SNAPSHOT_NAME_MIDDLE) > 0)
        precondition(os.path.basename(path).find(SNAPSHOT_NAME_MIDDLE) > 0)
        error = subprocess.call(
            ["btrfs", "subvolume", "delete"] + Run._verbose() + [path])
        if error:
            error_raise(f"btrfs subvolume delete {path} failed")
        return

    @staticmethod
    def rsync(source: str, destination: str, fat_mode: bool) -> None:
        """
        Mirrors (copy and delete) files with rsync to the destination.
        On an error: logs an error message and raises a Runtime exception.
        :param source: Path to mirror.
        :param destination: Path to destination. Path WITH snapshot name.
        :param fat_mode: True -> Use special rsync options for FAT file systems.
        """
        precondition(os.path.exists(source))
        precondition(os.path.exists(destination))
        precondition(destination.find(SNAPSHOT_NAME_MIDDLE) > 0)
        if fat_mode:
            # Do not use --archive on FAT because user and group change
            # will not work. Use a time compare window of a little more than
            # 1 hour (=3600s) to prevent a file copy based on DST changes
            # because FAT stores the local time.
            # Do not use --links use only --safe-links because the rsync
            # can not copy links to outside of the tree in a FAT.
            options = ["--times", "--safe-links", "--recursive", "--modify-window=3700"]
        else:
            options = ["--archive", ]
        options += Run._verbose()
        error = subprocess.call(
            ["rsync", "--one-file-system", "--delete-before"] +
            options +
            [source + "/", destination + "/"])
        if error:
            error_raise(f"rsync {source} to {destination} failed")
        return

    @staticmethod
    def rename(source: str, destination: str) -> None:
        """
        Rename on the file system.
        On an error: logs an error message and raises a Runtime exception.
        :param source: Path name of the directory to rename.
        :param destination: New name of the directory.
        :return:
        """
        precondition(os.path.exists(source))
        precondition(not_empty_str(destination))
        precondition(not os.path.exists(destination))
        try:
            os.rename(source, destination)
            if os.path.exists(source) or not os.path.exists(destination):
                error_raise(f"rename {source} to {destination} failed")
        except OSError as error:
            error_raise(f"rename {source} to {destination} failed with {error}")
        return


###############################################################################


class MountPoints:
    """
    Manages mount points.
    The class is used like a singleton object: The methods are class-methods.
    The members are class-variables.
    """

    # A temporary directory as base directory for the mounting points
    _base = None

    # The current mounted backup volumes.
    # uuid -> (mount_point:str, readonly:bool)
    _mounts: Dict[str, Tuple[str, bool]] = dict()

    @classmethod
    def _create_base(cls) -> None:
        """
        Creates a base directory for all mount points.
        Stores the absolute pathname in _base.
        """
        precondition(cls._base is None)
        # Search base directory for temporary files
        tempdir: str = ""
        if "XDG_RUNTIME_DIR" in os.environ:
            tempdir = os.environ["XDG_RUNTIME_DIR"]
        if not os.path.exists(tempdir):
            tempdir = tempfile.gettempdir()
        precondition(not_empty_str(tempdir))
        cls._base = tempfile.mkdtemp(prefix="ubackup.", dir=tempdir)
        if not cls._base or not os.path.exists(cls._base):
            error_raise("can not create work directory")
        logging.info(f"use temporary work directory {cls._base}")

    @classmethod
    def _ensure_base(cls) -> None:
        """
        Ensures the existing of the base directory.
        Optional creates a base directory.
        """
        if cls._base is None:
            cls._create_base()
        precondition(os.path.exists(cls._base))

    @classmethod
    def mount(cls, uuid: str, mode: str) -> str:
        """
        Mounts a volume.
        :param uuid: UUID of the volume to mount.
        :param mode: "r" Readonly mount,
                     "rw" read-write mount,
                     "r?" read or read-write mount.
        :return: absolute path to the mount point.
        """
        precondition(not_empty_str(uuid))
        cls._ensure_base()
        mount_point: str = ""
        if uuid in cls._mounts:
            mount_point, readonly = cls._mounts[uuid]
            if (mode == "r" and not readonly) or (mode == "rw" and readonly):
                # mount mode does not match, needs a remount
                cls.umount(uuid, forced=True)
                mount_point = ""
            else:
                # short cut: use the existing mount
                pass
        if not mount_point:
            precondition(uuid not in cls._mounts)
            mount_point = tempfile.mkdtemp(prefix="mp.", dir=cls._base)
            if not mount_point or not os.path.exists(mount_point):
                error_raise("can not create mount directory")
            readonly = mode == "r"
            Run.mount(uuid, mount_point, readonly)
            cls._mounts[uuid] = (mount_point, readonly)
        precondition(mount_point and os.path.exists(mount_point))
        return mount_point

    @classmethod
    def umount(cls, uuid: str, forced: bool = False) -> None:
        """
        Umounts a volume.
        Umounts a read-write mounted volume direct.
        An umount of a read-only mounted volume can be deferred if not forced.
        An umount of a not mounted backup volume is ignored.
        A sync will be always executed also if the umount is deferred.
        :param uuid: UUID of the volume to mount.
        :param forced: If true, then the volume is direct umounted also if it
            is only read-only mounted.
        """
        precondition(not_empty_str(uuid))
        logging.info("running sync")
        Run.sync()
        if uuid not in cls._mounts:
            logging.warning(f"umount of not mounted {uuid}")
        else:
            mount_point, readonly = cls._mounts[uuid]
            if forced or not readonly:
                logging.info(f"umount {uuid}")
                Run.umount(mount_point)
                # rmdir removes only an empty directory, it is save to use.
                os.rmdir(mount_point)
                del cls._mounts[uuid]
                precondition(uuid not in cls._mounts)
        return

    @classmethod
    def umount_all(cls) -> None:
        """
        Umounts all volumes.
        Also umounts volumes current used in this script.
        Does not deferred an umount.
        """
        logging.info("umount all ...")
        while cls._mounts.keys():
            # use temporary list because umount delete in the dictionary
            for uuid in list(cls._mounts.keys()):
                cls.umount(uuid, forced=True)
        if cls._base:
            # rmdir removes only an empty directory, it is save to use.
            os.rmdir(cls._base)
            cls._base = None
        return


###############################################################################


def make_snapshot_name(item: str) -> str:
    """
    Builds name of the current snapshot from the volume name.
    :param item: Volume name.
    :return: Snapshot name
    """
    precondition(Config.check_item(item))
    return Config.get_subvolume(item) + SNAPSHOT_NAME_APPENDIX


def list_snapshots(path: str, snapshot_name: str) -> List[str]:
    """
    Lists all snapshots of the same volume.
    :param path: Search the snapshots in this path.
    :param snapshot_name: The name of the snapshot as pattern base.
    :return: List of snapshots path names.
             List could be empty but it will be a list.
    """
    precondition(os.path.exists(path))
    precondition(not_empty_str(snapshot_name))
    pattern = os.path.join(path, snapshot_name.replace(BACKUP_DATE, "*"))
    lst = glob.glob(pattern)
    return lst if lst is not None else list()


def add_snapshot_date(path_list: List[str]) -> List[Tuple[str, datetime.datetime]]:
    """
    Adds to a list of snapshot/backup paths the datetime.
    Accepts a list of path with a file names of volume snapshots. So the last
    10 characters must be a date. This date will be parsed.
    :param path_list: List of snapshot path names.
    :return: List of tuples (path,date).
    """
    path_date_list: List[Tuple[str, datetime.datetime]] = []
    for path in path_list:
        try:
            path_date_list.append((path, datetime.datetime.strptime(path[-10:], "%Y-%m-%d")))
        except ValueError:
            logging.error("ignore file looks like snapshot '{path}'")
    return path_date_list


def get_common_snapshot(path1: str, path2: str, snapshot_name: str) -> str:
    """
    Get a common snapshot of the volume in both paths.
    If more than one snapshot is in both path, returns the newest snapshot.
    :param path1: Search in this path.
    :param path2: Snapshot must also exists in this path.
    :param snapshot_name: The name of the snapshot as pattern base.
    :return: None or a snapshot name (name without path).
    """
    precondition(os.path.exists(path1))
    precondition(os.path.exists(path2))
    precondition(not_empty_str(snapshot_name))
    # List of snapshots with path name
    lst1 = list_snapshots(path1, snapshot_name)
    lst2 = list_snapshots(path2, snapshot_name)
    # Cut the path names
    lst1 = [x.replace(path1 + "/", "", 1) for x in lst1]
    lst2 = [x.replace(path2 + "/", "", 1) for x in lst2]
    # Common names
    lst = [x for x in lst1 if x in lst2]
    return max(lst) if len(lst) > 0 else None


def get_old_snapshot(path: str, snapshot_name: str) -> str:
    """
    Get the snapshot or get the oldest snapshot.
    If the snapshot exists in the path, then the this snapshot is returned.
    If the snapshot is not in the path, then the oldest snapshot is returned.
    If no snapshot is in the path, then the current snapshot is created.
    :param path: Search in this path.
    :param snapshot_name: Name of the snapshot to search first.
    :return: An existing snapshot. The given snapshot or the oldest snapshot.
    """
    precondition(os.path.exists(path))
    precondition(not_empty_str(snapshot_name))
    snapshot_path = os.path.join(path, snapshot_name)
    if not os.path.exists(snapshot_path):
        # search oldest existing snapshot
        paths = list_snapshots(path, snapshot_name)
        if len(paths) > 0:
            snapshot_path = min(paths)
    # Now an old path was found or the snapshot_path does not exist
    if not os.path.exists(snapshot_path):
        # no existing snapshot
        # create one
        os.mkdir(snapshot_path)
    return snapshot_path


def make_snapshot(source: str) -> None:
    """
    Creates a snapshot on the source volume.
    Skips the creation if an up-to-date snapshot exists
    :param source: The name of the source volume.
    """
    precondition(Config.check_source(source))
    snapshot_name = make_snapshot_name(source)
    source_uuid = Config.get_uuid(source)
    # Check: volume must have a BTRFS filesystem
    if not BlockDeviceList.is_btrfs(source_uuid):
        error_raise(f"source volume {source} has no BTRFS")
    # Mount the source to check
    # Needs read access, write access is ok because most a snapshot follows
    mount_point = MountPoints.mount(source_uuid, "r?")
    try:
        snapshot_path = os.path.join(mount_point, snapshot_name)
        if not os.path.exists(snapshot_path):
            # Create snapshot, needs rw mount
            mount_point = MountPoints.mount(source_uuid, "rw")
            source_path = os.path.join(mount_point, Config.get_subvolume(source))
            snapshot_path = os.path.join(mount_point, snapshot_name)
            # Create the readonly snapshot of the BTRFS subvolume
            logging.info(f"create source snapshot {snapshot_name}")
            Run.snapshot(source_path, snapshot_path)
            logging.info(f"source snapshot {snapshot_name} is created")
        else:
            logging.info(f"skip snapshot creation, snapshot {snapshot_name} exists")
    finally:
        MountPoints.umount(source_uuid)
    return


def copy_snapshot(source: str, destination: str) -> None:
    """
    Copy a snapshot to the destination.
    If reference_snapshot is None, then a full copy of the snapshot will be done.
    If reference_snapshot contains the subvolume name of a snapshot, then only
    the increment will be copied to the destination.
    :param source:  Source backup volume name.
    :param destination: Destination backup volume name.
        or name of the subvolume used as reference for incremental copy.
    """
    precondition(Config.check_source(source))
    precondition(Config.check_destination(destination))
    snapshot_name = make_snapshot_name(source)
    source_uuid = Config.get_uuid(source)
    destination_uuid = Config.get_uuid(destination)
    # Check: volumes must have a BTRFS filesystem
    if not BlockDeviceList.is_btrfs(source_uuid):
        error_raise(f"source volume {source} has no BTRFS")
    if not BlockDeviceList.is_btrfs(destination_uuid):
        error_raise(f"destination volume {destination} has no BTRFS")
    mounted_destination = MountPoints.mount(destination_uuid, "rw")
    try:
        destination_path = os.path.join(mounted_destination, snapshot_name)
        if not os.path.exists(destination_path):
            # Copy snapshot
            # Mount the source readonly to ensure no change at the source
            mounted_source = MountPoints.mount(source_uuid, "r")
            try:
                source_path = os.path.join(mounted_source, snapshot_name)
                if not os.path.exists(source_path):
                    error_raise(f"missing source snapshot {snapshot_name}")
                logging.info(f"copy snapshot {snapshot_name}")
                # Incremental copy is possible is a common snapshot exists
                common = get_common_snapshot(mounted_source, mounted_destination, snapshot_name)
                if common:
                    logging.info(f"copy update to {common}")
                    common = os.path.join(mounted_source, common)
                    precondition(os.path.exists(common))
                Run.copy_snapshot(source_path, mounted_destination, common)
                logging.info(f"snapshot {snapshot_name} is copied")
            finally:
                MountPoints.umount(source_uuid)
        else:
            logging.info(f"skip copy snapshot, snapshot {snapshot_name} exists")
    finally:
        MountPoints.umount(destination_uuid)
    return


def copy_files(source: str, destination: str) -> None:
    """
    Copy the files from a snapshot to the destination.
    :param source:  Source backup volume name.
    :param destination: Destination backup volume name.
    """
    precondition(Config.check_source(source))
    precondition(Config.check_destination(destination))
    snapshot_name = make_snapshot_name(source)
    source_uuid = Config.get_uuid(source)
    destination_uuid = Config.get_uuid(destination)
    destination_fat = BlockDeviceList.is_fat(destination_uuid)
    if BlockDeviceList.is_btrfs(destination_uuid):
        logging.warning(f"copy files to BTRFS volume {destination}")
    mounted_destination = MountPoints.mount(destination_uuid, "rw")
    try:
        destination_path = os.path.join(mounted_destination, snapshot_name)
        if not os.path.exists(destination_path):
            # Copy files from snapshot
            # Mount the source readonly to ensure no change at the source
            mounted_source = MountPoints.mount(source_uuid, "r")
            try:
                source_path = os.path.join(mounted_source, snapshot_name)
                if not os.path.exists(source_path):
                    error_raise(f"missing source snapshot {snapshot_name}")
                logging.info(f"copy files from snapshot {snapshot_name}")
                # Search old destination
                old_destination_path = get_old_snapshot(mounted_destination, snapshot_name)
                # Update files in old destination
                logging.info(f"rsync from {source_path} to {old_destination_path}")
                Run.rsync(source_path, old_destination_path, destination_fat)
                # Rename old destination only after successful file copy
                # If it is the first copy to a backup volume, then the old name is the current
                if old_destination_path != destination_path:
                    Run.rename(old_destination_path, destination_path)
                logging.info(f"files {snapshot_name} are copied")
            finally:
                MountPoints.umount(source_uuid)
        else:
            logging.info(f"skip copy files, destination {snapshot_name} exists")
    finally:
        MountPoints.umount(destination_uuid)
    return


def sample_test_files(source_path: str, filter_date: datetime) -> List[str]:
    """
    Samples test files from the source path to check the copy.
    The number of test files is defined in CHECK_FILE_COUNT.
    :param source_path: Samples files from this directory tree.
    :param filter_date: Files must not be changed after this date.
    :return: List of files (full path names) to compare.
    """

    def samples(path: str, level: int, count: int) -> List[str]:
        """
        Samples test files from the path.
        :param path: Samples files from this directory tree
        :param level: Go maximal this number of levels deep into the dir tree.
        :param count: Maximal number of files to sample.
        :return: List of files (full path names).
        """
        precondition(os.path.exists(path))
        precondition(level >= 0)
        result_list = []
        try:
            if level > 0 and count > 3:
                dir_list = []
                with os.scandir(path) as directory:
                    for entry in directory:
                        if entry.is_dir(follow_symlinks=False):
                            dir_list.append((entry.stat().st_mtime, entry.path))
                dir_list.sort()
                # List files from current changed directories first
                for i in range(len(dir_list) - 1, 0, -1):
                    result_list.extend(samples(dir_list[i][1], level - 1, count - len(result_list)))
            if len(result_list) < count:
                file_list = []
                with os.scandir(path) as directory:
                    for entry in directory:
                        if entry.is_file(follow_symlinks=False):
                            m = entry.stat().st_mtime
                            if not filter_date or datetime.datetime.fromtimestamp(m) < filter_date:
                                file_list.append((m, entry.path))
                # Take the 3 most current files from each directory
                file_list.sort()
                for i in range(len(file_list) - 1, max(0, len(file_list) - 4), -1):
                    result_list.append(file_list[i][1])
        except PermissionError:
            # ignore missing access rights
            pass
        except Exception as error:
            # log error and continue
            # Continue because this is only a check. It is better to continue the backup and
            # log the error then to stop the backup on an error during file checking.
            logging.error(f"unhandled exception during file check: {error}")
        return result_list

    precondition(os.path.exists(source_path))
    precondition(CHECK_FILE_COUNT > 0)
    return samples(source_path, 10, CHECK_FILE_COUNT)


def compare_files(file_list: List[Tuple[str, str]]) -> None:
    """
    Compares the content of the file pairs.
    Writes errors to the log if the file pairs are not equal.
    :param file_list: List of file name pairs.
    """
    files_ok = 0
    files_bad = 0
    for file_a, file_b in file_list:
        if filecmp.cmp(file_a, file_b, shallow=False):
            files_ok += 1
        else:
            files_bad += 1
            logging.error(f"differing backup file found: '{file_a}' <> '{file_b}'")
    logging.info(f"backup files compared: {files_ok} are ok, {files_bad} are different")
    if files_bad > 0:
        # log the import number of different backup files also as error message
        logging.error(f"found {files_bad} differing backup files")


def check_copied_files(source: str, destination: str = None) -> None:
    """
    Check the files copied to the destination.
    Checks only some sample files and sample directories.
    :param source: Source volume name.
    :param destination: Destination volume name.
        Or None if a snapshot on the source value should be checked.
    """
    precondition(Config.check_source(source))
    source_mount_point = None
    destination_mount_point = None
    try:
        # source is always given by the function parameter:
        source_uuid = Config.get_uuid(source)
        snapshot_name = make_snapshot_name(source)
        source_mount_point = MountPoints.mount(source_uuid, "r")
        # destination could be given by the function parameter or could be
        # implicit given:
        if destination:
            # compare: snapshot of the source - backup of the snapshot
            precondition(Config.check_destination(destination))
            # mount the destination volume
            destination_uuid = Config.get_uuid(destination)
            destination_mount_point = MountPoints.mount(destination_uuid, "r")
            # destination and source are snapshots with the same name
            destination_path = os.path.join(destination_mount_point, snapshot_name)
            source_path = os.path.join(source_mount_point, snapshot_name)
            # The both snapshots must be unchanged, so no time filter is needed
            filter_date = None
        else:
            # compare: source data - snapshot of the source data
            # Both are on the source volume. So both are mounted before.
            destination_path = os.path.join(source_mount_point, snapshot_name)
            source_path = os.path.join(source_mount_point, Config.get_subvolume(source))
            # The source could be changed after the snapshot creation.
            # So compare only files unchanged at the day of the snapshot.
            filter_date = datetime.datetime.strptime(BACKUP_DATE, "%Y-%M-%d") - \
                          datetime.timedelta(days=1)
        # Collect a list of files for the compare. Only a small subset of the
        # files will be compared.
        source_file_list = sample_test_files(source_path, filter_date)
        logging.info(f"compare {len(source_file_list)} files to check the backup")
        compare_files(
            [(name, name.replace(source_path, destination_path, 1)) for name in source_file_list])
        logging.info("all files compared")
    finally:
        if source_mount_point:
            MountPoints.umount(source_uuid)
        if destination_mount_point:
            MountPoints.umount(destination_uuid)
    return


def backup_to(item: str) -> None:
    """
    Creates a backup or snapshot on the given item.
    :param item: Name of the backup volume to use.
    """
    precondition(Config.check_item(item))
    if Config.is_destination(item):
        # make a snapshot on source and copy to destination
        source = Config.get_source(item)
        destination = item
    else:
        # only make a snapshot on source
        source = item
        destination = None
    if Config.check_source(source):
        logging.info(f"snapshot {source}")
        make_snapshot(source)
        check_copied_files(source)
        Config.set_last_snapshot(source, BACKUP_DATE)
        if destination and Config.check_destination(destination):
            logging.info(f"backup from {source} to {destination}")
            if BlockDeviceList.is_btrfs(Config.get_uuid(destination)):
                # destination is also BTRFS: send the snapshot
                copy_snapshot(source, destination)
            else:
                # Copy the files from the snapshot
                copy_files(source, destination)
            check_copied_files(source, destination)
            Config.set_last_snapshot(destination, BACKUP_DATE)
    return


def thin_away(item: str) -> None:
    """
    Thins away old backups/snapshots.
    This function works on BTRFS volumes only.
    :param item: Name of the backup volume to use.
    """
    precondition(Config.check_item(item))
    uuid = Config.get_uuid(item)
    precondition(BlockDeviceList.is_btrfs(uuid))
    path = MountPoints.mount(uuid, "rw")
    keep = Config.get_keep(item)
    try:
        # create sorted list of backups/snapshots of the volume.
        to_analyse = list_snapshots(path, make_snapshot_name(item))
        number_backups = len(to_analyse)
        to_analyse = add_snapshot_date(to_analyse)
        to_analyse.sort(key=lambda x: x[1])
        to_delete = []
        # keep backups with distances of day, month, year
        for count, distance in ((keep["day"], datetime.timedelta(days=1)),
                                (keep["month"], datetime.timedelta(days=30)),
                                (keep["year"], datetime.timedelta(days=365))):
            # keep in the next 'count' time periods 'distance' on backup
            if not to_analyse and to_delete:
                # No backups i the regular time distance.
                # So reset and give the latest a new chance.
                to_analyse = to_delete
                to_analyse.sort(key=lambda x: x[1])
                to_delete = []
            precondition(count >= 0)
            precondition(distance >= datetime.timedelta(days=1))
            precondition(all(a[1] <= b[1] for a, b in zip(to_analyse, to_analyse[1:])))
            while count > 0 and to_analyse:
                # keep the last backup in the time period
                path, latest = to_analyse.pop()
                count -= 1
                # delete all previous backups in the time period
                while to_analyse and to_analyse[-1][1] > latest - distance:
                    to_delete.append(to_analyse.pop())
        # The rest of the list is to old
        to_delete += to_analyse
        # keep all backups which are the latest backup of a volume
        # keep these to allow incremental copy to the destinations.
        others = Config.list_last_snapshots(Config.get_subvolume(item))
        precondition(len(others) > 0)
        to_delete = [item for item in to_delete if item[0][-10:] not in others]
        # check the delete list
        max_delete = max(0, number_backups - max(1, keep["day"]))
        if len(to_delete) > max_delete:
            error_raise("the delete list is to long, program bug error")
        # execute the deletes
        for path, date in to_delete:
            precondition(path.find(SNAPSHOT_NAME_MIDDLE) > 0)
            precondition(os.path.basename(path).find(SNAPSHOT_NAME_MIDDLE) > 0)
            logging.info(f"delete old snapshot {os.path.basename(path)}")
            Run.delete_snapshot(path)
    finally:
        MountPoints.umount(uuid)
    return


def collect_all() -> List[str]:
    """
    Collect names of all connected volumes.
    The returned could be empty.
    :return: List of all sources/destinations available now.
    """
    connected = BlockDeviceList.list_uuid()
    names = Config.list_name(connected)
    logging.info(f"connected are: {', '.join(names)}")
    return names


def main(description: str = None) -> int:
    """
    Backup BTRFS subvolumes.
    :param description: Description of the program.
    :return: 0 = success, 1 = error
    """
    # Set result code to error at start and later on success reset
    result = 1
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Main argument is a list of backup destinations.
    # The backup sources will be found in the configuration file.
    parser.add_argument(
        "destinations",
        metavar="DESTINATION",
        type=str,
        nargs="*",
        help="Backup to this destination, or only snapshot this source")
    parser.add_argument(
        "--conf", "-C",
        metavar="CONFIG",
        type=argparse.FileType(),
        nargs="?",
        help="configuration file name",
        default=Config.get_default())
    parser.add_argument(
        "--verbose", "-v",
        action="count",
        help="Verbose 1: logging info messages, 2: verbose to called tools")
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Snapshot all online sources, backup to all online destinations")
    arguments = parser.parse_args()
    # Set verbose level before loading the configuration
    Config.set_verbose(arguments.verbose)
    # noinspection PyPep8,PyBroadException
    try:
        if os.geteuid() != 0:
            error_raise("Backup mounts the volumes. So it must run as root.")
        Config.load(arguments.conf)
        logging.info(f"Loaded configuration, ubackup {BACKUP_VERSION} runs")
        # Set verbose level again to optionally overwrite the config file
        Config.set_verbose(arguments.verbose)
        job_list = arguments.destinations if not arguments.all else collect_all()
        logging.info(f"creating backups with date {BACKUP_DATE}")
        if not job_list:
            if arguments.all:
                logging.warning("no configured volumes are online")
            else:
                error_raise("no backup destinations given")
        for destination in job_list:
            if Config.check_item(destination):
                backup_to(destination)
            else:
                logging.error(f"invalid backup destination {destination}")
        # The last snapshots are stored in the configuration file.
        Config.update(arguments.conf)
        # Thin away old backups on BTRFS volumes
        # (On other volumes the number of backups is fixed: simply the oldest
        # backup will be replaced during the backup process.)
        for destination in job_list:
            if Config.check_item(destination):
                if BlockDeviceList.is_btrfs(Config.get_uuid(destination)):
                    thin_away(destination)
        # program end without errors:
        result = 0
    except RuntimeError as error:
        logging.exception(f"ABORT ON ERROR: {error}")
    except:
        logging.exception("unhandled exception")
    finally:
        # Clean up
        MountPoints.umount_all()
    return result


if __name__ == "__main__":
    # The file doc string is the description of the program
    status: int = main(__doc__)
    sys.exit(status)
