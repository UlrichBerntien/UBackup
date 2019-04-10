#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backup BTRFS subvolumes

The script makes backups of a subvolume on a BTRFS volume.
The snapshots could be copied to other BTRFS volumes, FAT filesystem or other
filesystem. A history of old snapshots is stored on BTRFS volumes only.

The snapshots will be named "NAME-Snapshot-YYYY-MM-DD" where NAME is the name
of the subvolume, e.g. @Data, and YYYY-MM-DD is the creation date.

On the source volume each backup starts with a snapshot. Only one snapshot per
day will be created. A second call of the script at the same day will reuse the
existing snapshot on the source volume.

To BTRFS volumes a send - receive of snapshots is done.

To other volumes a rsync is done. To FAT volumes the simple "--archive" flag
is not used because user information could not stored on FAT volumes. To all
other filesystems a "rsync --archive" call is used.

The default configuration is ubackup.conf.json located  beside the script file,
in the current work directory or in ~/.config.

The configuration file has json format.
The configuration is one dictionary. The key is the name of the volume, the
value is a dictionary with details to the volume. The details are:
- subvolume: Name of the subvolume. Name without "-Snapshot-YYYY-MM-DD".
- uuid: UUID of the volume used to mount the volume.
- type: "source" or "destination"
- last-snapshot: null or the date of the last stored snapshot.
- comment: Comment text to the subvolume
"""

#########################################################################################
#
# Author: Ulrich Berntien
# Date: 2019-03-26
# Language: Python 3.6.7
#
# MIT License
# Copyright (c) 2019 Ulrich Berntien
#
#########################################################################################


import argparse
import datetime
import glob
import json
import os
import subprocess
import sys
import tempfile
from typing import *

#########################################################################################


# Default configuration file name.
# Default location is beside the script.
CONFIG_FILE = "ubackup.config.json"

# The date of the backup is a constant during running the backup script.
# The date at the script start is used.
# Because the snapshots containing the date only one snapshot per day is
# possible with this tool. More snapshots are possible if e.g. the hour
# is added to the backup date stamp.
BACKUP_DATE = datetime.date.today().isoformat()

# The name part of all snapshot subvolumes.
SNAPSHOT_NAME_MIDDLE = "-Snapshot-"

# The name of the snapshots and the copied snapshots on the backup volumes
# is "subvolume" + SNAPSHOT_APPENDIX.
SNAPSHOT_NAME_APPENDIX = SNAPSHOT_NAME_MIDDLE + BACKUP_DATE


#########################################################################################


def precondition(condition: bool) -> None:
    """
    Raises a exception on condition is false.
    Bugs in a backup program are fatal. Hence not the assert is used but this
    strict exception throwing function is used.
    If a condition is False, then a program bug must exists causing this failure.
    Only program errors are handled by the precondition calls. Configuration errors,
    user errors, etc. are handled soft.
    :param condition: True if the program works correct.
    """
    if not condition:
        raise RuntimeError('Precondition failed. Program bug.')


def not_empty_str(string: str) -> bool:
    """
    Checks string.
    :param string: Check this string.
    :return: True if and only if string is a not-empty string.
    """
    return isinstance(string, str) and string


#########################################################################################


class Logging:
    """
    Processing all text output to the user.
    The class is used like a singleton object: The methods are class-methods.
    The members are class-variables.
    """
    # TODO use Python Logging & add cmdline options: log to stdout or file

    # Flag indicating verbose output
    _verbose = False

    @classmethod
    def set_verbose(cls, verbose: bool) -> None:
        """
        Sets the verbose level for all output.
        :param verbose: True for verbose output, else False.
        """
        cls._verbose = verbose

    @classmethod
    def error(cls, message: str) -> None:
        """
        Prints error message.
        :param message: Error message to print.
        """
        # No check of the message in the error logging.
        # A raise in the error logging should be suppressed,
        print("[X] " + message)

    @classmethod
    def error_raise(cls, message: str) -> None:
        """
        Prints error message and raise RunTimeException.
        :param message: Error message to print.
        """
        # No check of the message in the error logging.
        # A raise in the error logging should be suppressed,
        cls.error(message)
        raise RuntimeError(message)

    @classmethod
    def warning(cls, message: str) -> None:
        """
        Prints warning message.
        :param message: Warning message to print.
        """
        precondition(not_empty_str(message))
        print("[!] " + message)

    @classmethod
    def info(cls, message: str) -> None:
        """
        Prints info message but only in verbose mode.
        :param message: Info message to print.
        """
        precondition(not_empty_str(message))
        if cls._verbose:
            print("[i] " + message)


#########################################################################################


class Config:
    """
    Handling the configuration file.
    The class is used like a singleton object: The methods are class-methods.
    The members are class-variables.
    """

    # The configuration in a dict.
    # This dict is read from and written to the configuration file.
    _raw_config: dict = None

    # Status of the configuration data in _raw_config.
    # 0 = no config, 1 = config loaded, 2 = config changed
    _status: int = 0

    # Verbose level
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
        return ''

    @classmethod
    def set_verbose(cls, level: int) -> None:
        """
        Stores the verbose level.
            0: no info message output
            1: info message output from this script
            2: level 1 + verbose pass through to all called tools
        :param level: 0,1 or 2.
        """
        precondition(0 <= level <= 3)
        cls._verbose = level
        Logging.set_verbose(level > 0)

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
        if name not in cls._raw_config:
            Logging.error(f"item {name} not defined.")
            return False
        cfg = cls._raw_config[name]
        if "type" not in cfg or cfg["type"] not in ('destination', 'source'):
            Logging.error(f"item {name} has no valid type")
            return False
        if "uuid" not in cfg or not cfg["uuid"]:
            Logging.error(f"item {name} has no uuid")
            return False
        if "subvolume" not in cfg:
            Logging.error(f"item {name} contains no subvolume name")
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
        cfg = cls._raw_config[name]
        if cfg["type"] != "destination":
            Logging.error(f"backup destination {name} has not type destination")
            return False
        subvolume = cls._raw_config[name]["subvolume"]
        number_sources = 0
        for value in cls._raw_config.values():
            if value["subvolume"] == subvolume and value["type"] == "source":
                number_sources += 1
        if number_sources == 0:
            Logging.error(f"no source defined for backup destination {name}")
            return False
        if number_sources > 1:
            Logging.error(f"more than one source defined for backup destination {name}")
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
        cfg = cls._raw_config[name]
        if cfg["type"] != "source":
            Logging.error(f"backup source {name} has not type source")
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
        precondition(cls._status > 0)
        return cls._raw_config[name]["type"] == "destination"

    @classmethod
    def get_source(cls, destination: str) -> str:
        """
        Gets the name of the backup source volume.
        :param destination:  The name of the destination volume.
        :return: Name of the source volume.
        """
        precondition(cls.check_destination(destination))
        precondition(cls._status > 0)
        subvolume = cls._raw_config[destination]["subvolume"]
        for key, value in cls._raw_config.items():
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
        precondition(cls._status > 0)
        return cls._raw_config[name]["subvolume"]

    @classmethod
    def get_uuid(cls, name: str) -> str:
        """
        Gets the uuid of the backup volume.
        :param name: The name of the source or destination volume.
        :return: The uuid.
        """
        precondition(cls.check_item(name))
        precondition(cls._status > 0)
        return cls._raw_config[name]["uuid"]

    @classmethod
    def set_last_snapshot(cls, name: str, date: str) -> None:
        """
        Set the date of the last snapshot.
        :param name: Name of the backup volume.
        :param date: The date of the snapshot.
        """
        precondition(cls.check_item(name))
        precondition(not_empty_str(date))
        precondition(cls._status > 0)
        if "last-snapshot" in cls._raw_config[name].keys():
            old_value = cls._raw_config[name]["last-snapshot"]
        else:
            old_value = None
        if old_value != date:
            cls._raw_config[name]["last-snapshot"] = date
            cls._status = 2

    @classmethod
    def load(cls, file) -> None:
        """
        Load the backup configuration file.
        :param file: The open configuration file.
        :return: The loaded configuration.
        """
        precondition(file.readable())
        Logging.info(f"load configuration {file.name}")
        if cls._status == 1:
            Logging.info("Reload configuration")
        if cls._status == 2:
            Logging.warning("Reload configuration, revert changes")
        # Reset the status
        cls._status = 0
        try:
            cls._raw_config = json.load(file)
            cls._status = 1
        except json.JSONDecodeError as error:
            Logging.error(f"Configuration file error {error}")
            sys.exit(1)

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
                Logging.info("Reopen configuration file for write")
                file.close()
                file = open(file.name, 'wt')
            Logging.info("write configuration file")
            json.dump(cls._raw_config, file, indent=4, sort_keys=True)


#########################################################################################


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
            Logging.error_raise("lsblk failed")
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
            Logging.error_raise("lsblk json structure unknown")
        for item in lsblk["blockdevices"]:
            if item["uuid"] == uuid:
                return item[entry]
        Logging.error_raise(f"volume {uuid} is not present")

    @staticmethod
    def is_filesystem_fat(uuid: str) -> bool:
        """
        Check if the volume has a FAT file system.
        :param uuid: The uuid of the volume.
        :return: True if and only if the volume has a FAT file system.
        """
        precondition(not_empty_str(uuid))
        info = BlockDeviceList._lsblk()
        return BlockDeviceList._get(info, uuid, "fstype") in ("exfat", "msdos", "vfat")

    @staticmethod
    def is_filesystem_btrfs(uuid: str) -> bool:
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


#########################################################################################


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
        Logging.info(f"mount {uuid}{' as readonly' if readonly else ''}")
        options = f"noatime,nodev,lazytime"
        if readonly:
            options += ",ro"
        current_mount_point = BlockDeviceList.mount_point(uuid)
        is_btrfs = BlockDeviceList.is_filesystem_btrfs(uuid)
        if current_mount_point and not is_btrfs:
            options += ",bind"
            source = ["--bind", current_mount_point]
        else:
            source = ["--uuid", uuid]
        error = subprocess.call(
            ["mount"] + Run._verbose() + ["--no-mtab", "--options", options,
                                          "--target", mount_point] + source)
        if error:
            Logging.error_raise(f"mount of {uuid} failed")
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
            Logging.error_raise(f"umount failed")
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
            Logging.error_raise(f"make snapshot {source} to {destination} failed")
        return

    @staticmethod
    def copy_snapshot(source: str, destination: str, common_snapshot) -> None:
        """
        Copy snapshot from BTRFS volume to other BTRFS volume.
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
            Logging.error_raise(f"copy snapshot {source} to {destination} failed")
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
            options = ["--times", "--links", "--recursive", "--modify-window=3700"]
        else:
            options = ["--archive", ]
        options += Run._verbose()
        error = subprocess.call(
            ["rsync", "--one-file-system", "--delete-before"] +
            options +
            [source + "/", destination + "/"])
        if error:
            Logging.error_raise(f"rsync {source} to {destination} failed")
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
                Logging.error_raise(f"rename {source} to {destination} failed")
        except OSError as error:
            Logging.error_raise(f"rename {source} to {destination} failed with {error}")
        return


#########################################################################################


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
            Logging.error_raise("can not create work directory")
        Logging.info(f"use temporary work directory {cls._base}")

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
        :param mode: "r" Readonly mount, "rw" read-write mount, "r?" read or read-write mount.
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
            else:
                # short cut: use the existing mount
                pass
        if not mount_point:
            precondition(uuid not in cls._mounts)
            mount_point = tempfile.mkdtemp(prefix="mp.", dir=cls._base)
            if not mount_point or not os.path.exists(mount_point):
                Logging.error_raise("can not create mount directory")
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
        :param uuid: UUID of the volume to mount.
        :param forced: If true, then the volume is direct umounted also if it
            is only read-only mounted.
        """
        precondition(not_empty_str(uuid))
        if uuid not in cls._mounts:
            Logging.warning(f"umount of not mounted {uuid}")
        else:
            mount_point, readonly = cls._mounts[uuid]
            if forced or not readonly:
                Logging.info(f"umount {uuid}")
                Run.umount(mount_point)
                # rmdir removes only an empty directory, it is save to use.
                os.rmdir(mount_point)
                del cls._mounts[uuid]
        return

    @classmethod
    def umount_all(cls) -> None:
        """
        Umounts all volumes.
        Also umounts volumes current used in this script.
        Does not deferred an umount.
        """
        Logging.info("umount all ...")
        while cls._mounts.keys():
            # use temporary list because umount delete in the dictionary
            for uuid in list(cls._mounts.keys()):
                cls.umount(uuid, forced=True)
        if cls._base:
            # rmdir removes only an empty directory, it is save to use.
            os.rmdir(cls._base)
            cls._base = None
        return


#########################################################################################


def make_snapshot_name(source: str) -> str:
    """
    Builds name of the current snapshot from the source volume name.
    :param source: Source volume name.
    :return: Snapshot name
    """
    precondition(Config.check_source(source))
    return Config.get_subvolume(source) + SNAPSHOT_NAME_APPENDIX


def list_snapshots(path: str, snapshot_name: str) -> List[str]:
    """
    Lists all snapshots with other date stamps.
    :param path: Search the snapshots in this path.
    :param snapshot_name: The name of the snapshot as pattern base.
    :return: List of snapshots path names. List could be empty but it will be a list.
    """
    precondition(os.path.exists(path))
    precondition(not_empty_str(snapshot_name))
    pattern = os.path.join(path, snapshot_name.replace(BACKUP_DATE, "*"))
    lst = glob.glob(pattern)
    return lst if lst is not None else list()


def get_common_snapshot(path1: str, path2: str, snapshot_name: str) -> str:
    """
    Get a common snapshot in both paths.
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
    if not BlockDeviceList.is_filesystem_btrfs(source_uuid):
        Logging.error_raise(f"source volume {source} has no BTRFS")
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
            Logging.info(f"create source snapshot {snapshot_name}")
            Run.snapshot(source_path, snapshot_path)
            Logging.info(f"source snapshot {snapshot_name} is created")
        else:
            Logging.info(f"skip snapshot creation, snapshot {snapshot_name} exists")
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
    if not BlockDeviceList.is_filesystem_btrfs(source_uuid):
        Logging.error_raise(f"source volume {source} has no BTRFS")
    if not BlockDeviceList.is_filesystem_btrfs(destination_uuid):
        Logging.error_raise(f"destination volume {destination} has no BTRFS")
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
                    Logging.error_raise(f"missing source snapshot {snapshot_name}")
                Logging.info(f"copy snapshot {snapshot_name}")
                # Incremental copy is possible is a common snapshot exists
                common = get_common_snapshot(mounted_source, mounted_destination, snapshot_name)
                if common:
                    Logging.info(f"copy update to {common}")
                    common = os.path.join(mounted_source, common)
                    precondition(os.path.exists(common))
                Run.copy_snapshot(source_path, mounted_destination, common)
                Logging.info(f"snapshot {snapshot_name} is copied")
            finally:
                MountPoints.umount(source_uuid)
        else:
            Logging.info(f"skip copy snapshot, snapshot {snapshot_name} exists")
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
    destination_fat = BlockDeviceList.is_filesystem_fat(destination_uuid)
    if BlockDeviceList.is_filesystem_btrfs(destination_uuid):
        Logging.warning(f"copy files to BTRFS volume {destination}")
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
                    Logging.error_raise(f"missing source snapshot {snapshot_name}")
                Logging.info(f"copy files from snapshot {snapshot_name}")
                # Search old destination
                old_destination_path = get_old_snapshot(mounted_destination, snapshot_name)
                # Update files in old destination
                Logging.info(f"rsync from {source_path} to {old_destination_path}")
                Run.rsync(source_path, old_destination_path, destination_fat)
                # Rename old destination only after successful file copy
                # If it is the first copy to a backup volume, then the old name is the current
                if old_destination_path != destination_path:
                    Run.rename(old_destination_path, destination_path)
                Logging.info(f"files {snapshot_name} are copied")
            finally:
                MountPoints.umount(source_uuid)
        else:
            Logging.info(f"skip copy files, destination {snapshot_name} exists")
    finally:
        MountPoints.umount(destination_uuid)
    return


def check_copied_files(source: str, destination: str = None) -> None:
    """
    Check the files copied to the destination.
    Checks only some sample files and sample directories.
    :param source: Source volume name.
    :param destination: Destination volume name.
        Or None if a snapshot on the source value should be checked.
    """
    precondition(Config.check_source(source))
    if destination is not None:
        precondition(Config.check_destination(destination))
    # TODO, implement the function


def backup_to(item: str) -> None:
    """
    Create a backup or snapshot on the given item.
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
        Logging.info(f"snapshot {source}")
        make_snapshot(source)
        check_copied_files(source)
        Config.set_last_snapshot(source, BACKUP_DATE)
        if destination and Config.check_destination(destination):
            Logging.info(f"backup from {source} to {destination}")
            if BlockDeviceList.is_filesystem_btrfs(Config.get_uuid(destination)):
                # destination is also BTRFS: send the snapshot
                copy_snapshot(source, destination)
            else:
                # Copy the files from the snapshot
                copy_files(source, destination)
            check_copied_files(source, destination)
            Config.set_last_snapshot(destination, BACKUP_DATE)
    return


def main(description: str = None):
    """
    Backup BTRFS subvolumes.
    :param description: Description of the program.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Main argument is a list of backup destinations.
    # The backup sources will be found in the configuration file.
    parser.add_argument(
        'destinations',
        metavar='DESTINATION',
        type=str,
        nargs='+',
        help='Backup to this destination, or only snapshot this source')
    parser.add_argument(
        '--conf', '-C',
        metavar='CONFIG',
        type=argparse.FileType(),
        nargs='?',
        help='configuration file name',
        default=Config.get_default())
    parser.add_argument(
        '--verbose', '-v',
        action='count',
        help='Verbose 1: logging info messages, 2: verbose to called tools')
    arguments = parser.parse_args()
    Config.set_verbose(arguments.verbose)
    try:
        Config.load(arguments.conf)
        Logging.info(f"creating backups with date {BACKUP_DATE}")
        if os.geteuid() != 0:
            Logging.error_raise("Backup mounts the volumes. So it must run as root.")
        for destination in arguments.destinations:
            if Config.check_item(destination):
                backup_to(destination)
            else:
                Logging.error(f"invalid backup destination {destination}")
        # The last snapshots are stored in the configuration file.
        Config.update(arguments.conf)
        # TODO, thin away old backups/snapshots on all destinations and all referenced sources
    except RuntimeError as error:
        print(f"ABORT ON ERROR: {error}")
    finally:
        # Clean up
        MountPoints.umount_all()
    return


if __name__ == '__main__':
    # The file doc string is the description of the program
    main(__doc__)
    # TODO: set return codes
