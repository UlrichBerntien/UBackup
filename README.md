# UBackup

Backup with BTRFS Snapshots


## Main points

- Create BTRFS snapshot on a source volume
- Create maximal 1 snapshot per day
- Copy snapshot to other BTRFS volume, uses automatically incremental copy
- Copy snapshot via rsync to other filesystem volumes, e.g. exfat
- Configuration in json file
- Identification of the source and destination volumes by UUIDs
- Automatic decryption with cryptsetup tool  
- Command line tool


## Design ideas

The backup is designed for volumes with BTRFS.
Creating snapshots on a source drive and incremental copying to the
destination drives is supported by the BTRFS.
The backup tools calls BTRFS commands and does a few bookkeeping.

If the BTRFS file system crashes and pull the copied snapshot also down,
to rescue the data a copy to other file systems via rsync is built-in.

Special switches for FAT file systems are built-in.
The simple "rsync --archive" does not work because user and group change will not work.
On FAT DST switching is handled by ignoring file changes with only 1 hour time distance.
This is necessary because FAT stores the local time.

BTRFS is used because it provides snapshots and
there is no much RAM needed during operation.
XFS or ext4 do not create snapshots.
ZFS supports snapshots but needs a lot of RAM.

On my storage many files are only stored for reading.
Theses files will be created and never or very spare update.
In snapshots, these unchanged files do not occupy extra space.
So often snapshots could be created,
also on a laptop with according limited storage drive capacity.

A version system (git) is used independently for files,
where the file history is needed and used regular,
e.g.  files in software development.
The old snapshots will be used in emergency situations only,
typical an unintended deleted file.


### System

The backup script is designed for a system with a laptop / PC as a workplace
and two external hard disks for data backup.
The backup system works for the backup of a laptop,
should also be usable for the backup of a small server.

Two external HDDs are used to store the drives on two different places.
Also the situation of a crash, e.g. electrical high voltage pulse through
the power line, during the backup run destroying the
laptop and the connected HDD is covered by two external HDDs.

The main backup is stored on the external hard drives,
only connected during the backup.
Undesired deletion of files or intentional attacks to the laptop / PC
can not reach the separated storage drives.

As an additional backup line, snapshots remain on the laptop / PC.
These backups can be accessed quickly if files are corrupted or deleted.
The snapshots on the laptop / PC are normally not mounted
and not visible to the normal user.
Only the root user can mount the snapshots.
The local stored snapshots are a protection for some cases.
For the bigger problems, e.g. a destroyed laptop in a crash,
the external storage drives hold all data save stored in a closed locker.

The backup system should also be suitable for automatic backup in a small server.
The server works on a SSD and a HDD sleeps inside the server for daily automatic
backups.
The internal HDD is always connected.
The HDD is only used once a day for the backup,
afterwards the HDD goes back into sleep mode.
Another two HDD are external and only connected during the backup once per month.
(If needed once per week or once per day a backup to the external HDD is possible.)


### Configuration

The configuration file is created and changed with a simple text editor (e.g. vi).

Source volume(s) and destination volume(s) are configured.
Most volumes are identified by their UUID.
All subvolumes on the volumes are identified by name.

You can specify multiple source subvolumes.
For each source subvolume, multiple destination (backup) subvolumes can be specified.
The same subvolume name links the source and destination subvolumes.
The snapshots are stored on source and destination volumes in subvolumes with
the name "source-subvolume-name plus creation-date".

On a volume could be multiple subvolumes.
Multiple source and multiple destination subvolumes on one volume are possible.
But typical source and destination subvolumes are on different volumes.

The destination volume (and also the source volumes) could be encrypted.
A decryption of the volumes with the cryptsetup tool is implemented in the script
and controlled by the configuration file.
Current is the password input handled by the prompt of the cryptsetup tool.

The cryptsetup supports Veracrypt encrypted volumes.
With a backup to Veracrypt volume + exfat filesystem the backup
could be read on MS Windows systems.

Because the identification of encrypted partitions is complicated
(e.g. UUID of Veracrypt encrypted volumes is not available ),
the drive is identified by the combination of device vendor + device serial number.

The decryption of the configured volumes starts automatic after script start.
The solution is designe for external USB drives only connected during backup.

The configuration file also contains the logging configuration.
The log file is a simple text file.
Also logging to console is possible. Verbose output is supported for debugging.


### Typical usage

After configuration, only one command is required:

```sh
sudo ubackup.py -a
```

This will create snapshots of all connected source subvolumes
and will backup the snapshots to all connected destination subvolumes.
Old backups may be automatically deleted.
The number of backups to keep is given by the configuration.

The switch -a or --all activates all connected volumes.
The volumes could but need not mounted before the backup tool started.
If the volumes are listed by the `lsblk -f` the backup tool will mount and
unmount the volumes automatic.


#### Typical usage of the backup tool on a laptop:

Enter `sudo ubackup.py -a` once a week.
On the internal SSD will be a snapshot created from the current data subvolume.
The backup of the data subvolume on the internal SSD
helps in case of accidental deletion or modification of data files.

Once a month:
connect the laptop with an external HDD and enter `sudo ubackup.py -a`.
A snapshot of the data subvolume will be created on the internal SSD.
The BTRFS snapshot will be incrementally copied to the external HDD.
Rsync will update a copy in the FAT file system on the external HDD.
This will rescue if the internal SSD is broken.
Also a rescue is possible if the BTRFS is damaged on source and snapshot.


#### Typical usage of the backup tool on a small server/PC:

At night (early morning) `sudo ubackup.py -a` is called automatically via cron.
On the internal SSD the current data subvolume will be saved in a snapshot.
The internal HDD will be woken up,
the snapshot will be incremental copied to the internal HDD,
the internal HDD will sleep again.
The old snapshot on the SSD will be automatically deleted,
if it is not is needed for incremental copying to the external HDD.
Snapshots on the internal SSD needed for incremental copy
to a destination volume will never be deleted.

Once a week or once a month: Connect one of the two external HDDs
and call `sudo ubackup.py -a`.
If a snapshot has not been taken that day,
the backup script will create a snapshot on the internal SSD of the data subvolume.
The snapshot will be incremental copied to the external HDD
(and also to the internal HDD if a new snapshot was created).
The script will call `rsync` to update a copy of the data subvolume on
a volume with FAT file system on the external HDD.
Older backups may automatically be deleted as configured.
