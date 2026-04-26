# Alpine Linux einrichten

Alpine Linux einrichten fuer durchfuehren von Restic Backups.
Getrenntes Linux System fuer das Backup, damit Schadsoftware
auf dem Arbeits-PC keinen direkten Einfluss auf das Backup hat.

Alpine Linux verwenden, weil dieses gut von einem USB-Flashspeicher starten kann.

Ein System mit wenigen notwendigen Paketen einrichten fuer das Backup.
Wenige Pakete fuehren zu kleiner Angriffsflaeche.
Wenige Pakete fuehren zu schnellen Updates.

# Installieren

[Alpine Linux Download Page](https://alpinelinux.org/downloads/)

Das Standard ISO-Image laden fuer x86 System.

Installation laufen lassen (z.B. in virtueller Maschine).

```
setup-alpine
```

- sys (system disk mode) Installation ausfuehren.
- OpenSSH Client und Server.
- Downloads ueber Proxy in Europa.
- Community Repository aktivieren.

Standard Benutzer und Benutzergruppen einrichten.

SSH Login nur ueber Schluessel.
Kein SSH Login fuer root, kein SSH Login ueber Kennwort.


# Notwendinge Pakete installieren

Aktualisieren der Paket-Metadaten und aktualisieren der installierten Pakete.

```
apk update
apk upgrade
```

Auch die Dokumentation installieren, damit Probleme ggf. offline geloest werden koennen.
Das `docs` Metapaket installieren, damit werden alle Man-Pages zu den installierten
Paketen automatisch auch installiert.

Fuer Arbeit auf dem Terminal (tmux) und die Ausfuehrung der Skripte (dash).
Bei Alpine Linux wird die dash unter `/usr/bin/dash` installiert.

Unter `/bin/sh` ist immer die ash aus der Busybox.
Das Backup und Copy Skript koennen von ash Implementierung der Busybox ausgefuehrt werden.

Fuer die Suche nach vorhandenen Blockgeraeten/Volumes `lsblk` installieren.
Auch das `mount` Tool installieren.

In der Alpine Standard-Installation ist der `mount` Befehl in der `busybox` aktiv.
Dieser `mount` Befehl innerhalb der `busybox` kann keine BTRFS mounten.

```
apk add docs
apk add tmux dash lsblk mount
```


Fuer Arbeit mit den verwendeten Datei-Systemen (btrfs, xfs, ext4) und Verschluesselungen (LUKS,gocryptfs):

```
apk add btrfs-progs xfsprogs e2fsprogs
apk add cryptsetup gocryptfs 
```

Fuer Restic-Backup und Restic-Backup-Copy:

```
apk add restic rsync
```
