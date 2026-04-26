#!/bin/sh
#
# Kopieren/Synchronisieren der Slave-Platten vom Backup.
# Das Backup wird mit Restic auf eine externe Festplatte geschrieben.
# Der Inhalt des Restic-Repositories wird mit rsync auf Slave-Platten
# gesichert.
#
# Das Script ist geschrieben fuer die ash Implementierung in der Busybox.
#
###############################################################################


# Abbruch bei Fehler
# Zugriff auf nicht definierte Variable als Fehler einstufen
set -eu

#
# Konfiguration
# UUID der Master-Platte mit dem Restic Repository
# UUID von allen Slave-Platten getrennt durch Leerzeichen.
#
readonly SOURCE_UUID="d6ea3233-be1f-48f7-b9b5-ad820eec9de4"
readonly DEST_UUID="fd777fe2-bc79-4c0d-b2c0-0b9824107875 167aec8d-9856-47e7-95e3-80c5748a6906"


###############################################################################

# Gemeinsam verwendet Funktionen
. ./restic-common-functions.sh


#
# Synchronisieren alle angeschlossenen Slave-Platten mit der Master-Platte.
# Achtung: Dateien auf den Slave-Patten koennen dabei auch geloescht werden.
# Arguments:
#       keine
# Globale Variablen:
#       SOURCE_UUID - UUID Der Master-Platte. (Eingabe)
#       DEST_UUID - UUIDs der Slave-Platten, Leerzeichen getrennt. (Ausgabe)
#
main() {
    local source_dev source_mountpoint dest_dev dest_mountpoint dest_uuid
    #
    # Mount der Master-Platte
    #
    source_dev="/dev/disk/by-uuid/$SOURCE_UUID"
    if [ ! -b "$source_dev" ]
    then
        info_print "Suche Master-Volume $SOURCE_UUID"
        error_exit "Kann Master-Volume nicht finden."
    fi
    info_print "Mount vom Master-Volume $SOURCE_UUID"
    source_mountpoint=$(mktemp --directory)
    # Mount erfolgt read-only
    mount -o ro,lazytime,nodev,nosuid "$source_dev" "$source_mountpoint"
    cleanup_add mount "$source_mountpoint"

    #
    # Synchronisieren aller Slave-Platten von der Master-Platte aus.
    #
    for dest_uuid in $DEST_UUID
    do
        dest_dev="/dev/disk/by-uuid/$dest_uuid"
        if [ -b "$dest_dev" ]
        then
            info_print "Mount des Slave-Volume $dest_uuid"
            dest_mountpoint=$(mktemp --directory)
            mount -o lazytime,nodev,nosuid "$dest_dev" "$dest_mountpoint"
            cleanup_add mount "$dest_mountpoint"
            info_print "Kopieren auf Slave-Volume $dest_uuid"
            rsync -aHAX --sparse --one-file-system --numeric-ids --delete --info=progress2 "${source_mountpoint}/" "${dest_mountpoint}/"
            sync
        fi
    done
}


info_print "restic-copy start"
# Option -u verwendet, weil id in der Busybox die Option --user nicht kennt.
[ "$(id -u)" -eq 0 ] || error_exit "restic-copy muss unter root laufen"
main
