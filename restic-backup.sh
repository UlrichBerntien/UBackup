#!/bin/sh
#
# Backup der Datenverzeichnisse vom Arbeits-PC
# - Spiegeln der Daten auf externes Dateisystem
# - Inkrementelles Backup mit Restic
# Kennwoerter werden im Dialog eingegeben.
#
# Backup wird auf getrenntem System gestartet, das von einer externen SSD
# gestartet wird. Das System wird nur fuer Backup Erstellen verwendet.
# Geschrieben wird das Backup (Mirrors und Restic Repository) auf eine
# externe HD.
#
# Das Script ist geschrieben fuer die ash Implementierung in der Busybox.
# Das Backup wird auf einem kleinen Alpine Linux System durchgefuehrt.
#
###############################################################################

# Abbruch bei Fehler
# Zugriff auf nicht definierte Variable als Fehler einstufen
set -eu

#
# Konfiguration
#
# Hostname fuer das Restic Backup.
# Damit wird sichergestellt, dass immer der gleiche Hostname verwendet wird,
# auch wenn der aktuelle Arbeits-PC auf eine andere Maschine wechselt.
readonly RESTIC_HOST="Backy"
# Volume auf dem das Restic Repository liegt.
readonly RESTIC_DEVICE="/dev/disk/by-uuid/d6ea3233-be1f-48f7-b9b5-ad820eec9de4"

# Namen der BTRFS Subvolumes, die gesichert werden.
# Die Subvolumes werden unter /mnt/data und /mnt/work gemountet.
# Der vollstaendige Name der BTRFS Subvolumes ist @Data und @Work
readonly SOURCE_NAMES="@Data /mnt/data @Work /mnt/work"
# Volume auf dem LUKS verschluesselt das BTRFS Filesystem ist.
readonly SOURCE_UUID_ENCRYPTED="771a5dda-716a-4929-87cc-4bf7f18cdbe8"

# Die beiden Volumes auf die alle Dateien der beiden Source Subvolumes gespiegelt werden.
# Der Mirror1 hat ein mit LUKS verschluesseltes Dateisystem.
# Der Mirror2 hat ein Filesystem und darin gocrypfs verschluesselt die Dateien.
readonly MIRROR_1_UUID_ENCRYPTED="43232135-1c71-4338-b190-122630b48182"
readonly MIRROR_2_UUID="29e2501c-93a7-457e-bf96-e2ae36d220ce"


###############################################################################

# Gemeinsam verwendet Funktionen
. ./restic-common-functions.sh

#
# Datensicherung durch Spiegeln und Inkrementelles Backup
#
main() {
    info_print "Backup start"
    # fuse Modul in den Kernel laden, wird von gocryptfs benoetigt.
    # Bei Alpine Linux ist das fuse Modul nicht als Standard geladen.
    modprobe fuse
    # Zugriff auf Quellen herstellen
    open_sources
    # Inkrementelles Backup mit Restic
    # Mit dem Backup beginnen, falls beim rsync etwas falsch laeuft.
    do_backup
    # Spiegeln auf Filesystem 1 (LUKS verschluesseltes BTRFS Dateisystem)
    do_mirror_1
    # Spiegeln auf Filesystem 2 (gocryptfs verschluesselts EXT4 Dateisystem)
    do_mirror_2
}

#
# Prueft ob auf einem Mountpoint bereits ein Geraet gemounted ist.
# Argument:
#       Pfad des Mountpoints
# Rueckgabe:
#       1 falls gemounted, sonst 0  
#
is_mounted() {
    [ -d "$1" ] || error_exit "Mountpoint existiert nicht. Fehlt: $1"
    # Suche nach dem Mount. Der Mountpoint muss durch Leerzeichen abgegrenzt sein,
    # dann ist sichergestellt, dass er nicht Teil eines anderen Pfads ist.
    grep -q " $1 " /proc/mounts
}


#
# Herstellen des Zugriffs (nur lesen) auf die Quellen
# Variablen:
#       SOURCE_UUID_ENCRYPTED - UUID vom verschluesselten Geraet mit den Quellen
#       SOURCE_NAMES - Subvolumes und Mountpoints der Sources
#       SOURCE_DEVICE - Geraet mit der Quelle (das ist ein BTRFS Volume)
# Argumente:
#       keine
#
open_sources() {
    local dev_crypted dev_source -
    dev_crypted="/dev/disk/by-uuid/$SOURCE_UUID_ENCRYPTED"
    dev_source="/dev/mapper/luks-$SOURCE_UUID_ENCRYPTED"
    [ -b "$dev_crypted" ] || error_exit "Kann Geraet mit verschluesselter Quelle nicht finden. Fehlt: $SOURCE_UUID_ENCRYPTED"
    if [ ! -b "$dev_source" ]
    then
        info_print "Oeffnen der Quelle $SOURCE_UUID_ENCRYPTED"
        cryptsetup open "$dev_crypted" "luks-$SOURCE_UUID_ENCRYPTED"
        cleanup_add crypt "luks-$SOURCE_UUID_ENCRYPTED"
        partprobe
    fi
    [ -b "$dev_source" ] || error_exit "Kann Volumen mit Quelle nicht finden. Fehlt: $dev_source"
    set -- $SOURCE_NAMES
    [ $(( $# % 2 )) -eq 0 ] || error_exit "Bug: SOURCE_NAMES enthaelt keine Paare"
    while [ $# -gt 1 ]
    do
        [ -d "$2" ] || mkdir -p "$2"
        [ -d "$2" ] || error_exit "Mountpoint kann nicht erstellt werden, fehlt: $2"
        if ! is_mounted "$2"
        then
            mount -o "ro,subvol=/$1" "$dev_source" "$2"
            cleanup_add mount "$2"
        fi
        shift 2
    done
}


#
# Spiegeln der Daten auf das Ziel-Dateisystem 1.
# LUKS verschluesseltes BTRFS Dateisystem.
# Variablen:
#       SOURCE_NAMES - Subvolumes und Mountpoints der Sources
#       MIRROR_1_UUID_ENCRYPTED - UUID des LUKS verschluesselten Volumes
# Argumente:
#       keine
#
do_mirror_1() {
    local dev_crypted dev_mirror mountpoint sub dest -
    info_print 'Spiegeln der Dateien mit rsync auf Platte mit LUKS Verschluesselung'
    # Zugriff auf Ziel herstellen
    dev_crypted=/dev/disk/by-uuid/$MIRROR_1_UUID_ENCRYPTED
    dev_mirror=/dev/mapper/luks-$MIRROR_1_UUID_ENCRYPTED
    [ -b "$dev_crypted" ] || error_exit "Kann Geraet fuer Spiegelung nicht finden. Fehlt: $dev_crypted"
    if [ ! -b "$dev_mirror" ]
    then
        info_print "Oeffnen Ziel fuer Dateien spiegeln, $MIRROR_1_UUID_ENCRYPTED"
        cryptsetup open "$dev_crypted" "luks-$MIRROR_1_UUID_ENCRYPTED"
        cleanup_add crypt "luks-$MIRROR_1_UUID_ENCRYPTED"
    fi
    [ -b "$dev_mirror" ] || error_exit "Kann Volumen mit Spiegel nicht finden. Fehlt: $dev_mirror"
    mountpoint=$(mktemp --directory)
    mount -o lazytime,nodev,nosuid "$dev_mirror" "$mountpoint"
    cleanup_add mount "$mountpoint"
    # Spiegeln
    # copy/update the file tree
    info_print "Dateien werden mit rsync gespiegelt"
    # Zerlegen der Source Verzeichnisse und Subvolumes
    set -- $SOURCE_NAMES
    [ $(($# % 2)) -eq 0 ] || error_exit "Bug: SOURCE_NAMES enthaelt keine Paare"
    while [ $# -gt 1 ]
    do
        sub=$(basename "$2")
        dest=${mountpoint}/${sub}/
        [ -d "$2" ] || error_exit "Kann nicht auf Quell-Verzeichnis zugreifen, fehlt: $2"
        [ -d "$dest" ] || error_exit "Kann Zielverzeichnis im Spiegel nicht finden. Fehlt: $dest"
        rsync -aHAX --sparse --one-file-system --numeric-ids --delete --info=progress2 "$2/" "$dest"
        shift 2
    done
    # TODO Stichproben, dass neue Dateien auf Spiegel sind
    sync
}


#
# Spiegeln der Daten auf das Ziel-Dateisystem 2.
# Variablen:
#       SOURCE_NAMES - Subvolumes und Mountpoints der Sources
#       MIRROR_2_UUID - UUID des Volumes auf dem die Dateien gocryptfs gespiegelt sind
# Argumente:
#       keine
#
do_mirror_2() {
    local dev_mirror mountpoint_crypted mountpoint -
    info_print 'Spiegeln der Dateien mit rsync auf Platte mit gocryptfs Verschluesselung'
    # Zugriff auf Ziel herstellen
    dev_mirror=/dev/disk/by-uuid/$MIRROR_2_UUID
    [ -b "$dev_mirror" ] || error_exit "Kann Geraet fuer Spiegelung nicht finden. Fehlt: $dev_mirror"
    mountpoint_crypted=$(mktemp --directory)
    mount -o lazytime,nodev,nosuid "$dev_mirror" "$mountpoint_crypted"
    cleanup_add mount "$mountpoint_crypted"
    mountpoint=$(mktemp --directory)
    info_print "Oeffne Ziel fuer Datei-Spiegelung ueber gocryptfs"
    gocryptfs "${mountpoint_crypted}/content" "$mountpoint"
    # gocryptfs arbeitet zusammen mit umount, kein spezielles close notwendig.
    cleanup_add mount "$mountpoint"
    # Spiegeln
    # copy/update the file tree
    info_print "Dateien werden mit rsync gespiegelt"
    # Zerlegen der Source Verzeichnisse und Subvolumes
    set -- $SOURCE_NAMES
    [ $(( $# % 2 )) -eq 0 ] || error_exit "Bug: SOURCE_NAMES enthaelt keine Paare"
    while [ $# -gt 1 ]
    do
        local sub dest
        sub=$(basename "$2")
        dest=${mountpoint}/${sub}/
        [ -d "$2" ] || error_exit "Kann nicht auf Quell-Verzeichnis zugreifen, fehlt: $2"
        [ -d "$dest" ] || error_exit "Kann Zielverzeichnis im Spiegel nicht finden. Fehlt: $dest"
        rsync -aHAX --sparse --one-file-system --numeric-ids --delete --info=progress2 "$2/" "$dest"
        shift 2
    done
    # TODO Stichproben, dass neue Dateien auf Spiegel sind
    sync
}


#
# Inkrementelles Backup.
# Variablen:
#       SOURCE_NAMES - Subvolumes und Mountpoints der Sources
#       RESTIC_DEVICE - Volume auf dem das Restic Backup Repository liegt
#       RESTIC_HOST - Hostname unter dem das Restic Backup gespeichert wird
# Argumente:
#       keine
#
do_backup() {
    local mountpoint -
    info_print 'Sichere Dateien mit Restic Backup'
    # Zugriff auf Restic Repository herstellen
    info_print "Suche Backup Speicher Volume $RESTIC_DEVICE"
    [ -b "$RESTIC_DEVICE" ] || error_exit "Kann Geraet mit Restic Backup Repository nicht finden."
    info_print "Verbinde Ziel fuer Rectic-Backup, $RESTIC_DEVICE"
    mountpoint=$(mktemp --directory)
    mount -o lazytime,nodev,nosuid "$RESTIC_DEVICE" "$mountpoint"
    cleanup_add mount "$mountpoint"
    # Inkrementelles Backup mit Restic
    info_print "Sichere Daten mit Rectic-Backup"
    # Zerlegen der Source Verzeichnisse und Subvolumes
    set -- $SOURCE_NAMES
    [ $(( $# % 2 )) -eq 0 ] || error_exit "Bug: SOURCE_NAMES enthaelt keine Paare"
    while [ $# -gt 1 ]
    do
        info_print "Erstelle Rectic-Backup von $2"
        [ -d "$2" ] || error_exit "Kann nicht auf Quell-Verzeichnis zugreifen, fehlt: $2"
        restic backup --one-file-system "--repo=$mountpoint" "--host=$RESTIC_HOST" "$2"
        shift 2
    done
    # TODO: Stichproben, dass die neue Dateien im Backup sind
    sync
}


###############################################################################

# Option -u wird verwendet, weil id in der Busybox die Option --user nicht kennt.
[ "$(id -u)" -eq 0 ] || error_exit "Backup muss unter root laufen"
main
