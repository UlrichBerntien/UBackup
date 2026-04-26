#!/bin/sh
#
# Kontrolle der Backup-Festpatten.
# Aktuell werden die Platten getrennt kontrolliert.
# Vergleiche zwischen den Platten sind noch nicht implementiert.
#
# Das Skript ist geschrieben für die ash Implementierung der Busybox.
#
###############################################################################


# Abbruch bei Fehler
# Zugriff auf nicht definierte Variable als Fehler einstufen
set -eu

# Gemeinsam verwendet Funktionen
. ./restic-common-functions.sh

# UUID des Volumes und dazu die Daten auf dem Volume als Code.
readonly UUID_AND_TYPE="\
d6ea3233-be1f-48f7-b9b5-ad820eec9de4 xfs_restic \
fd777fe2-bc79-4c0d-b2c0-0b9824107875 xfs_restic \
167aec8d-9856-47e7-95e3-80c5748a6906 xfs_restic \
29e2501c-93a7-457e-bf96-e2ae36d220ce ext4_gocrypts_mirror \
43232135-1c71-4338-b190-122630b48182 luks_btrfs_mirror \
"

# Pruefe Restic Repository auf einem XFS Dateisystem
# Argument:
#   $1 - Volume
check_xfs_restic() {
    local mountpoint
    info_print "Pruefe Restic Repository auf einem XFS Dateisystem"
    # Hardware pruefen
    smartctl --test=short "$1"
    [ -b "$1" ] || error_exit "Kein Volume $1"
    # Prüfen des XFS Dateisystems mit Status-Ausgaben (-v)
    xfs_repair -v "$1"
    # Mounten für Kontrolle
    mountpoint=$(mktemp --directory)
    mount -o lazytime,nodev,nosuid "$1" "$mountpoint"
    cleanup_add mount "$mountpoint"
    # Alle Daten im Repository pruefen, wird das Repository groesser kann z.B. --read-data-subset='50%' verwendet werden
    info_print "Oefnen Restic Repository fuer Pruefung"
    restic check --read-data "--repo=$mountpoint"
    smartctl --all "$1"
    info_print "Pruefung abgeschlossen"
}


# Pruefe gespiegelte Dateien auf einem BTRFS Dateisystem LUKS verschluesselt
# Argument:
#   $1 - Volume
#   $2 - UUID des Volumes
check_luks_btrfs_mirror() {
    info_print "Pruefe gespiegelte Dateien auf einem BTRFS Dateisystem LUKS verschluesselt"
    [ -b "$1" ] || error_exit "Kein Volume $1"
    # Hardware pruefen
    smartctl --test=short "$1"
    # Verschluesselung oeffnen
    info_print "Oefnen LUKS Verschluesselung fuer Pruefung"
    cryptsetup open "$1" "luks-$2"
    cleanup_add crypt "luks-$2"
    partprobe
    # Pruefen des Dateisystems und pruefen der Datenbloecke ueber die internen Pruefsummen.
    btrfs check --check-data-csum --progress "/dev/mapper/luks-$2"
    smartctl --all "$1"
    info_print "Pruefung abgeschlossen"
}


# Pruefe gespiegelte Dateien gocryptfs verschluesselt auf einem EXT4 Dateisystem
# Argument:
#   $1 - Volume
check_ext4_gocrypts_mirror() {
    local mountpoint
    info_print "Pruefe gespiegelte Dateien gocryptfs verschluesselt auf einem EXT4 Dateisystem"
    [ -b "$1" ] || error_exit "Kein Volume $1"
    # Hardware pruefen
    smartctl --test=short "$1"
    # Dateisystem pruefen
    e2fsck -D -f "$1"
    mountpoint=$(mktemp --directory)
    mount -o lazytime,nodev,nosuid "$1" "$mountpoint"
    cleanup_add mount "$mountpoint"
    # Dateien über Prüfsummen in gocryptfs pruefen
    info_print "Oefnen gocryptfs Verschluesselung fuer Pruefung"
    gocryptfs -fsck "$mountpoint/content"
    smartctl --all "$1"
    info_print "Pruefung abgeschlossen"
}


main() {
    local volume -
    set -- $UUID_AND_TYPE
    [ $(( $# % 2 )) -eq 0 ] || error_exit "BUG: Liste der zu pruefenden Volumes enthaelt keine Paare"
    while [ $# -gt 1 ]
    do
        volume="/dev/disk/by-uuid/$1"
        if [ -b "$volume" ]
        then
            case "$2" in
            xfs_restic)
                check_xfs_restic "$volume"
                ;;
            luks_btrfs_mirror)
                check_luks_btrfs_mirror "$volume" "$1"
                ;;
            ext4_gocrypts_mirror)
                check_ext4_gocrypts_mirror "$volume"
                ;;
            esac
        fi
	shift 2
    done
}


###############################################################################

# Option -u verwendet, weil id in der Busybox die Option --user nicht kennt.
[ "$(id -u)" -eq 0 ] || error_exit "Backup Check muss unter root laufen"
main
