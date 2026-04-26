#!/bin/sh
#
# Gemeinsame Funktionen fuer Restic-Backup und Restic-(Backup)-Copy.
#
###############################################################################

#
# Fehlermeldung ausgeben und Script beenden
# Fehlermeldung wird in roter Schrift ausgegeben.
# Argumente:
#       alles wird ausgegeben
#
error_exit() {
    if [ -t 1 ]
    then printf "\033[1;31mFEHLER: %s\n\033[0m" "$*" >&2
    else printf "%s\n" "$*" >&2
    fi
    exit 1
}


#
# Informations-Meldung ausgeben.
# Informations-Meldung wird in blauer Schrift ausgegeben.
# Argumente:
#       alles wird ausgegeben
#
info_print() {
    if [ -t 1 ]
    then printf "\033[1;34m%s\n\033[0m" "$*" >&2
    else printf "%s\n" "$*" >&2
    fi
}

# Stack fuer mount and cryptsetup, die geschlossen werden muessen.
# Paare von Eintraege "crypt Mapping-Name" oder "mount Mount-Punkt"
# fuer offene cryptsetup oder mount Volumes.
CLEANUP_MOUNTS=""

# Eine Aufraeumarbeit eintragen
# Argumente:
#   1) crypt|mount
#   2) Name fuer cryptsetup close oder umount
cleanup_add() {
    [ $# -eq 2 ] || error_exit "BUG: cleanup_add benoetigt Typ und Ziel"
    [ "$1" = 'crypt' ] || [ "$1" = 'mount' ] || error_exit "BUG: falsche cleanup_add Methode"
    CLEANUP_MOUNTS="$1 $2 $CLEANUP_MOUNTS"
}

# Aufraeumen durchfuehren.
cleanup_do() {
    local -
    # Das Aufraeumen nicht beim ersten Fehler abbrechen
    set +e
    set -- $CLEANUP_MOUNTS
    [ $(( $# % 2 )) -eq 0 ] || error_exit "BUG: Cleanup Liste defekt"
    while [ $# -gt 0 ]
    do
        case "$1" in
            crypt)
                cryptsetup close "$2"
                ;;
            mount)
                # umount in der Busyshell kennt nicht die Optionen --quiet --lazy.
                # Daher hier nur der Aufruf von umount mit der Option -l fuer lazy umount.
                umount -l "$2"
                ;;
        esac
        shift 2
    done
}

# Immer am Skriptende aufraeumen
trap cleanup_do EXIT INT TERM
