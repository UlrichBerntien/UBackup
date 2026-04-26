# Konzept Datensicherung

Gesichert werden die Dokumente und langfristig benoetigte Daten.
Gesichert werden auch die Dateien an denen aktuell gearbeitet wird.
Nicht gesichert wird das instellierte Linux und die installierten Programme.

## Die zu sichernden Daten

Die Daten liegen auf getrennten Subvolumes von BTRFS.
Das Backup Skript soll auch arbeiten wenn die getrennte BTRFS Filesysteme verwendet werden.
BTRFS wird verwendet, weil diese Filesystem Pruefsummen von Daten kontrollierten kann.

| (Sub)Volume | Inhalt                                         |
| ----------- | ---------------------------------------------- |
| @Data       | Dokumente und langfristig notwendige Dateien   |
| @Work       | Dateien an denen aktuell gearbeitet wird       |

Aktuell liegen die Dateien auf den Arbeits-PC.
Kuenftig koennten die Dateien auf ein NAS verschoben werden um mit mehreren Geraeten zu arbeiten.

Immer liegen alle Daten auf einem Dateisystem. Verknuepfungen auf andere
Dateisysteme sollten nicht existieren. Falls sie existieren werden sie
nicht im Backup gesichert.

Die BTRFS Dateisysteme liegen auf Volumes, die mit LUKS verschluesselt sind.

## Die Backups

Die Backups werden auf externe Festplatten gemacht.

Festplatten sind billiger als Bandlaufwerke und billiger als SSD.
Festplatten halten die Daten laenger als SSD.

Drei verschiedene Software Kombinationen werden verwendet:

- Verschluesseltes Restic Repository auf XFS
- Mirror auf LUKS verschluesseltem BTRFS
- Mirror mit gocryptfs verschluesselt auf EXT4

Falls ein systematischer Fehler in einem Verschluesselungsprogramm
oder in einem Filesystem auftritt, gibt es zwei Sicherungen die nicht
betroffen sein sollen. Falls ein systematischer Fehler in tiefer liegenden,
gemeinsam genutzten, Softwareschichten auftritt, besteht die Hoffnung das
nicht alle drei verwendeten Programme betroffen sind.

Es bleibt das Risiko, dass die gocryptfs verschluesselten Dokumente und
E-Mails durch einen Fehler im gocryptfs verloren gehen. Das Risiko koennte
behoben werden, indem beim Backup die verschluesselten Datien entschluesselt
werden.
Dann ist das Risiko, dass beim Backup alle Dateien einmal unverschluesselt
durchgereicht werden. Es wurde entschieden die innere Verschluesselung der
Dateien mit gocrypt nicht aufzuheben, nur die aeussere Verschluesselung ueber LUKS.

Drei Festplatten werden verwendet.
Eine Festplatte ist das Master-Backup.
Die anderen beiden Festplatten sind Slave-Backups.

Das Backup-Skript kopiert die Daten vom Arbeits-PC auf das Master-Backup.
Das Copy-Skript kopiert Daten vom Master-Backup auf ein Slave-Backup.

Auf dem Master-Backup werden alle 3 Software-Kombinationen gespeichert.
Auf jedem Slave-Backup wird nur eine Kopie des verschluesselten Restic Repositories gespeichert.

Daher koennen die Slave-Backup Festplatten kleiner und billiger sein.
Nachteil ist das Risiko fuer einen kombinierten Hardware-Ausfall des Master-Backup
und Software-Fehler bei Restic auf den beiden Slave-Backups.

# Kontrollen der Backups

Die gesicheten Daten liegen in drei verschiedenen Konfigurationen vor.
Jede Konfigurationen wird mit einer zugeschnitte Methode geprüft.

- XFS / Restic-Repository

Kontrolle des XFS Dateisystems auf Fehler ggf. mit Reperatur von Fehlern. (`xfs_repair`)
Kontrolle über die Prüfsummen innerhalb des Restic-Repository. (`restic check --read-data`)

- LUKS / BTRFS / Datei-Kopien

Kontrolle über die Prüfsummen der Datenblöcke innerhalb des BTRFS Dateisystems. (`btrfs check`)

- EXT4 / gocrypfs / Datei-Kopien

Kontrolle des ext4 Dateisystems.  (`e2fsck -D -f`)
Kontrolle über die Prüfsummen der Klartext-Dateien innerhalb des gocryptfs Dateisystems. (`gocryptfs -fsck`)

## Kontrolle der Backups gegeneinander

TODO

## Kontrolle Backup gegen Live-Daten

TODO

