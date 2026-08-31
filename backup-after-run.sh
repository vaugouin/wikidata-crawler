#!/bin/bash
# =============================================================================
# Sauvegarde complete de la base APRES un run reussi du crawler.
# =============================================================================
#
# POURQUOI ICI. Le bon moment pour sauvegarder la base, c'est quand le crawler
# vient de finir : les tables cibles portent le lot courant, l'etape 114 a retire
# les lignes des lots precedents et l'etape 115 a vide le staging de ses anciens
# lots. La base est alors dans son etat le plus propre et le plus petit de la
# semaine, et le prochain run ne la touchera pas avant trois ou quatre jours.
#
# POURQUOI PAS DANS LE CRAWLER. Le script de sauvegarde vit sur l'hote, dans une
# autre pile (damp-vaugouin-com), qui n'est pas montee dans le conteneur du
# crawler. Une etape 116 ne pourrait donc pas l'appeler. La decision reste cote
# hote, ou elle a acces aux deux.
#
# COMMENT IL SAIT QU'UN RUN A REUSSI. Le conteneur tourne avec --rm : une fois
# sorti, il ne reste rien sur l'hote. Le volume partage est le seul terrain
# commun, donc un run reussi y depose last_successful_run.json (voir
# WikidataCrawler._write_success_sentinel). Ce script le lit, le compare au
# marqueur du dernier lot deja sauvegarde, et n'agit que si les deux different :
# un run donne declenche une sauvegarde et une seule, quel que soit le nombre de
# passages horaires.
#
# CE QUE FAIT LE SCRIPT APPELE (backupvaugouindb.sh, pile damp-vaugouin-com) :
# il source /home/debian/docker/damp-vaugouin-com/.env, puis lance mariadb-dump
# DANS le conteneur de la base (docker exec), et ecrit
# /backups/<base>-backup-<horodatage>.sql.gz, chemin interne au conteneur.
#
# POURQUOI ON VERIFIE MALGRE TOUT. Jusqu'au 2026-08-31, ce script se terminait
# sur un `if [ $? -eq 0 ] ... else echo "Backup failed!"` qui AFFICHAIT l'echec
# sans sortir en erreur : son code de retour etait celui du echo, donc 0, quoi
# qu'il arrive. Et son propre test etait faux, car $? apres un `docker exec bash
# -c "mariadb-dump ... | gzip > f"` est celui de gzip, pas du dump.
#
# Les deux sont corriges cote tmdb-front (backupvaugouindb-common.sh : pipefail,
# vrais codes de retour, verification de la taille et du marqueur de fin). On
# verifie quand meme ici, pour deux raisons : la copie deployee sur le VPS peut
# etre en retard sur le depot, et celui qui commande une sauvegarde ne devrait
# pas dependre de la rigueur de celui qui l'execute.
#
# Verification, dans cet ordre : code de retour, absence de "Backup failed!"
# dans la sortie, puis TAILLE REELLE du fichier produit, relue dans le
# conteneur. C'est la seule des trois qui distingue une sauvegarde d'une
# sauvegarde vide.
#
# EN CAS D'ECHEC de la sauvegarde, le marqueur n'est PAS ecrit. Le passage
# suivant reessaiera donc, jusqu'a reussir ou jusqu'a ce que le lancement du run
# suivant efface le volume partage. Un echec silencieux est ainsi impossible.
#
# USAGE
#   ./backup-after-run.sh              # sauvegarde si un run reussi l'attend
#   ./backup-after-run.sh --wait       # attend d'abord la fin du conteneur en cours
#   ./backup-after-run.sh --force      # sauvegarde meme si ce lot l'a deja ete
#   ./backup-after-run.sh --dry-run    # dit ce qu'il ferait, sans rien faire
#
# APPEL AUTOMATIQUE. run-if-new-dump.sh l'appelle a chaque passage horaire, avant
# de verifier le dump : la sauvegarde du run precedent part donc toujours AVANT
# que le run suivant ne commence a modifier la base. Rien a ajouter dans cron.
#
# Pour le lancer separement malgre tout, par exemple a une heure creuse :
#   47 5 * * * /home/debian/docker/wikidata-crawler/backup-after-run.sh >> \
#              /home/debian/docker/wikidata-crawler/backup-after-run.log 2>&1
# =============================================================================

set -uo pipefail

# cron demarre avec un PATH minimal, qui ne contient pas toujours docker. On
# COMPLETE le PATH herite au lieu de l'ecraser, pour que le script reste
# executable ailleurs que sur le VPS (bancs d'essai, autre machine).
PATH="${PATH:-}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export PATH

# Les trois chemins sont surchargeables par l'environnement : c'est ce qui permet
# de faire tourner le script hors du VPS sans toucher a ses valeurs par defaut.
STACK="${STACK_DIR:-/home/debian/docker/wikidata-crawler}"
SHARED="${SHARED_DIR:-/home/debian/docker/shared_data/wikidata-crawler}"
CONTENEUR=wikidata-crawler
SENTINELLE="$SHARED/last_successful_run.json"
MARQUEUR="$STACK/.last-backup-batch"
VERROU=/tmp/backup-after-run.lock

# Le script de sauvegarde de la pile damp-vaugouin-com. Surchargeable par
# l'environnement pour les tests ou si la pile demenage.
SCRIPT_SAUVEGARDE="${BACKUP_SCRIPT:-/home/debian/docker/damp-vaugouin-com/backupvaugouindb.sh}"

# Plancher de taille du .gz produit, en octets. Un dump complet de vaugouindb
# compresse pese des centaines de Mo ; 1 Mo est donc tres en dessous du normal et
# tres au-dessus d'un fichier vide ou tronque des les premieres tables.
TAILLE_MINIMALE="${TAILLE_MINIMALE:-1048576}"

ATTENDRE=0
FORCER=0
DRY_RUN=0
for argument in "$@"; do
  case "$argument" in
    --wait)    ATTENDRE=1 ;;
    --force)   FORCER=1 ;;
    --dry-run) DRY_RUN=1 ;;
    *) echo "ERREUR : argument inconnu '$argument'. Voir l'en-tete du script."; exit 2 ;;
  esac
done

# --------------------------------------------------------------------------
# VERROU : une seule sauvegarde a la fois. Une sauvegarde complete peut durer
# plus longtemps que l'heure qui separe deux passages ; sans verrou, le passage
# suivant en lancerait une seconde par-dessus.
# --------------------------------------------------------------------------
# flock absent et verrou deja pris se ressemblent : `! flock` est vrai dans les
# deux cas, d'ou le test separe.
if ! command -v flock >/dev/null 2>&1; then
  echo "ERREUR : flock introuvable (paquet util-linux). On s'arrete."
  exit 2
fi
exec 8>"$VERROU" || { echo "ERREUR : verrou $VERROU inaccessible."; exit 2; }
if ! flock -n 8; then
  echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') : une sauvegarde est deja en cours, on passe."
  exit 0
fi

# --------------------------------------------------------------------------
# --wait : attendre la fin du conteneur avant de decider.
# Sans cette option, un conteneur en cours signifie simplement qu'il n'y a rien a
# faire : la sentinelle du run precedent a deja ete traitee, celle du run en
# cours n'existe pas encore.
# --------------------------------------------------------------------------
if [ -n "$(docker ps -q -f name="^${CONTENEUR}$" 2>/dev/null)" ]; then
  if [ "$ATTENDRE" = "1" ]; then
    echo "Le crawler tourne. Attente de sa fin (trois a quatre jours pour un run complet) ..."
    CODE_CONTENEUR=$(docker wait "$CONTENEUR" 2>/dev/null)
    echo "Conteneur termine, code de retour ${CODE_CONTENEUR:-inconnu}."
    if [ "${CODE_CONTENEUR:-1}" != "0" ]; then
      echo "Le run a echoue : pas de sauvegarde. Voir la variable serveur"
      echo "strwikidatacrawlerlasterror pour la cause."
      exit 1
    fi
  else
    echo "Le crawler tourne, sa sentinelle n'existera qu'a la fin. Rien a faire."
    echo "(--wait pour attendre sa fin et sauvegarder dans la foulee.)"
    exit 0
  fi
fi

# --------------------------------------------------------------------------
# La sentinelle : y a-t-il un run reussi ?
# --------------------------------------------------------------------------
if [ ! -f "$SENTINELLE" ]; then
  echo "Aucune sentinelle de run reussi ($SENTINELLE). Rien a sauvegarder."
  exit 0
fi

lire_champ() {
  # Extrait une valeur de chaine du JSON d'une ligne, sans jq (absent du VPS).
  sed -nE "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"([^\"]*)\".*/\1/p" "$SENTINELLE"
}

STATUT=$(lire_champ status)
LOT=$(lire_champ import_batch_id)
FIN=$(lire_champ finished_at)

if [ "$STATUT" != "SUCCESS" ]; then
  echo "La sentinelle ne porte pas SUCCESS (statut lu : '${STATUT:-vide}'). Rien a faire."
  exit 0
fi
if [ -z "$LOT" ]; then
  echo "ERREUR : sentinelle illisible, IMPORT_BATCH_ID introuvable dans $SENTINELLE :"
  cat "$SENTINELLE"
  exit 2
fi

DEJA_FAIT=""
[ -f "$MARQUEUR" ] && DEJA_FAIT=$(cat "$MARQUEUR" 2>/dev/null)

if [ "$LOT" = "$DEJA_FAIT" ] && [ "$FORCER" = "0" ]; then
  echo "Le lot $LOT a deja ete sauvegarde. Rien a faire (--force pour refaire)."
  exit 0
fi

# --------------------------------------------------------------------------
# Sauvegarde.
# --------------------------------------------------------------------------
if [ ! -f "$SCRIPT_SAUVEGARDE" ]; then
  echo "ERREUR : script de sauvegarde introuvable : $SCRIPT_SAUVEGARDE"
  echo "Corriger le chemin dans ce script, ou passer BACKUP_SCRIPT=... dans"
  echo "l'environnement. Le marqueur n'est pas ecrit : le passage suivant"
  echo "reessaiera."
  exit 2
fi

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') : sauvegarde de la base ==="
echo "  lot termine    : $LOT"
echo "  fin du run     : ${FIN:-?}"
echo "  script appele  : $SCRIPT_SAUVEGARDE"

if [ "$DRY_RUN" = "1" ]; then
  echo "--dry-run : je m'arrete ici. J'aurais execute le script ci-dessus,"
  echo "puis ecrit '$LOT' dans $MARQUEUR."
  exit 0
fi

# Depuis son propre repertoire : un script de pile suppose souvent y trouver son
# .env ou son docker-compose.yml. La sortie est capturee pour etre relue plus
# bas, et reaffichee telle quelle pour rester dans le journal.
SORTIE=$( cd "$(dirname "$SCRIPT_SAUVEGARDE")" && bash "$SCRIPT_SAUVEGARDE" 2>&1 )
CODE=$?
printf '%s\n' "$SORTIE" | sed 's/^/  /'

echouer() {
  echo "ERREUR : $1"
  echo "Le marqueur n'est pas ecrit, le passage suivant reessaiera."
  exit 2
}

[ "$CODE" -ne 0 ] && echouer "le script de sauvegarde a rendu le code $CODE."
if printf '%s' "$SORTIE" | grep -qiE 'Backup failed|^Error:'; then
  echouer "le script de sauvegarde signale un echec dans sa sortie."
fi

# Verification de la taille : la seule qui distingue une vraie sauvegarde d'un
# .gz vide. On relit le chemin et le conteneur dans la ligne de confirmation
# (« File saved as /backups/x.sql.gz in container y. »).
FICHIER=$(printf '%s' "$SORTIE" | sed -nE 's|.*File saved as ([^ ]+) in container ([^ ]+)$|\1|p' | tail -1)
BASE_CONTENEUR=$(printf '%s' "$SORTIE" | sed -nE 's|.*File saved as ([^ ]+) in container ([^ ]+)$|\2|p' | tail -1)
BASE_CONTENEUR="${BASE_CONTENEUR%.}"

if [ -z "$FICHIER" ] || [ -z "$BASE_CONTENEUR" ]; then
  # Sortie inattendue : on ne peut pas verifier, mais rien ne prouve un echec.
  # On accepte en le disant, plutot que de bloquer la chaine sur une phrase qui
  # a change de forme.
  echo "AVERTISSEMENT : impossible de relire le chemin du fichier dans la sortie."
  echo "Taille non verifiee. Sauvegarde consideree comme reussie."
else
  TAILLE=$(docker exec "$BASE_CONTENEUR" stat -c %s "$FICHIER" 2>/dev/null)
  case "$TAILLE" in
    ''|*[!0-9]*) echouer "fichier $FICHIER introuvable dans le conteneur $BASE_CONTENEUR." ;;
  esac
  if [ "$TAILLE" -lt "$TAILLE_MINIMALE" ]; then
    echouer "$FICHIER ne pese que $TAILLE octets (plancher : $TAILLE_MINIMALE). Dump tronque ou vide."
  fi
  echo "  fichier verifie : $FICHIER, $TAILLE octets, dans $BASE_CONTENEUR"
fi

echo "$LOT" > "$MARQUEUR"
echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') : sauvegarde terminee pour le lot $LOT ==="
