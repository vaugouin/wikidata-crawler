#!/bin/bash
# =============================================================================
# Lance le crawler UNIQUEMENT si Wikimedia a publie un nouveau dump.
# =============================================================================
#
# POURQUOI. Le dump JSON de Wikidata demarre le lundi mais met environ quatre
# jours a se generer, et `latest-all.json.bz2` n'est rafraichi qu'a la fin. Lancer
# entre les deux re-telecharge le dump PRECEDENT sous un nouvel identifiant de
# batch et re-ingere des donnees deja presentes. C'est arrive le 2026-08-03 :
# 3 jours 18 heures de VPS pour un resultat identique au chiffre pres, sans que
# rien ne le signale puisque le run etait un succes.
#
# La procedure de lancement efface d'abord le volume partage, donc au moment ou
# l'on se poserait la question il est deja trop tard pour comparer. Ce script
# inverse l'ordre : il verifie AVANT, et n'efface que s'il y a du neuf.
#
# CE QU'IL FAIT, dans cet ordre, et seulement si le dump est nouveau :
#   0. sauvegarde la base si un run reussi l'attend (backup-after-run.sh), avant
#      toute chose : si un nouveau run part, il va modifier la base pendant trois
#      jours, et la sauvegarde de l'etat termine doit partir avant ;
#   1. verifie, dans un conteneur jetable, sans telecharger un octet ;
#   2. ecrit un nouvel IMPORT_BATCH_ID dans .env (l'ancien est sauvegarde) ;
#   3. vide /home/debian/docker/shared_data/wikidata-crawler ;
#   4. lance ./wikidata-crawler.sh, qui reconstruit l'image et part sur 101.
#
# DEUX GARDES, pour qu'il puisse tourner toutes les heures sans surveillance :
#   1. un verrou flock : une seule execution a la fois ;
#   2. si le conteneur wikidata-crawler tourne, on sort immediatement, sans meme
#      interroger Wikimedia. Un run dure trois a quatre jours, et pendant ce temps
#      l'ancre de comparaison est instable : l'etape 101 a deja telecharge le
#      nouveau dump mais n'a pas encore enregistre sa taille. Verifier la serait au
#      mieux inutile, au pire destructeur, puisque relancer effacerait le volume
#      partage sous les pieds du run en cours.
#
# USAGE
#   ./run-if-new-dump.sh              # verifie, et lance s'il y a du neuf
#   ./run-if-new-dump.sh --dry-run    # verifie et dit ce qu'il ferait, sans agir
#
# CRON, toutes les heures a la minute 17, decalee du haut de l'heure ou tout le
# monde interroge Wikimedia en meme temps. Une verification coute une requete HEAD,
# et 167 fois sur 168 elle ne fait rien.
#   17 * * * * /home/debian/docker/wikidata-crawler/run-if-new-dump.sh >> \
#              /home/debian/docker/wikidata-crawler/run-if-new-dump.log 2>&1
#
# Les journaux du run NE partent PAS dans ce fichier : wikidata-crawler.sh ne suit
# les journaux du conteneur que devant un terminal, sans quoi la tache cron
# resterait vivante trois jours a ecrire des giga-octets. Les suivre a la main :
#   docker logs -f wikidata-crawler
# =============================================================================

set -uo pipefail

# cron demarre avec un PATH minimal, qui ne contient pas toujours docker.
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

STACK=/home/debian/docker/wikidata-crawler
SHARED=/home/debian/docker/shared_data/wikidata-crawler
IMAGE=wikidata-crawler-python-app
CONTENEUR=wikidata-crawler
VERROU=/tmp/run-if-new-dump.lock

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

cd "$STACK" || { echo "ERREUR : $STACK introuvable."; exit 2; }

# --------------------------------------------------------------------------
# GARDE 1 : une seule execution a la fois.
# Le verrou protege la fenetre entre la verification et le demarrage du
# conteneur : deux executions simultanees pourraient toutes deux conclure « dump
# nouveau » et lancer deux crawlers sur le meme volume partage. -n = ne pas
# attendre, sortir tout de suite : une execution horaire n'a aucune raison de
# faire la queue.
# --------------------------------------------------------------------------
# flock absent et verrou deja pris se ressemblent : `! flock` est vrai dans les
# deux cas. Sans ce test, un flock manquant ferait taire le script pour toujours,
# sous un message qui accuserait une execution concurrente inexistante.
if ! command -v flock >/dev/null 2>&1; then
  echo "ERREUR : flock introuvable (paquet util-linux). Sans verrou, deux executions"
  echo "simultanees pourraient lancer deux crawlers sur le meme volume. On s'arrete."
  exit 2
fi
exec 9>"$VERROU" || { echo "ERREUR : verrou $VERROU inaccessible."; exit 2; }
if ! flock -n 9; then
  echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') : une autre execution est en cours, on passe."
  exit 0
fi

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') : verification du dump Wikidata ==="

# --------------------------------------------------------------------------
# GARDE 2 : ne rien verifier pendant que le crawler tourne.
# Un run dure trois a quatre jours. Pendant ce temps l'ancre de comparaison est
# instable : l'etape 101 a deja telecharge le nouveau dump mais n'a pas encore
# enregistre sa taille, et le volume partage se remplit. Une verification dans
# cette fenetre pourrait conclure « nouveau dump » et relancer par-dessus le run
# en cours, ce qui detruirait ses fichiers de travail. On sort donc sans rien
# faire, silencieusement : c'est le cas normal 99 fois sur 100 apres un depart.
# --------------------------------------------------------------------------
if [ -n "$(docker ps -q -f name="^${CONTENEUR}$" 2>/dev/null)" ]; then
  DEPUIS=$(docker inspect -f '{{.State.StartedAt}}' "$CONTENEUR" 2>/dev/null | cut -c1-19)
  echo "Le crawler tourne deja (demarre le ${DEPUIS:-?}). Rien a faire."
  exit 0
fi

# --------------------------------------------------------------------------
# SAUVEGARDE DE LA BASE, si un run reussi l'attend.
# Placee ICI, avant la verification du dump, et ce n'est pas un detail d'ordre :
# si le dump est nouveau, la suite de ce script efface le volume partage et lance
# un run qui va modifier la base pendant trois jours. La sauvegarde de l'etat
# termine doit donc partir avant, pas apres.
#
# Un echec de sauvegarde n'empeche pas le lancement : perdre une sauvegarde
# hebdomadaire est moins grave que d'immobiliser la chaine derriere un script
# casse. L'echec reste visible dans ce journal, et backup-after-run.sh
# reessaiera au passage suivant puisqu'il n'ecrit son marqueur qu'en cas de
# succes.
# --------------------------------------------------------------------------
if [ -x "$STACK/backup-after-run.sh" ]; then
  if [ "$DRY_RUN" = "1" ]; then
    "$STACK/backup-after-run.sh" --dry-run
  else
    if ! "$STACK/backup-after-run.sh"; then
      echo "AVERTISSEMENT : la sauvegarde a echoue (voir ci-dessus). On continue."
    fi
  fi
else
  echo "AVERTISSEMENT : $STACK/backup-after-run.sh absent ou non executable."
  echo "Aucune sauvegarde ne sera declenchee apres les runs."
fi

# L'image doit exister pour que le guetteur tourne. Sans cache c'est long, avec
# cache c'est instantane, donc on la construit sans condition.
docker build -q -t "$IMAGE" . >/dev/null || { echo "ERREUR : construction de l'image."; exit 2; }

# 1. Le guetteur. --network=host et --env-file pour joindre la base (il lit la
#    taille du dump traite au dernier run) ; --entrypoint parce que l'image en
#    declare un autre.
docker run --rm --network=host --env-file .env \
  -v "$SHARED":/shared \
  --entrypoint python "$IMAGE" check_new_dump.py
VERDICT=$?

case "$VERDICT" in
  1) echo "Rien a faire : meme dump qu'au dernier run."; exit 0 ;;
  2) echo "Rien a faire : verification impossible (voir ci-dessus)."; exit 2 ;;
  0) : ;;
  *) echo "Rien a faire : code de retour inattendu ($VERDICT)."; exit 2 ;;
esac

BATCH="wikidata_full_$(date -u '+%Y%m%d_%H%M')"

if [ "$DRY_RUN" = "1" ]; then
  echo "--dry-run : je m'arrete ici. J'aurais fait :"
  echo "  IMPORT_BATCH_ID=$BATCH dans $STACK/.env"
  echo "  vider $SHARED, soit :"
  echo "      le dump latest-all.json.bz2 (102 Go)"
  echo "      $SHARED/pass1"
  echo "      $SHARED/pass2"
  echo "      $SHARED/item_cache"
  echo "    depuis un conteneur, parce que ces trois repertoires appartiennent a root"
  echo "  ./wikidata-crawler.sh"
  echo
  echo "  Contenu actuel :"
  ls -la "$SHARED" 2>/dev/null | tail -n +2 | sed 's/^/    /'
  exit 0
fi

# 2. Nouvel identifiant de batch. Sauvegarde datee du .env avant d'y toucher :
#    il porte les identifiants de la base, on ne le modifie pas sans filet.
cp -p .env ".env.bak.$(date -u '+%Y%m%d_%H%M%S')"
if grep -q '^IMPORT_BATCH_ID=' .env; then
  sed -i -E "s|^IMPORT_BATCH_ID=.*|IMPORT_BATCH_ID=${BATCH}|" .env
else
  printf '\nIMPORT_BATCH_ID=%s\n' "$BATCH" >> .env
fi
echo "IMPORT_BATCH_ID = $BATCH"

# 3. Table rase du volume partage : le dump perime ET les sorties des trois
#    passes, /shared/pass1, /shared/pass2 et /shared/item_cache.
#
#    L'EFFACEMENT SE FAIT DANS UN CONTENEUR, et ce n'est pas un principe, c'est une
#    necessite. Ces trois sous-repertoires sont crees par le crawler, qui tourne en
#    root : ils appartiennent donc a root. Or supprimer un fichier depend des droits
#    d'ecriture sur son REPERTOIRE parent, pas sur le fichier lui-meme. L'utilisateur
#    debian peut donc effacer le dump, pose a la racine de $SHARED qui lui appartient,
#    mais PAS le contenu de pass1, pass2 et item_cache. Un `rm -rf` cote hote laisserait
#    silencieusement les sorties des trois passes en place, et pass1 relirait
#    core_entity_ids.txt d'un run precedent.
#
#    find -mindepth 1 -delete vide sans supprimer /shared lui-meme (le point de
#    montage), et -delete implique -depth, donc le contenu part avant les repertoires.
mkdir -p "$SHARED"
echo "Effacement du volume partage (dump, pass1, pass2, item_cache) ..."
docker run --rm -v "$SHARED":/shared --entrypoint find "$IMAGE" /shared -mindepth 1 -delete
if [ $? -ne 0 ]; then
  echo "ERREUR : l'effacement a echoue. On ne lance pas : un run demarre sur des"
  echo "sorties de passes perimees produirait un resultat faux sans le signaler."
  exit 2
fi

RESTE=$(ls -A "$SHARED" 2>/dev/null | wc -l)
if [ "$RESTE" -ne 0 ]; then
  echo "ERREUR : $RESTE entree(s) subsistent dans $SHARED :"
  ls -la "$SHARED"
  exit 2
fi
echo "  volume partage vide, verifie."

# 4. Depart. wikidata-crawler.sh reconstruit l'image, lance en detache et suit
#    les journaux.
echo "Lancement du crawler (environ 3 a 4 jours) ..."
./wikidata-crawler.sh
echo "Conteneur lance. Suivre avec : docker logs -f $CONTENEUR"
echo "Les verifications horaires suivantes sortiront sans rien faire tant qu'il tourne."

