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
#   1. verifie, dans un conteneur jetable, sans telecharger un octet ;
#   2. ecrit un nouvel IMPORT_BATCH_ID dans .env (l'ancien est sauvegarde) ;
#   3. vide /home/debian/docker/shared_data/wikidata-crawler ;
#   4. lance ./wikidata-crawler.sh, qui reconstruit l'image et part sur 101.
#
# USAGE
#   ./run-if-new-dump.sh              # verifie, et lance s'il y a du neuf
#   ./run-if-new-dump.sh --dry-run    # verifie et dit ce qu'il ferait, sans agir
#
# CRON, tous les jours a 8h : ne fait rien les six jours ou il n'y a rien a faire.
#   0 8 * * * /home/debian/docker/wikidata-crawler/run-if-new-dump.sh >> \
#             /home/debian/docker/wikidata-crawler/run-if-new-dump.log 2>&1
# =============================================================================

set -uo pipefail

STACK=/home/debian/docker/wikidata-crawler
SHARED=/home/debian/docker/shared_data/wikidata-crawler
IMAGE=wikidata-crawler-python-app

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') : verification du dump Wikidata ==="
cd "$STACK" || { echo "ERREUR : $STACK introuvable."; exit 2; }

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
exec ./wikidata-crawler.sh
