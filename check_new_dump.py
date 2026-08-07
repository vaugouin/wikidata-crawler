#!/usr/bin/env python3
"""Dit si Wikimedia a publie un nouveau dump, AVANT de lancer quoi que ce soit.

POURQUOI CE SCRIPT EXISTE. Le dump JSON complet de Wikidata demarre le lundi mais
met environ QUATRE JOURS a se generer : le cycle du 2026-08-03 n'a ete disponible
que le 2026-08-07 a 03:57 UTC. Or `latest-all.json.bz2` n'est rafraichi qu'a la
fin de cette generation. Lancer le crawler entre les deux re-telecharge le dump
PRECEDENT, sous un nouvel identifiant de batch, et re-ingere des donnees deja
presentes.

C'est arrive le 2026-08-03 : 3 jours 18 heures de VPS pour un resultat
rigoureusement identique, 120 986 268 entites et 35 122 018 statements au chiffre
pres. Rien dans les journaux ne le signalait, puisque le run etait un succes.

La procedure de lancement commence par supprimer le fichier local, donc au moment
ou l'on se pose la question il est deja trop tard pour comparer. D'ou ce guetteur,
a lancer AVANT : il interroge le serveur en HEAD, sans rien telecharger, et compare
la taille annoncee a celle du dump reellement traite au dernier run (variable
serveur `strwikidatacrawlerdumpsize`, ecrite par l'etape 101).

La taille est le discriminant : deux dumps consecutifs different de plusieurs
centaines de mega-octets (102 354 154 676 contre 102 577 987 071 en aout 2026).

USAGE
    python check_new_dump.py                 # verdict lisible
    python check_new_dump.py --quiet         # rien sur la sortie, juste le code retour

CODES DE RETOUR, pour un cron ou un `&&`
    0  nouveau dump disponible, le crawler peut partir
    1  meme dump qu'au dernier run, ne pas lancer
    2  impossible de conclure (serveur injoignable, aucune reference enregistree)

EXEMPLE DE CRON, tous les jours a 8h, qui ne lance que s'il y a du neuf :
    0 8 * * * cd /home/debian/docker/wikidata-crawler && python3 check_new_dump.py --quiet \
              && ./wikidata-crawler.sh
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import citizenphil as cp  # noqa: E402

CRAWLER_PREFIX = "strwikidatacrawler"
DEFAULT_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2"


def human_go(octets: int) -> str:
    return f"{octets / 1_000_000_000:.2f} Go"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--url", default=os.environ.get("DUMP_URL") or DEFAULT_URL,
                        help="URL du dump a surveiller (defaut: DUMP_URL du .env, sinon latest-all.json.bz2)")
    parser.add_argument("--dump-file", default=os.environ.get("DUMP_FILE", "/shared/latest-all.json.bz2"),
                        help="chemin du dump local, utilise comme reference tant que la variable serveur est vide")
    parser.add_argument("--quiet", action="store_true", help="ne rien afficher, ne rendre que le code retour")
    args = parser.parse_args()

    def say(*a):
        if not args.quiet:
            print(*a)

    # Ce que le serveur propose aujourd'hui. HEAD : aucun octet de dump transfere.
    # Wikimedia repond 403 aux agents utilisateurs par defaut des bibliotheques :
    # on envoie le meme en-tete descriptif que le telechargement de l'etape 101
    # (wikidata_crawler.py:158), faute de quoi ce script conclurait toujours
    # "impossible de conclure" sans qu'on comprenne pourquoi.
    user_agent = os.environ.get("WIKIMEDIA_USER_AGENT", "").strip() or "python-httpx"
    try:
        response = httpx.head(args.url, follow_redirects=True, timeout=30.0,
                              headers={"User-Agent": user_agent})
        response.raise_for_status()
        taille_distante = int(response.headers.get("content-length", 0))
        modifie_le = response.headers.get("last-modified", "")
    except httpx.HTTPStatusError as exc:
        say(f"IMPOSSIBLE DE CONCLURE : le serveur repond {exc.response.status_code}.")
        if exc.response.status_code == 403:
            say("  403 = agent utilisateur refuse. Wikimedia exige un en-tete descriptif :")
            say("  renseigner WIKIMEDIA_USER_AGENT dans le .env, par exemple")
            say("  'wikidata-crawler/1.0 (adresse de contact)'. Verifie : 'python-httpx' est")
            say("  refuse, un agent nomme passe.")
        return 2
    except Exception as exc:
        say(f"IMPOSSIBLE DE CONCLURE : le serveur ne repond pas ({exc}).")
        return 2

    if not taille_distante:
        say("IMPOSSIBLE DE CONCLURE : le serveur n'annonce pas de taille.")
        return 2

    age = ""
    if modifie_le:
        try:
            publie = parsedate_to_datetime(modifie_le)
            jours = (datetime.now(timezone.utc) - publie).days
            age = f", publie il y a {jours} jour(s)"
        except Exception:
            pass

    say(f"Dump propose par le serveur : {human_go(taille_distante)} ({taille_distante} octets)")
    say(f"  publie le                 : {modifie_le or 'inconnu'}{age}")

    # Ce que le dernier run a reellement traite. La variable serveur est la
    # reference : elle survit a l'effacement du volume partage, qui est la premiere
    # etape de la procedure de lancement.
    reference = (cp.f_getservervariable(f"{CRAWLER_PREFIX}dumpsize", 0) or "").strip()
    origine = "dernier run traite"

    # Repli tant que la variable n'existe pas : le fichier encore sur le disque.
    # Elle ne sera ecrite qu'a la prochaine etape 101, or la comparaison est utile
    # des maintenant. Ce repli disparait de lui-meme au premier run.
    if not reference.isdigit() and args.dump_file:
        chemin = os.path.expanduser(args.dump_file)
        if os.path.isfile(chemin):
            reference = str(os.path.getsize(chemin))
            origine = "fichier local (variable serveur pas encore ecrite)"

    if not reference.isdigit():
        say("  dernier run               : aucune reference, ni en base ni sur disque")
        say("")
        say("IMPOSSIBLE DE CONCLURE : ni la variable serveur "
            f"{CRAWLER_PREFIX}dumpsize ni le fichier")
        say("local n'existent. La variable sera ecrite par la prochaine etape 101.")
        return 2

    taille_traitee = int(reference)
    say(f"  {origine:25} : {human_go(taille_traitee)} ({taille_traitee} octets)")
    say("")

    if taille_distante == taille_traitee:
        say("MEME DUMP QU'AU DERNIER RUN. Ne pas lancer : le crawler re-ingererait a")
        say("l'identique, pour 3 a 4 jours de machine. Le cycle hebdomadaire demarre le")
        say("lundi mais n'aboutit que ~4 jours plus tard ; reessayer en fin de semaine.")
        return 1

    ecart = taille_distante - taille_traitee
    signe = "+" if ecart > 0 else ""
    say(f"NOUVEAU DUMP DISPONIBLE ({signe}{ecart / 1_000_000:.0f} Mo par rapport au dernier traite).")
    say("Le crawler peut partir : supprimer le fichier local puis lancer wikidata-crawler.sh.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
