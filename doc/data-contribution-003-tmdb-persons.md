# DATA-CONTRIBUTION-003: Wikidata persons completed on TMDb

Operation log and procedure for the TMDb contributions drawn from the Wikidata base (ticket DATA-CONTRIBUTION-003 in the Nestor backlog, `projets/t2s-backlog/topics/data-contribution.md`). Goal: earn TMDb badges by adding data TMDb lacks, without ever creating a person who already exists on TMDb.

Everything below is reproducible from files in this repository. The TMDb API token comes from `tmdb-crawler/.env` (`TMDB_API_TOKEN`) and is never printed or committed.

## 1. Pipeline

| Step | File (in `doc/sql/`) | Kind | Result |
|---|---|---|---|
| 1. Candidates v1 | `data-contribution-003-wikidata-without-tmdb.sql` | read-only SQL, VPS gate | `-20261104.txt` |
| 2. Candidates v2 | `data-contribution-003-wikidata-without-tmdb-v2.sql` | read-only SQL, VPS gate | `-v2-20261105.txt` |
| 3. Person worksheet (top 50) | `data-contribution-003-person-worksheet.sql` | read-only SQL, VPS gate | `-20261105.txt` |
| 4. Check against live TMDb | `data-contribution-003-tmdb-person-check.py` | read-only TMDb API | `archive/data-contribution-003-tmdb-person-check-20261105.json` |
| 5. Pre-check of each id to write | `data-contribution-003-tmdb-id-precheck.py` | read-only TMDb API | `archive/data-contribution-003-tmdb-id-precheck-20261105.json` |
| 6. Entry on TMDb | Chrome, by hand or Claude in Chrome (section 4) | write on themoviedb.org | 36 persons, section 5 |
| 7. Post-check | edit form re-read in Chrome (section 4) | read-only | `{"ok": 36, "bad": []}` |

SQL files run through the agent gate: `ssh vps-agent pull wikidata-crawler`, then `ssh vps-agent run wikidata-crawler/doc/sql/<file>.sql`; the output after the `=====` line is saved next to the `.sql` as `<file>-YYYYMMDD.txt`. Run them when no text2sql-eval campaign is running (v2 takes about 20 minutes on the VPS).

## 2. Selection queries

**v1** lists Wikidata movies, series and persons that have an IMDb id (P345), no TMDb id on Wikidata (P4947 movie, P4983 series, P4985 person), and are unknown to the local TMDb copy both by QID and by IMDb id. It also lists the reverse case (Wikidata items missing a TMDb id that TMDb already has): Wikidata write-backs, not TMDb creations. Its ranking by number of label languages proved useless (bots copy one label to hundreds of languages).

**v2** adds, per family, a `BASE` then a `CAND` temporary table with filter flags, and counts what each filter drops:

- movies: IMDb id starting with `tt`; P31 not a TV class (Q21191270, Q1983062, Q5398426, Q63952888, Q1259759, Q526877, Q581714); not part (P179) of a known series; absent from the series and episode tables;
- series: `tt` id; P31 not an episode or ceremony (Q4504495, Q21191270, Q1983062);
- persons: `nm` id; a film occupation (P106) or at least one credit on Wikidata; credits counted through the reverse lookup on `ITEM_VALUE.ID_ITEM` over P161, P725, P57, P58, P86, P162, P1431, P344, P1040, P2554, P2515, and among them the credits on works the TMDb copy has (`NB_CREDITS_ON_TMDB`);
- ranking: number of external ids, then number of statements; persons by `NB_CREDITS_ON_TMDB`.

v2 result: 55,793 movies, 16,599 series, 131,927 persons (60,590 credited on a TMDb work).

**Worksheet** freezes the top 50 QIDs of v2 section H (persons) so the list does not move while it is worked on. Section P gives the TMDb form data (name, gender, birth and death dates at Wikidata precision, place of birth, occupations, description, TMDb search link). Section C gives 2,830 credits: TMDb type and id, title, year, TMDb department and job mapped from the Wikidata property, character (P453).

## 3. Checks before any write

The local MariaDB copy of TMDb lags behind TMDb and matches only on the IMDb id. A person "absent" from it is very often present on TMDb without an IMDb id. Existence is therefore always decided on the live TMDb API, never on the copy (Philippe's rule, 2026-11-05; his own tool for it is `https://www.vaugouin.com/tmdb/person-search.php`).

`data-contribution-003-tmdb-person-check.py` (step 4) classifies each worksheet person:

- `/find/{imdb}?external_source=imdb_id`; if nothing, `/search/person` by name, and a candidate is kept only if its `/person/{id}/combined_credits` overlap the worksheet credits;
- statuses: `present_with_imdb`, `present_without_imdb`, `absent`;
- it also lists the worksheet credits missing on TMDb (725 for the 50 persons).

Result on the top 50: 35 `present_without_imdb`, 14 `present_with_imdb`, 1 `absent` (Oswald Stoll, nm0831524, Q7108170). G. Devarajan (rank 1) was counted `present_with_imdb` with an empty TMDb IMDb id; Philippe identified his record (TMDb 1122114) with person-search.php.

`data-contribution-003-tmdb-id-precheck.py` (step 5), on the 35 `present_without_imdb`, is the gate for each edit. An edit is written only if all four hold:

1. the IMDb id is on no TMDb person (`/find` by `imdb_id` returns nothing);
2. the QID is on no TMDb person (`/find` by `wikidata_id` returns nothing);
3. the target person has no IMDb id and no Wikidata id yet (`/person/{id}/external_ids`);
4. the target shares credits with the Wikidata person (between 3 and 70 here, column `shared`).

All 35 passed. Felix Bartsch (3 shared credits) and Jatinder Shah (15) are the weakest matches.

## 4. Kind of update and Chrome procedure

**Only one kind of update was made:** filling the empty IMDb id and Wikidata id fields of an existing TMDb person, on the External IDs tab of the edit form. No person was created, no field already filled was changed, no credit, date or biography was touched.

Procedure per person, in Chrome logged in as `vaugouin` (Claude in Chrome extension, JavaScript tool; the same steps work by hand):

1. Open `https://www.themoviedb.org/person/<tmdb_id>/edit?active_nav_item=external_ids`.
2. Read `#imdb_id` and `#wikidata_id`; stop if either is not empty.
3. Focus `#imdb_id` with JavaScript (`document.querySelector('#imdb_id').focus()`), then type the `nm` id with the keyboard tool. Do the same for `#wikidata_id` with the QID. Typing (rather than setting `.value`) is what makes the form register the change.
4. Click Save: `[...document.querySelectorAll('button,input[type=submit]')].find(b => /^\s*save\s*$/i.test(b.innerText || b.value)).click()`.
5. Post-check: fetch the edit page again (`fetch(url).then(r => r.text())`), parse it with `DOMParser`, and compare `#imdb_id` and `#wikidata_id` to the expected values.

The TMDb API shows a saved id only after a cache delay (Devarajan still appeared without ids just after his save): post-check on the edit form, not the API.

Saves: Devarajan by Philippe, the other 35 by Claude in Chrome on 2026-11-05, with Philippe's explicit go-ahead in the session. Final post-check over the 36 records: `{"ok": 36, "bad": []}`.

## 5. Persons completed on TMDb (2026-11-05)

| Rank | Name | TMDb | IMDb | Wikidata | Shared credits |
|---|---|---|---|---|---|
| 1 | G. Devarajan | 1122114 | nm0222219 | Q5512207 | 102 |
| 4 | Anand Raj Anand | 1039576 | nm0025607 | Q4751271 | 32 |
| 5 | Ricardo Conord | 1815972 | nm0175570 | Q6108101 | 70 |
| 6 | Hareesh Perumanna | 1608615 | nm7075397 | Q28070985 | 68 |
| 7 | A. T. Ummer | 1167695 | nm0880899 | Q13110599 | 39 |
| 9 | M. K. Arjunan | 1199677 | nm0035021 | Q2053416 | 37 |
| 10 | Burton L. King | 1153824 | nm0454535 | Q2928642 | 62 |
| 11 | Praveen K. L. | 222845 | nm3164715 | Q7238852 | 54 |
| 12 | Mario Savino | 1819306 | nm0767759 | Q16599878 | 51 |
| 13 | Cayetano Biondo | 1544183 | nm0083178 | Q5055301 | 50 |
| 14 | Alencier Ley Lopez | 1574335 | nm4425773 | Q26251134 | 60 |
| 16 | Alfonso del Real | 1605277 | nm0713999 | Q5667396 | 55 |
| 17 | Rafael Diserio | 1775328 | nm0228315 | Q16624098 | 43 |
| 18 | Vijayakumar | 1501423 | nm8326844 | Q21067208 | 45 |
| 19 | Kottarakkara Sreedharan Nair | 1337566 | nm0619758 | Q6434301 | 37 |
| 20 | Francis Boeniger | 1713738 | nm0091291 | Q19903994 | 52 |
| 22 | Pedro Quartucci | 1791674 | nm0702957 | Q2417167 | 50 |
| 23 | Álvaro Durañona y Vedia | 1813631 | nm0243938 | Q6172768 | 40 |
| 26 | Jayiram Samal | 3877549 | nm3374348 | Q2792151 | 22 |
| 27 | Mikhail Meerovich | 1587508 | nm0583468 | Q4288040 | 35 |
| 28 | Miguel Gómez Bao | 1586413 | nm0350861 | Q6014454 | 47 |
| 29 | Aparajita Mohanty | 1956712 | nm3382815 | Q4779123 | 25 |
| 31 | Francisco Audenino | 1819863 | nm0041540 | Q5865167 | 37 |
| 32 | Dina Perbellini | 1021648 | nm0672809 | Q3707818 | 35 |
| 33 | Lucio Demare | 1722292 | nm0218120 | Q11689767 | 43 |
| 36 | Gunnar Sønstevold | 1635713 | nm0845652 | Q155202 | 32 |
| 37 | Jatinder Shah | 1810701 | nm5035495 | Q23771900 | 15 |
| 40 | María Santos | 1916590 | nm0764202 | Q6004557 | 36 |
| 42 | Felix Bartsch | 1758839 | nm0059341 | Q23581929 | 3 |
| 43 | Lalo Malcolm | 1917390 | nm0539118 | Q5969631 | 35 |
| 45 | E.D. Horkheimer | 4922406 | nm0394742 | Q15117297 | 13 |
| 46 | Ghantasala Venkateswara Rao | 1139299 | nm0710550 | Q3429974 | 24 |
| 47 | Rodolfo Onetto | 1813618 | nm0648617 | Q7357181 | 37 |
| 48 | Kiran Kumar | 232828 | nm0474820 | Q6414811 | 26 |
| 49 | Arindam Sil | 1377009 | nm1908800 | Q4790711 | 38 |
| 50 | Aurelia Ferrer | 1886056 | nm0274534 | Q10857806 | 36 |

## 6. Left open (nothing written)

- **IMDb id differs between TMDb and Wikidata** for 13 persons of the top 50. Compare the IMDb pages one by one before correcting either base:

| Rank | Name | TMDb | IMDb on TMDb | IMDb on Wikidata | Wikidata |
|---|---|---|---|---|---|
| 2 | José Sacristán | 105217 | nm0755366 | nm0755364 | Q2269625 |
| 3 | Gautam Raju | 1106924 | nm1179074 | nm0707492 | Q26878090 |
| 8 | Viggo Larsen | 1172452 | nm0488933 | nm0488932 | Q731703 |
| 15 | Enzo Maggio | 1169660 | nm5037444 | nm0535982 | Q3726570 |
| 21 | Vijaya Bhaskar | 585265 | nm6457449 | nm6168318 | Q7929224 |
| 24 | Mohan | 584359 | nm2061768 | nm0595897 | Q6893516 |
| 25 | Francisco Álvarez | 1452586 | nm1643711 | nm0959433 | Q8962663 |
| 30 | Jan Frycz | 46246 | nm0296810 | nm0296809 | Q55374 |
| 35 | Rony David | 1389740 | nm5445536 | nm4236361 | Q37994724 |
| 38 | Kozhikode Narayanan Nair | 1296865 | nm5000970 | nm1985564 | Q16200798 |
| 39 | Marguerite Viby | 563753 | nm0469802 | nm0895825 | Q4984538 |
| 41 | Thomas Bentley | 106967 | nm0072873 | nm0072872 | Q7787584 |
| 44 | Tom Walls | 64121 | nm0906470 | nm0909398 | Q3530914 |

  Viggo Larsen (TMDb Wikidata id Q38052538) and Marguerite Viby (Q5922059) also carry on TMDb a QID other than the worksheet's: possibly a homonym on one side.
- **Possible IMDb duplicate for Devarajan:** nm0222219 ("Paravur Devarajan", the id Wikidata gives and the one entered) and nm16830145 ("G. Devarajan").
- **Oswald Stoll** (nm0831524, Q7108170) is absent from TMDb: the only creation candidate of the top 50.
- **Missing credits:** 725 worksheet credits absent from TMDb for the 50 persons (92 for Devarajan), listed in the step 4 JSON (`missing`).
- **Real creations:** run steps 3 and 4 on a larger slice of v2 (top 500) to find persons truly absent from TMDb.
