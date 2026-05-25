-- ============================================================================
-- 07_resolve_media_resources.sql
--
-- Populates the media resolution layer (V2) from already-loaded statement and
-- typed-value tables. URL-only resolution: every URL is built from a known
-- pattern, no network calls are made. The script is idempotent and safe to
-- re-run via wikidata_crawler.py --start-step 112.
--
-- Sources
--   - Wikimedia Commons          T_WC_WIKIDATA_MEDIA_VALUE
--   - YouTube         (P1651)    T_WC_WIKIDATA_EXTERNAL_ID_VALUE
--   - Internet Archive (P724)    T_WC_WIKIDATA_EXTERNAL_ID_VALUE
--
-- Targets
--   - T_WC_WIKIDATA_MEDIA_RESOURCE      (one row per (statement, platform, id))
--   - T_WC_WIKIDATA_MEDIA_RESOURCE_URL  (3 URLs/Commons, 3/YouTube, 2/IA)
--   - T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK is intentionally left untouched
--     (it stores HTTP-check history, only populated by a future check step).
--
-- Scope
--   Only statements whose ID_WIKIDATA exists in T_WC_WIKIDATA_MOVIE / SERIE /
--   PERSON (the entities the front-end actually renders).
--
-- Idempotency
--   Both target tables have UNIQUE KEYs covering the natural identity of a
--   resource / URL, so INSERT ... ON DUPLICATE KEY UPDATE handles re-runs.
--
-- Important operational note
--   Set @IMPORT_BATCH_ID before running (wikidata_crawler.py rewrites the
--   placeholder line below to the active batch id).
-- ============================================================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

SET @IMPORT_BATCH_ID = 'BATCH_20260309_001';

-- ============================================================================
-- 1. COMMONS resources
-- ============================================================================

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE (
    ID_STATEMENT,
    ID_WIKIDATA,
    ID_PROPERTY,
    SOURCE_PLATFORM,
    SOURCE_IDENTIFIER,
    SOURCE_IDENTIFIER_NORMALIZED,
    RESOURCE_KEY,
    RESOURCE_KIND,
    CONTENT_ROLE,
    CONTENT_SCOPE,
    RESOURCE_TITLE,
    MIME_TYPE_PRIMARY,
    FILE_EXTENSION_PRIMARY,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    IS_ACTIVE,
    RESOLUTION_STATUS,
    RESOLUTION_METHOD,
    LAST_RESOLVED_AT,
    LAST_SUCCESS_AT,
    IMPORT_BATCH_ID
)
SELECT
    src.ID_STATEMENT,
    src.ID_WIKIDATA,
    src.ID_PROPERTY,
    'commons',
    src.file_name,
    src.file_norm,
    CONCAT('commons:', LEFT(src.file_norm, 200), ':S', src.ID_STATEMENT),
    src.kind,
    'unknown',
    'unknown',
    src.file_name,
    NULL,
    src.ext,
    CASE WHEN src.kind IN ('video','audio') THEN 1 ELSE 0 END,
    1,
    0,
    1,
    'resolved',
    'url_pattern',
    NOW(),
    NOW(),
    @IMPORT_BATCH_ID
FROM (
    SELECT
        s.ID_STATEMENT,
        s.ID_WIKIDATA,
        s.ID_PROPERTY,
        TRIM(mv.FILE_NAME) AS file_name,
        REPLACE(TRIM(mv.FILE_NAME), ' ', '_') AS file_norm,
        LOWER(SUBSTRING_INDEX(TRIM(mv.FILE_NAME), '.', -1)) AS ext,
        CASE LOWER(SUBSTRING_INDEX(TRIM(mv.FILE_NAME), '.', -1))
            WHEN 'webm' THEN 'video'
            WHEN 'mp4'  THEN 'video'
            WHEN 'ogv'  THEN 'video'
            WHEN 'mov'  THEN 'video'
            WHEN 'mkv'  THEN 'video'
            WHEN 'wav'  THEN 'audio'
            WHEN 'mp3'  THEN 'audio'
            WHEN 'oga'  THEN 'audio'
            WHEN 'ogg'  THEN 'audio'
            WHEN 'opus' THEN 'audio'
            WHEN 'flac' THEN 'audio'
            WHEN 'pdf'  THEN 'document'
            WHEN 'djvu' THEN 'document'
            ELSE 'image'
        END AS kind
    FROM T_WC_WIKIDATA_STATEMENT s
    INNER JOIN T_WC_WIKIDATA_MEDIA_VALUE mv ON mv.ID_STATEMENT = s.ID_STATEMENT
    WHERE s.DELETED = 0
      AND (s.RANK IS NULL OR s.RANK <> 'deprecated')
      AND TRIM(IFNULL(mv.FILE_NAME, '')) <> ''
      AND (
          EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
      )
) src
ON DUPLICATE KEY UPDATE
    RESOURCE_KIND          = VALUES(RESOURCE_KIND),
    RESOURCE_TITLE         = VALUES(RESOURCE_TITLE),
    FILE_EXTENSION_PRIMARY = VALUES(FILE_EXTENSION_PRIMARY),
    IS_PLAYABLE            = VALUES(IS_PLAYABLE),
    IS_ACTIVE              = VALUES(IS_ACTIVE),
    RESOLUTION_STATUS      = VALUES(RESOLUTION_STATUS),
    RESOLUTION_METHOD      = VALUES(RESOLUTION_METHOD),
    LAST_RESOLVED_AT       = VALUES(LAST_RESOLVED_AT),
    LAST_SUCCESS_AT        = VALUES(LAST_SUCCESS_AT),
    IMPORT_BATCH_ID        = VALUES(IMPORT_BATCH_ID),
    DELETED                = 0;


-- ============================================================================
-- 2. YOUTUBE resources (P1651 = YouTube video ID)
-- ============================================================================

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE (
    ID_STATEMENT,
    ID_WIKIDATA,
    ID_PROPERTY,
    SOURCE_PLATFORM,
    SOURCE_IDENTIFIER,
    SOURCE_IDENTIFIER_NORMALIZED,
    RESOURCE_KEY,
    RESOURCE_KIND,
    CONTENT_ROLE,
    CONTENT_SCOPE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    IS_ACTIVE,
    RESOLUTION_STATUS,
    RESOLUTION_METHOD,
    LAST_RESOLVED_AT,
    LAST_SUCCESS_AT,
    IMPORT_BATCH_ID
)
SELECT
    s.ID_STATEMENT,
    s.ID_WIKIDATA,
    s.ID_PROPERTY,
    'youtube',
    TRIM(ev.VALUE_EXTERNAL_ID),
    TRIM(ev.VALUE_EXTERNAL_ID),
    CONCAT('youtube:', LEFT(TRIM(ev.VALUE_EXTERNAL_ID), 200), ':S', s.ID_STATEMENT),
    'video',
    'unknown',
    'unknown',
    1,
    0,
    1,
    1,
    'resolved',
    'url_pattern',
    NOW(),
    NOW(),
    @IMPORT_BATCH_ID
FROM T_WC_WIKIDATA_STATEMENT s
INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
WHERE s.DELETED = 0
  AND (s.RANK IS NULL OR s.RANK <> 'deprecated')
  AND s.ID_PROPERTY = 'P1651'
  AND TRIM(IFNULL(ev.VALUE_EXTERNAL_ID, '')) <> ''
  AND (
      EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
   OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
   OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
  )
ON DUPLICATE KEY UPDATE
    RESOURCE_KIND      = VALUES(RESOURCE_KIND),
    IS_PLAYABLE        = VALUES(IS_PLAYABLE),
    IS_EMBEDDABLE      = VALUES(IS_EMBEDDABLE),
    IS_ACTIVE          = VALUES(IS_ACTIVE),
    RESOLUTION_STATUS  = VALUES(RESOLUTION_STATUS),
    RESOLUTION_METHOD  = VALUES(RESOLUTION_METHOD),
    LAST_RESOLVED_AT   = VALUES(LAST_RESOLVED_AT),
    LAST_SUCCESS_AT    = VALUES(LAST_SUCCESS_AT),
    IMPORT_BATCH_ID    = VALUES(IMPORT_BATCH_ID),
    DELETED            = 0;


-- ============================================================================
-- 3. INTERNET ARCHIVE resources (P724 = Internet Archive ID)
-- ============================================================================

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE (
    ID_STATEMENT,
    ID_WIKIDATA,
    ID_PROPERTY,
    SOURCE_PLATFORM,
    SOURCE_IDENTIFIER,
    SOURCE_IDENTIFIER_NORMALIZED,
    RESOURCE_KEY,
    RESOURCE_KIND,
    CONTENT_ROLE,
    CONTENT_SCOPE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    IS_ACTIVE,
    RESOLUTION_STATUS,
    RESOLUTION_METHOD,
    LAST_RESOLVED_AT,
    LAST_SUCCESS_AT,
    IMPORT_BATCH_ID
)
SELECT
    s.ID_STATEMENT,
    s.ID_WIKIDATA,
    s.ID_PROPERTY,
    'internet_archive',
    TRIM(ev.VALUE_EXTERNAL_ID),
    TRIM(ev.VALUE_EXTERNAL_ID),
    CONCAT('internet_archive:', LEFT(TRIM(ev.VALUE_EXTERNAL_ID), 200), ':S', s.ID_STATEMENT),
    'video',
    'unknown',
    'unknown',
    1,
    1,
    1,
    1,
    'resolved',
    'url_pattern',
    NOW(),
    NOW(),
    @IMPORT_BATCH_ID
FROM T_WC_WIKIDATA_STATEMENT s
INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
WHERE s.DELETED = 0
  AND (s.RANK IS NULL OR s.RANK <> 'deprecated')
  AND s.ID_PROPERTY = 'P724'
  AND TRIM(IFNULL(ev.VALUE_EXTERNAL_ID, '')) <> ''
  AND (
      EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
   OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
   OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA AND m.DELETED = 0)
  )
ON DUPLICATE KEY UPDATE
    RESOURCE_KIND      = VALUES(RESOURCE_KIND),
    IS_PLAYABLE        = VALUES(IS_PLAYABLE),
    IS_DOWNLOADABLE    = VALUES(IS_DOWNLOADABLE),
    IS_EMBEDDABLE      = VALUES(IS_EMBEDDABLE),
    IS_ACTIVE          = VALUES(IS_ACTIVE),
    RESOLUTION_STATUS  = VALUES(RESOLUTION_STATUS),
    RESOLUTION_METHOD  = VALUES(RESOLUTION_METHOD),
    LAST_RESOLVED_AT   = VALUES(LAST_RESOLVED_AT),
    LAST_SUCCESS_AT    = VALUES(LAST_SUCCESS_AT),
    IMPORT_BATCH_ID    = VALUES(IMPORT_BATCH_ID),
    DELETED            = 0;


-- ============================================================================
-- 4. COMMONS URL variants (page, thumbnail, file)
-- ============================================================================

-- Commons file page (HTML page on commons.wikimedia.org)
INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'page',
    src.url,
    src.url,
    SHA2(src.url, 256),
    1,
    1,
    0,
    1,
    0,
    0,
    0,
    1
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://commons.wikimedia.org/wiki/File:',
               REPLACE(TRIM(mr.SOURCE_IDENTIFIER), ' ', '_')) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'commons'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    IS_CANONICAL     = VALUES(IS_CANONICAL),
    IS_PREFERRED     = VALUES(IS_PREFERRED),
    IS_ACTIVE        = 1,
    DELETED          = 0;

-- Commons thumbnail (Special:FilePath gives an auto-resized JPEG)
INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER,
    MIME_TYPE
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'thumbnail',
    src.url,
    src.url,
    SHA2(src.url, 256),
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    2,
    'image/jpeg'
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://commons.wikimedia.org/wiki/Special:FilePath/',
               REPLACE(TRIM(mr.SOURCE_IDENTIFIER), ' ', '_'),
               '?width=400') AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'commons'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    MIME_TYPE        = VALUES(MIME_TYPE),
    IS_ACTIVE        = 1,
    DELETED          = 0;

-- Commons direct file (upload.wikimedia.org; path uses MD5 of normalized filename)
INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER,
    FILE_EXTENSION
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'file',
    src.url,
    src.url,
    SHA2(src.url, 256),
    0,
    0,
    1,
    1,
    src.is_playable,
    1,
    0,
    3,
    src.ext
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        mr.FILE_EXTENSION_PRIMARY AS ext,
        CASE WHEN mr.RESOURCE_KIND IN ('video','audio') THEN 1 ELSE 0 END AS is_playable,
        CONCAT('https://upload.wikimedia.org/wikipedia/commons/',
               SUBSTRING(MD5(REPLACE(TRIM(mr.SOURCE_IDENTIFIER), ' ', '_')), 1, 1),
               '/',
               SUBSTRING(MD5(REPLACE(TRIM(mr.SOURCE_IDENTIFIER), ' ', '_')), 1, 2),
               '/',
               REPLACE(TRIM(mr.SOURCE_IDENTIFIER), ' ', '_')) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'commons'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    FILE_EXTENSION   = VALUES(FILE_EXTENSION),
    IS_DIRECT_FILE   = VALUES(IS_DIRECT_FILE),
    IS_PLAYABLE      = VALUES(IS_PLAYABLE),
    IS_DOWNLOADABLE  = VALUES(IS_DOWNLOADABLE),
    IS_ACTIVE        = 1,
    DELETED          = 0;


-- ============================================================================
-- 5. YOUTUBE URL variants (watch, embed, thumbnail)
-- ============================================================================

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'watch',
    src.url,
    src.url,
    SHA2(src.url, 256),
    1,
    1,
    0,
    1,
    1,
    0,
    0,
    1
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://www.youtube.com/watch?v=', TRIM(mr.SOURCE_IDENTIFIER)) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'youtube'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    IS_CANONICAL     = VALUES(IS_CANONICAL),
    IS_PREFERRED     = VALUES(IS_PREFERRED),
    IS_PLAYABLE      = VALUES(IS_PLAYABLE),
    IS_ACTIVE        = 1,
    DELETED          = 0;

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'embed',
    src.url,
    src.url,
    SHA2(src.url, 256),
    0,
    0,
    0,
    1,
    1,
    0,
    1,
    2
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://www.youtube.com/embed/', TRIM(mr.SOURCE_IDENTIFIER)) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'youtube'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    IS_PLAYABLE      = VALUES(IS_PLAYABLE),
    IS_EMBEDDABLE    = VALUES(IS_EMBEDDABLE),
    IS_ACTIVE        = 1,
    DELETED          = 0;

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER,
    MIME_TYPE
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'thumbnail',
    src.url,
    src.url,
    SHA2(src.url, 256),
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    3,
    'image/jpeg'
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://i.ytimg.com/vi/', TRIM(mr.SOURCE_IDENTIFIER), '/hqdefault.jpg') AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'youtube'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    MIME_TYPE        = VALUES(MIME_TYPE),
    IS_ACTIVE        = 1,
    DELETED          = 0;


-- ============================================================================
-- 6. INTERNET ARCHIVE URL variants (page, file)
-- ============================================================================

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'page',
    src.url,
    src.url,
    SHA2(src.url, 256),
    1,
    1,
    0,
    1,
    1,
    0,
    0,
    1
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://archive.org/details/', TRIM(mr.SOURCE_IDENTIFIER)) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'internet_archive'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    IS_CANONICAL     = VALUES(IS_CANONICAL),
    IS_PREFERRED     = VALUES(IS_PREFERRED),
    IS_PLAYABLE      = VALUES(IS_PLAYABLE),
    IS_ACTIVE        = 1,
    DELETED          = 0;

INSERT INTO T_WC_WIKIDATA_MEDIA_RESOURCE_URL (
    ID_MEDIA_RESOURCE,
    URL_TYPE,
    URL,
    URL_NORMALIZED,
    URL_HASH,
    IS_CANONICAL,
    IS_PREFERRED,
    IS_DIRECT_FILE,
    IS_ACTIVE,
    IS_PLAYABLE,
    IS_DOWNLOADABLE,
    IS_EMBEDDABLE,
    DISPLAY_ORDER
)
SELECT
    src.ID_MEDIA_RESOURCE,
    'file',
    src.url,
    src.url,
    SHA2(src.url, 256),
    0,
    0,
    1,
    1,
    0,
    1,
    0,
    2
FROM (
    SELECT
        mr.ID_MEDIA_RESOURCE,
        CONCAT('https://archive.org/download/', TRIM(mr.SOURCE_IDENTIFIER)) AS url
    FROM T_WC_WIKIDATA_MEDIA_RESOURCE mr
    WHERE mr.SOURCE_PLATFORM = 'internet_archive'
      AND mr.DELETED = 0
) src
ON DUPLICATE KEY UPDATE
    URL              = VALUES(URL),
    URL_NORMALIZED   = VALUES(URL_NORMALIZED),
    IS_DIRECT_FILE   = VALUES(IS_DIRECT_FILE),
    IS_DOWNLOADABLE  = VALUES(IS_DOWNLOADABLE),
    IS_ACTIVE        = 1,
    DELETED          = 0;
