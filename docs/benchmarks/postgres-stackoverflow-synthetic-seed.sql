\timing on
SET statement_timeout = 0;
SET maintenance_work_mem = '512MB';
SET work_mem = '128MB';

DROP SCHEMA IF EXISTS so_pg_src CASCADE;
CREATE SCHEMA so_pg_src;

CREATE UNLOGGED TABLE so_pg_src._payloads AS
SELECT
  g::int AS id,
  md5(g::text || ':01') || md5(g::text || ':02') || md5(g::text || ':03') || md5(g::text || ':04') ||
  md5(g::text || ':05') || md5(g::text || ':06') || md5(g::text || ':07') || md5(g::text || ':08') ||
  md5(g::text || ':09') || md5(g::text || ':10') || md5(g::text || ':11') || md5(g::text || ':12') ||
  md5(g::text || ':13') || md5(g::text || ':14') || md5(g::text || ':15') || md5(g::text || ':16') AS body_512,
  md5(g::text || ':17') || md5(g::text || ':18') || md5(g::text || ':19') || md5(g::text || ':20') ||
  md5(g::text || ':21') || md5(g::text || ':22') || md5(g::text || ':23') || md5(g::text || ':24') AS body_256
FROM generate_series(1, 1000000) AS g;
ALTER TABLE so_pg_src._payloads ADD PRIMARY KEY (id);
ANALYZE so_pg_src._payloads;

CREATE UNLOGGED TABLE so_pg_src.users AS
SELECT
  g::int AS id,
  (random() * 100000)::int AS reputation,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval AS creation_date,
  'User ' || g AS display_name,
  'City ' || (g % 5000) AS location,
  p.body_256 AS about_me,
  (random() * 100000)::int AS views,
  (random() * 10000)::int AS up_votes,
  (random() * 1000)::int AS down_votes
FROM generate_series(1, 1000000) AS g
JOIN so_pg_src._payloads p ON p.id = g;

CREATE UNLOGGED TABLE so_pg_src.post_types AS
SELECT * FROM (VALUES
  (1, 'Question'), (2, 'Answer'), (3, 'Wiki'), (4, 'TagWikiExcerpt'), (5, 'TagWiki')
) AS t(id, name);

CREATE UNLOGGED TABLE so_pg_src.vote_types AS
SELECT * FROM (VALUES
  (1, 'AcceptedByOriginator'), (2, 'UpMod'), (3, 'DownMod'), (4, 'Offensive'),
  (5, 'Favorite'), (6, 'Close'), (7, 'Reopen'), (8, 'BountyStart'), (9, 'BountyClose')
) AS t(id, name);

CREATE UNLOGGED TABLE so_pg_src.link_types AS
SELECT * FROM (VALUES
  (1, 'Linked'), (3, 'Duplicate')
) AS t(id, name);

CREATE UNLOGGED TABLE so_pg_src.posts AS
SELECT
  g::int AS id,
  CASE WHEN g % 5 = 0 THEN 1 ELSE 2 END AS post_type_id,
  CASE WHEN g % 5 = 0 THEN (g + 1)::int ELSE NULL::int END AS accepted_answer_id,
  CASE WHEN g % 5 = 0 THEN NULL::int ELSE ((g - 1) / 5 * 5 + 1)::int END AS parent_id,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval + ((g % 86400) || ' seconds')::interval AS creation_date,
  ((g % 200) - 20)::int AS score,
  CASE WHEN g % 5 = 0 THEN (100 + (g % 100000))::int ELSE NULL::int END AS view_count,
  p.body_512 AS body,
  ((g - 1) % 1000000 + 1)::int AS owner_user_id,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval + (((g * 7) % 86400) || ' seconds')::interval AS last_activity_date,
  CASE WHEN g % 5 = 0 THEN 'How do I solve synthetic problem #' || g ELSE NULL END AS title,
  CASE WHEN g % 5 = 0 THEN '<postgresql><airflow><dlt><benchmark>' ELSE NULL END AS tags,
  CASE WHEN g % 5 = 0 THEN (g % 12)::int ELSE NULL::int END AS answer_count,
  (g % 20)::int AS comment_count,
  CASE WHEN g % 5 = 0 THEN (g % 100)::int ELSE NULL::int END AS favorite_count
FROM generate_series(1, 8000000) AS g
JOIN so_pg_src._payloads p ON p.id = ((g - 1) % 1000000 + 1);

CREATE UNLOGGED TABLE so_pg_src.comments AS
SELECT
  g::int AS id,
  ((g - 1) % 8000000 + 1)::int AS post_id,
  (g % 25)::int AS score,
  p.body_256 AS text,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval + (((g * 11) % 86400) || ' seconds')::interval AS creation_date,
  ((g - 1) % 1000000 + 1)::int AS user_id
FROM generate_series(1, 12000000) AS g
JOIN so_pg_src._payloads p ON p.id = ((g - 1) % 1000000 + 1);

CREATE UNLOGGED TABLE so_pg_src.votes AS
SELECT
  g::int AS id,
  ((g - 1) % 8000000 + 1)::int AS post_id,
  ((g - 1) % 9 + 1)::int AS vote_type_id,
  CASE WHEN g % 4 = 0 THEN NULL::int ELSE ((g - 1) % 1000000 + 1)::int END AS user_id,
  date '2008-07-31' + ((g % 5200) || ' days')::interval AS creation_date,
  CASE WHEN g % 97 = 0 THEN (50 + (g % 500))::int ELSE NULL::int END AS bounty_amount
FROM generate_series(1, 20000000) AS g;

CREATE UNLOGGED TABLE so_pg_src.badges AS
SELECT
  g::int AS id,
  ((g - 1) % 1000000 + 1)::int AS user_id,
  'Badge ' || (g % 250) AS name,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval AS date,
  ((g - 1) % 3 + 1)::int AS class,
  (g % 7 = 0) AS tag_based
FROM generate_series(1, 3000000) AS g;

CREATE UNLOGGED TABLE so_pg_src.post_links AS
SELECT
  g::int AS id,
  timestamp '2008-07-31' + ((g % 5200) || ' days')::interval AS creation_date,
  ((g - 1) % 8000000 + 1)::int AS post_id,
  ((g * 17 - 1) % 8000000 + 1)::int AS related_post_id,
  CASE WHEN g % 11 = 0 THEN 3 ELSE 1 END AS link_type_id
FROM generate_series(1, 1000000) AS g;

DROP TABLE so_pg_src._payloads;

ANALYZE so_pg_src.users;
ANALYZE so_pg_src.posts;
ANALYZE so_pg_src.comments;
ANALYZE so_pg_src.votes;
ANALYZE so_pg_src.badges;
ANALYZE so_pg_src.post_links;
ANALYZE so_pg_src.post_types;
ANALYZE so_pg_src.vote_types;
ANALYZE so_pg_src.link_types;

SELECT
  n.nspname AS schema,
  pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total_size,
  sum(pg_total_relation_size(c.oid)) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'so_pg_src'
  AND c.relkind IN ('r', 'm')
GROUP BY n.nspname;

SELECT
  c.relname,
  pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
  c.reltuples::bigint AS estimated_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'so_pg_src'
  AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC;
