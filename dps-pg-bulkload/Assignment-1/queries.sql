DROP TABLE IF EXISTS query1;
CREATE TABLE query1 AS
SELECT COUNT(*)::bigint AS "count of comments"
FROM comments
WHERE author = 'xymemez';

DROP TABLE IF EXISTS query2;
CREATE TABLE query2 AS
SELECT
  subreddit_type AS "subreddit type",
  COUNT(*)::bigint AS "subreddit count"
FROM subreddits
GROUP BY subreddit_type;

DROP TABLE IF EXISTS query3;
CREATE TABLE query3 AS
SELECT
  s.name AS "name",
  COUNT(*)::bigint AS "comments count",
  ROUND(AVG(NULLIF(c.score,'')::numeric), 2) AS "average score"
FROM comments c
JOIN subreddits s ON s.name = c.subreddit_id
GROUP BY s.name
ORDER BY "comments count" DESC
LIMIT 10;

DROP TABLE IF EXISTS query4;
CREATE TABLE query4 AS
SELECT
  a.name AS "name",
  a.link_karma AS "link karma",
  a.comment_karma AS "comment karma",
  CASE WHEN a.link_karma >= a.comment_karma THEN 1 ELSE 0 END AS "label"
FROM authors a
WHERE ((COALESCE(a.link_karma,0) + COALESCE(a.comment_karma,0)) / 2.0) > 1000000
ORDER BY ((COALESCE(a.link_karma,0) + COALESCE(a.comment_karma,0)) / 2.0) DESC, a.name ASC;

DROP TABLE IF EXISTS query5;
CREATE TABLE query5 AS
SELECT
  s.subreddit_type AS "sr type",
  COUNT(c.id)::bigint AS "comments num"
FROM comments c
JOIN subreddits s ON s.name = c.subreddit_id
WHERE c.author = '[deleted_user]'
GROUP BY s.subreddit_type;
