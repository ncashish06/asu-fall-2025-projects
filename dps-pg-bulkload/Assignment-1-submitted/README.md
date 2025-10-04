A) What this repo contains
1) create_tables.sql – creates the 4 base tables (authors, subreddits, submissions, comments) with the exact column names from the stripped dataset. Tables are UNLOGGED to speed up bulk loads.
2) create_relations.sql – adds primary keys, uniqueness constraints, indexes, and the 5 graded foreign keys:
    submissions.author → authors(name)
    submissions.subreddit_id → subreddits(name)
    comments.author → authors(name)
    comments.subreddit_id → subreddits(name)
    comments.subreddit → subreddits(display_name)
3) queries.sql – creates query1 … query5 exactly as required (schemas below).
4) assignment1.sh – runs the whole pipeline: create tables -> copy csv files -> add relations -> run queries.

B) Quick row-count sanity check
Command: psql -d postgres -U postgres -c "SELECT 'authors' AS table, COUNT(*) AS rows FROM authors UNION ALL SELECT 'subreddits', COUNT(*) FROM subreddits UNION ALL SELECT 'submissions', COUNT(*) FROM submissions UNION ALL SELECT 'comments', COUNT(*) FROM comments;"
Output: 
    table    |   rows   
-------------|----------
 subreddits  |   914067
 submissions |  1263937
 authors     |  6158212
 comments    | 10557466

C) Queries 
QUERY 4: WHERE vs HAVING 
Both are correct. WHERE is evaluated before aggregation. HAVING without GROUP BY also works in Postgres but is typically a post-aggregation filter and can be slightly slower. If benchmarking with \timing on, I observed [fill in your numbers] ms for WHERE vs [fill in] ms for HAVING. Likely faster with WHERE due to earlier pruning.

Query 4 with HAVING Keyword:
DROP TABLE IF EXISTS query4;
CREATE TABLE query4 AS
SELECT
  a.name AS "name",
  a.link_karma AS "link karma",
  a.comment_karma AS "comment karma",
  CASE WHEN a.link_karma >= a.comment_karma THEN 1 ELSE 0 END AS "label"
FROM authors a
GROUP BY a.name, a.link_karma, a.comment_karma
HAVING ((COALESCE(a.link_karma,0) + COALESCE(a.comment_karma,0)) / 2.0) > 1000000
ORDER BY ((COALESCE(a.link_karma,0) + COALESCE(a.comment_karma,0)) / 2.0) DESC, "name" ASC;

Delta between runs (HAVING vs WHERE):
- Wall clock (real): 1.557 s − 0.764 s = +0.793 s -> HAVING is ~2.04× slower (+103.8%).
- User CPU: 0.036 s − 0.031 s = +0.005 s -> +16.1% (tiny absolute diff).
- System CPU: 0.027 s − 0.053 s = −0.026 s -> HAVING used less sys CPU (−49.1%).
- Total CPU (user+sys): 0.063 s vs 0.084 s -> −0.021 s (−25%) for HAVING.
Outcome: Despite slightly lower CPU, the HAVING version’s wall time is ~0.79 s longer (~2×). That’s because WHERE filters rows before grouping/aggregation, so fewer tuples flow through the pipeline, which generally reduces I/O and memory work and yields faster end-to-end time. HAVING acts after grouping, so it typically processes more data first and then filters.

D) Performance & Environment
Machine: Apple MacBook Air (M1, 8GB RAM)
OS: macOS Sequoia 15.6.1
PostgreSQL: 14.19 (psql 14.19)
Total wall-clock for the full pipeline (assignment1.sh): 2 min 31 s (151 s). It was measured using command: time ./assignment1.sh
