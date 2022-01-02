-- STACK OVERFLOW QUERY
-- Query for stats by tag
SELECT
  REPLACE(t.tag, '|', '') AS tag,
  COUNT(q.tags) AS total_posts,
  SUM(q.view_count) AS total_views,
  SUM(q.answer_count) AS total_answers,
  SUM(q.comment_count) AS total_comments,
  SUM(q.favorite_count) AS total_favorites,
  EXTRACT(YEAR FROM q.creation_date) AS year
FROM (SELECT CONCAT('|', tags, '|') AS tags, answer_count, comment_count, creation_date, favorite_count, view_count
      FROM `bigquery-public-data.stackoverflow.posts_questions`) AS q,
     (SELECT CONCAT('|', tag_name, '|') AS tag
      FROM `bigquery-public-data.stackoverflow.tags`
      ORDER BY count DESC LIMIT 1000) AS t
WHERE q.tags LIKE CONCAT('%', t.tag, '%')
GROUP BY t.tag, year
ORDER BY total_posts DESC;

-- GITHUB QUERIES - adapted from: https://github.com/madnight/githut
-- Query for repo language count
SELECT
    name AS language,
    COUNT(*) AS num_repos
FROM bigquery-public-data.github_repos.languages, UNNEST(language) as l
GROUP BY l.name
ORDER BY num_repos DESC

-- Query for event types
SELECT language as name, year, quarter, count 
FROM ( SELECT *
       FROM ( SELECT lang as language, y as year, q as quarter, type, COUNT(*) as count
              FROM ( SELECT a.type type, b.lang lang, a.y y, a.q q
                     FROM ( -- Returns a list of all historical Github events
                            SELECT type, EXTRACT(YEAR FROM created_at) as y, EXTRACT(QUARTER FROM created_at) as q,
                            REGEXP_REPLACE(repo.url, r'(https:\/\/github\.com\/|https:\/\/api\.github\.com\/repos\/)', '') as name
                            FROM `githubarchive.month.*` ) a
                     JOIN ( -- Returns a list of unique repos
                            SELECT repo_name as name, lang
                            FROM ( -- Returns a list of repos, grouped by repo name
                                   SELECT *, ROW_NUMBER() OVER (PARTITION BY repo_name ORDER BY lang) as num
                                   FROM ( -- Returns the a list of repos
                                          SELECT repo_name, FIRST_VALUE(l.name) OVER (partition by repo_name order by l.bytes DESC) AS lang
                                          FROM `bigquery-public-data.github_repos.languages`, UNNEST(language) AS l))
                            WHERE num = 1 order by repo_name) b
                     ON a.name = b.name)
              GROUP by type, language, year, quarter
              order by year, quarter, count DESC)
       WHERE count >= 100)
WHERE type = 'PullRequestEvent'

-- QUERIES FOR FILTERING DATA BY CHOSEN LANGUAGES
SELECT *
FROM test-project-334301.most_popular_cleaning_data.github_repos
WHERE language IN (SELECT lang FROM test-project-334301.most_popular_cleaning_data.languages);

SELECT name, year, SUM(count) AS count
FROM test-project-334301.most_popular_cleaning_data.github_prs
WHERE name IN (SELECT lang FROM test-project-334301.most_popular_cleaning_data.languages)
GROUP BY name, year;

SELECT name, year, SUM(count) AS count
FROM test-project-334301.most_popular_cleaning_data.github_issues
WHERE name IN (SELECT lang FROM test-project-334301.most_popular_cleaning_data.languages)
GROUP BY name, year;

SELECT *
FROM test-project-334301.most_popular_cleaning_data.stack_overflow
WHERE tag IN (SELECT LOWER(lang) AS lowercase_lang FROM test-project-334301.most_popular_cleaning_data.languages)