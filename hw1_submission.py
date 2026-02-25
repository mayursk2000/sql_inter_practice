def query_example():
  """
  This is just an example query to showcase the format of submission
  Please format the query in Bigquery console and paste it here.
  Submission starts from query_one.
  """
  return """
SELECT
 repo_name,
 author.name AS author_name,
FROM
 `bigquery-public-data.github_repos.sample_commits`;
  """

def query_one():
    """Query one"""
    # add the formatted query between the triple quotes
    return """
    SELECT author.name AS name, COUNT(*) AS count
FROM `bigquery-public-data.github_repos.sample_commits`
WHERE author.name IS NOT NULL AND author.email IS NOT NULL
GROUP BY author.name, author.email
ORDER BY count DESC
LIMIT 10;
    """

def query_two():
    """Query two"""
    return """
    SELECT license, COUNT(*) AS count
FROM `bigquery-public-data.github_repos.licenses`
WHERE license IS NOT NULL AND repo_name IS NOT NULL
GROUP BY license
ORDER BY count DESC
LIMIT 10;

    """

def query_three():
    """Query three"""
    return """
SELECT
  CASE
    WHEN license LIKE '%gpl%' THEN 'gpl'
    WHEN license LIKE '%bsd%' THEN 'bsd'
    WHEN license LIKE '%mit%' THEN 'mit'
    ELSE 'other'
    END AS family,
  COUNT(*) AS count
FROM `bigquery-public-data.github_repos.licenses`
GROUP BY family
ORDER BY count DESC;

    """

def query_four():
    """Query four"""
    return """
SELECT l.name AS name, COUNT(DISTINCT repo_name) AS count
FROM
  `bigquery-public-data.github_repos.languages`,
  UNNEST(language) AS l
GROUP BY name
ORDER BY count DESC
LIMIT 10;

    """

def query_five():
    """Query five"""
    return """
WITH
  repo_totals AS (
    SELECT
      repo_name,
      language,
      (SELECT SUM(l.bytes) FROM UNNEST(language) AS l) AS total_bytes
    FROM
      `bigquery-public-data.github_repos.languages`
  )
SELECT
  l.name AS name,
  COUNT(*) AS count
FROM
  repo_totals,
  UNNEST(language) AS l
WHERE
  total_bytes > 0
  AND l.bytes / total_bytes >= 0.5
GROUP BY name
ORDER BY count DESC, name DESC

    """

def query_six():
    """Query six"""
    return """
SELECT
  l.name AS name,
  MAX_BY(s.repo_name, s.watch_count) AS repo_name
FROM
  `bigquery-public-data.github_repos.languages` AS r,
  UNNEST(r.language) AS l
JOIN
  `bigquery-public-data.github_repos.sample_repos` AS s
  ON
    r.repo_name = s.repo_name
GROUP BY name
ORDER BY name DESC
    """

def query_seven():
    """Query seven"""
    return """
WITH
  top_repos AS (
    SELECT
      repo_name
    FROM
      `bigquery-public-data.github_repos.sample_repos`
    ORDER BY watch_count DESC
    LIMIT 100
  )
SELECT
  l.name AS name,
  COUNT(*) AS occurance
FROM
  top_repos AS t
JOIN
  `bigquery-public-data.github_repos.languages` AS r
  ON
    t.repo_name = r.repo_name,
  UNNEST(r.language) AS l
GROUP BY name
ORDER BY occurance DESC, name DESC

    """

def query_eight():
    """Query eight"""
    return """
WITH
  repo_commits AS (
    SELECT
      repo_name,
      COUNT(*) AS commit_count
    FROM
      `bigquery-public-data.github_repos.sample_commits`
    GROUP BY repo_name
  )
SELECT
  l.name AS name,
  MAX_BY(rc.repo_name, rc.commit_count) AS repo_name
FROM
  `bigquery-public-data.github_repos.languages` AS r,
  UNNEST(r.language) AS l
JOIN
  repo_commits AS rc
  ON
    r.repo_name = rc.repo_name
GROUP BY name
ORDER BY MAX(rc.commit_count) DESC, name DESC

    """

def query_nine():
    """Query nine"""
    return """
SELECT
  EXTRACT(YEAR FROM committer.date) AS year,
  COUNT(*) AS count
FROM
  `bigquery-public-data.github_repos.sample_commits`
GROUP BY year
ORDER BY year DESC
    """

def query_ten():
    """Query ten"""
    return """
SELECT
  EXTRACT(DAYOFWEEK FROM committer.date) AS day_num,
  COUNT(*) AS count
FROM
  `bigquery-public-data.github_repos.sample_commits`
GROUP BY day_num
ORDER BY count DESC
    """

def query_eleven():
    """Query eleven"""
    return """
WITH repo_commits AS (
  SELECT
    repo_name,
    COUNT(*) AS commit_count
  FROM
    `bigquery-public-data.github_repos.sample_commits`
  GROUP BY repo_name
),
repo_activity AS (
  SELECT
    s.repo_name,
    s.watch_count,
    CASE
      WHEN rc.commit_count >= 1000 THEN 'high'
      WHEN rc.commit_count >= 100 THEN 'medium'
      ELSE 'low'
    END AS activity_level
  FROM
    `bigquery-public-data.github_repos.sample_repos` AS s
  JOIN
    repo_commits AS rc
  ON
    s.repo_name = rc.repo_name
),
buckets AS (
  SELECT 'high' AS activity_level, 1 AS sort_order UNION ALL
  SELECT 'medium', 2 UNION ALL
  SELECT 'low', 3
),
stats AS (
  SELECT
    b.activity_level,
    b.sort_order,
    AVG(ra.watch_count) AS avg_watch
  FROM
    buckets AS b
  LEFT JOIN repo_activity AS ra ON b.activity_level = ra.activity_level
  GROUP BY b.activity_level, b.sort_order
),
medians AS (
  SELECT DISTINCT
    b.activity_level,
    PERCENTILE_CONT(ra.watch_count, 0.5) OVER(PARTITION BY b.activity_level) AS median_watch
  FROM
    buckets AS b
  LEFT JOIN repo_activity AS ra ON b.activity_level = ra.activity_level
)
SELECT
  s.activity_level,
  s.avg_watch,
  m.median_watch
FROM
  stats AS s
JOIN
  medians AS m ON s.activity_level = m.activity_level
ORDER BY s.sort_order
    """


def query_twelve():
    """Query twelve"""
    return """
WITH
  author_repo_commits AS (
    SELECT
      author.name AS author_name,
      author.email AS author_email,
      repo_name,
      COUNT(*) AS repo_commit_count
    FROM
      `bigquery-public-data.github_repos.sample_commits`
    GROUP BY author.name, author.email, repo_name
  ),
  author_total AS (
    SELECT
      author_name,
      author_email,
      SUM(repo_commit_count) AS commit_count
    FROM
      author_repo_commits
    GROUP BY author_name, author_email
  ),
  author_top_repo AS (
    SELECT
      author_name,
      author_email,
      MAX_BY(repo_name, repo_commit_count) AS repo_name
    FROM
      author_repo_commits
    GROUP BY author_name, author_email
  )
SELECT
  t.author_name,
  t.commit_count,
  r.repo_name
FROM
  author_total AS t
JOIN
  author_top_repo AS r
  ON t.author_name = r.author_name AND t.author_email = r.author_email
ORDER BY t.commit_count DESC
LIMIT 10

    """

def query_thirteen():
    """Query thirteen"""
    return """
SELECT
  repo_name,
  ROUND(
    COUNTIF(author.name = committer.name AND author.email = committer.email)
      / COUNT(*),
    2) AS ratio
FROM
  `bigquery-public-data.github_repos.sample_commits`
GROUP BY repo_name
ORDER BY repo_name DESC

    """

def query_fourteen():
    """Query fourteen"""
    return """
WITH
  author_counts AS (
    SELECT
      repo_name,
      author.name AS author_name,
      author.email AS author_email,
      COUNT(*) AS cnt
    FROM
      `bigquery-public-data.github_repos.sample_commits`
    GROUP BY repo_name, author.name, author.email
  ),
  committer_counts AS (
    SELECT
      repo_name,
      committer.name AS committer_name,
      committer.email AS committer_email,
      COUNT(*) AS cnt
    FROM
      `bigquery-public-data.github_repos.sample_commits`
    GROUP BY repo_name, committer.name, committer.email
  ),
  top_authors AS (
    SELECT
      repo_name,
      MAX_BY(author_name, cnt) AS author_name
    FROM
      author_counts
    GROUP BY repo_name
  ),
  top_committers AS (
    SELECT
      repo_name,
      MAX_BY(committer_name, cnt) AS committer_name
    FROM
      committer_counts
    GROUP BY repo_name
  ),
  top_languages AS (
    SELECT
      repo_name,
      MAX_BY(l.name, l.bytes) AS language
    FROM
      `bigquery-public-data.github_repos.languages`,
      UNNEST(language) AS l
    GROUP BY repo_name
  )
SELECT
  a.repo_name,
  a.author_name,
  c.committer_name,
  lang.language
FROM
  top_authors AS a
JOIN
  top_committers AS c
  ON a.repo_name = c.repo_name
JOIN
  top_languages AS lang
  ON a.repo_name = lang.repo_name
ORDER BY a.repo_name DESC

    """

def query_fifteen():
    """Query fifteen"""
    return """
WITH
  qualified_repos AS (
    SELECT
      l.repo_name,
      l.license,
      r.watch_count
    FROM
      `bigquery-public-data.github_repos.licenses` AS l
    JOIN
      `bigquery-public-data.github_repos.sample_repos` AS r
      ON
        l.repo_name = r.repo_name
    WHERE
      l.license = 'mit'
      AND r.watch_count >= 8000
  ),
  author_commits AS (
    SELECT
      c.repo_name,
      c.author.name AS author_name,
      COUNT(*) AS cnt
    FROM
      `bigquery-public-data.github_repos.sample_commits` AS c
    JOIN
      qualified_repos AS q
      ON
        c.repo_name = q.repo_name
    GROUP BY c.repo_name, c.author.name, c.author.email
  )
SELECT
  q.repo_name,
  q.license,
  q.watch_count,
  MAX_BY(a.author_name, a.cnt) AS author_name
FROM
  qualified_repos AS q
JOIN
  author_commits AS a
  ON
    q.repo_name = a.repo_name
GROUP BY q.repo_name, q.license, q.watch_count
ORDER BY q.repo_name DESC

    """
