# Query Plan Visualizations


# querygraph CLI

querygraph is a simple CLI tool that can be used to generate visual query graphs from BigQuery query
execution metadata.  

```
Usage: querygraph <job>

Allowed format of the <job> argument:
 - project:location.jobid
 - project:jobid


Optional Flags:

  -dot_path string
        path to the dot (graphviz) binary (default "/usr/bin/dot")
  -out string
        path to the output file.  By default, creates a file based on the job ID in the current directory.
```

The format of the job ID is the same fully qualified format as reported by tools such as the Google Cloud Console or the `bq` CLI tool:  `projectid:location.job_id` or `projectid.job_id`

For example, here's a job with location included:
`shollyman-demo-test:US.bquxjob_4cccbe9b_17b0e287542`

Or, location omitted:
`shollyman-demo-test:bquxjob_4cccbe9b_17b0e287542`


## Examples of Use

### Golang Imports
This query identifies commonly imported golang packages found in the github public data tables.
```sql
SELECT
  entry,
  COUNT(*) as frequency,
  COUNT(DISTINCT repo_name) as distinct_repos
FROM (
  SELECT
    files.repo_name,
    SPLIT(REGEXP_EXTRACT(contents.content, r'.*import\s*[(]([^)]*)[)]'), '\n') AS entries
  FROM `bigquery-public-data.github_repos.contents` AS contents
  JOIN (
    SELECT id, repo_name FROM `bigquery-public-data.github_repos.files`
    WHERE path LIKE '%.go' GROUP BY id, repo_name
  ) AS files
  USING (id)
  WHERE REGEXP_CONTAINS(contents.content, r'.*import\s*[(][^)]*[)]')
)
CROSS JOIN UNNEST(entries) as entry
WHERE entry IS NOT NULL AND entry != ""
GROUP BY entry
ORDER BY distinct_repos DESC, frequency DESC
LIMIT 1000
```
![Image of query plan shollyman-demo-test:US.bquxjob_4cccbe9b_17b0e287542](./examples/shollyman-demo-test_US_bquxjob_4cccbe9b_17b0e287542.png)
