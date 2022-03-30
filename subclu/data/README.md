# i18n Topic Model Queries

The focus of this subfolder will be queries on how to select data for the topic model. As of November 2021 the model is content based (it's based on **text content**)
- subreddits (title & description text)
- posts (title + body text)
- comments (body text)

In the future we plan to use a behavior-based models.

---

The core SQL queries for topic modeling are in the topic model repo here:<br>
https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n

There are two main SQL-related folders:
- `sql_dataset_selection_and_creation`
  - Queries to select data for each version of the topic model
- `sql_misc`
  - Queries to a) EDA before selecting data or b) explore the topic model outputs

## Selecting data (`sql_dataset_selection_and_creation`)
The folder for queries to select data is:
- `subreddit_clustering_i18n/subclu/data/sql_dataset_selection_and_creation/`
- https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/tree/master/subclu/data/sql_dataset_selection_and_creation

Example for v0.4.0 queries:<br>
- folder in github:
  - https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/tree/master/subclu/data/sql_dataset_selection_and_creation/v0.4.0_add_more_geo_and_active_subs
- Important queries:
  - [`_00_geo_subreddits.sql`](https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/blob/master/subclu/data/sql_dataset_selection_and_creation/v0.4.0_add_more_geo_and_active_subs/_00_geo_subreddits.sql)
    - Create new table with lower geo-relevance threshold & definition (this way Cricket is relevant to India & the UK) 
  - [`_01_select_subreddits.sql`](https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/blob/master/subclu/data/sql_dataset_selection_and_creation/v0.4.0_add_more_geo_and_active_subs/_01_select_subreddits.sql)
    - Based on new geo-relevance definition, select highly active subs + subs that are i18n targets (e.g., Germany, France, India, Australia) 
  - [`_02_select_posts_for_modeling.sql`](https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/blob/master/subclu/data/sql_dataset_selection_and_creation/v0.4.0_add_more_geo_and_active_subs/_02_select_posts_for_modeling.sql)
    - Selects posts for the selected subreddits
    - Currently we limit to the top 1,200 comments from selected subreddits
    - Adds text from OCR (if it exists) and does some basic text clean up
  - [`_03_select_comments_for_modeling.sql`](https://github.snooguts.net/david-bermejo/subreddit_clustering_i18n/blob/master/subclu/data/sql_dataset_selection_and_creation/v0.4.0_add_more_geo_and_active_subs/_03_select_comments_for_modeling.sql)
    - Selects comments for the selected posts
    - Currently we limit to the top 9 comments for each post
    - Also excludes short comments


## EDA & scratch work (`sql_misc`)

These queries are more about exploration than anything. Expect them to be messy :D
