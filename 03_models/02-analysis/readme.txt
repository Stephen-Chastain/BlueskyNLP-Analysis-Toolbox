# Bluesky NLP Analysis Toolbox

Statistical analysis of sentiment toward political and cultural keywords in Bluesky posts, built on a Snowflake warehouse of NLP-labeled posts from the Bluesky firehose (Summer-Fall 2025).

The toolbox takes posts that have already been labeled for sentiment (natural language processing, RoBERTa Model), matches them against a keyword lookup table, aggregates them at the **thread** level, and runs a set of statistical tests: sample-size checks, goodness-of-fit, stationarity, decomposition, correlation and before/after event comparisons. Results feed a Tableau Public dashboard.

**dashboard:** https://public.tableau.com/views/DailyKeywordsSentimentDashboard/KeywordTimeseriesDashboard?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

---

## Data and sampling

- **Source:** Bluesky firehose posts collected from July 2025, stored in Snowflake and labeled with sentiment (Positive / Neutral / Negative) and a confidence score. Named-entity recognition labels are also available in the source view.
- **Sampling design:** an *interrupted census* — every post in a 5-minute window, every 3 hours throughout the day. It is neither a random sample nor a continuous one, so time-series analysis aggregates to the **day** to give a continuous series.
- **Keywords:** a lookup table of keywords, each tagged with a **topic** (e.g. US domestic, global conflicts, culture war, economic, science) and a **category** (*issue specific* or *coded* language). A post matches a keyword when its text contains it (case-insensitive).
- **Confidence filter:** only matches with a sentiment confidence score above 0.6 are kept.

## Why threads, not posts

Counting individual posts would over-weight "tweetstorms" — one author posting a chain of self-replies that almost certainly share one sentiment. That is pseudoreplication, and it inflates sample sizes and autocorrelation.

Posts are therefore grouped into **threads** (all posts back to the same root), and each *author × keyword × thread* combination counts once per sentiment. For example:

- 1 author, 6 posts, same sentiment → 1 flag
- 3 authors, 1 post each, same sentiment → 3 flags
- 2 authors, same topic, different sentiments → 1 flag for each sentiment

## Repository contents

All code is in [`02-analysis/`](02-analysis/). Data ingestion and NLP labeling happened upstream of this repository.

### SQL views (Snowflake)

Create in this order. `thread_matches` is a prerequisite for the threads, time-series and authors views.

| File | Creates | Purpose |
|---|---|---|
| `view_nlp_data.sql` | `firehose_nlp_vw` | Joins NLP labels to post text; adds date parts (year, quarter, month, day, hour) and text-length fields. |
| `create_threads_matches_view.sql` | `THREAD_MATCHES` | Matches posts to keywords, assigns each post its thread root, applies the 0.6 confidence filter. |
| `create_threads_nlp_view.sql` | `THREADS_NLP` | Sentiment per keyword per thread: authors, posts, positive/negative/neutral mentions and percentages. |
| `create_timeseries_nlp_view.sql` | `TIMESERIES_NLP` | Thread-level sentiment by keyword and hour/day/month/year. |
| `create_authors_nlp_view.sql` | `AUTHORS_NLP` | Sentiment per author per keyword, plus threads started by each author. |
| `text_keyword_summarystats_vw.sql` | `THREADS_KEYWORDS_SUMMARYSTATS` | Keyword-level totals across all threads, for population-level tests. |
| `aggregate_timeseries_daily.sql` | *(query)* | Rolls `TIMESERIES_NLP` up to daily values for time-series analysis. |

### Notebooks and scripts

| File | Method | Question it answers |
|---|---|---|
| `initial_samplesize_cochrans.ipynb` | Cochran's formula | Does each keyword have enough posts for reliable estimates? (95% confidence, ±5% margin → minimum 385.) |
| `cochrans_samplesize_sentiments.ipynb` | Cochran's formula | Same check at the thread level, separately for positive, neutral, negative and total threads. |
| `sentiment_on_threads_chisquares.ipynb` | Chi-square goodness-of-fit | Does a keyword's sentiment split differ significantly from an even 33/33/33 split? |
| `ADF_on_keyword_table.ipynb` | Augmented Dickey-Fuller | Is each keyword's daily % positive / % negative stationary, or does it trend? |
| `timeseries_analysis_pipeline.ipynb` | ADF → seasonal decomposition → Pearson | Full time-series pipeline (below). |
| `welchs_test_sentiment_before_after_date.py` | Welch's t-test | Did a keyword's sentiment change significantly after a chosen event date? Built for interactive use behind a dashboard. |
| `DateTimestamp.dax` | DAX | Power BI calculated column that builds a date from the year/month/day fields, returning blank for invalid dates. |

### Time-series pipeline

`timeseries_analysis_pipeline.ipynb` runs, for each keyword, on daily % positive, % negative, positive mentions and negative mentions:

1. **ADF test** — flags each series as stationary or trending (p ≤ 0.05).
2. **Seasonal decomposition** — only for trending series. Fits both additive and multiplicative models with a 7-day (weekly) period and reports trend, seasonal range and residual spread. The model with more random residuals is the better fit.
3. **Pearson correlation** — between daily positive and negative mentions, using differenced data where a series is non-stationary. Runs only for keywords with at least 100 positive mentions, to avoid spurious correlations on sparse data.

The output is one summary row per keyword.

## Setup

```bash
pip install -r 02-analysis/requirements.txt
```

The notebooks connect to Snowflake with key-pair authentication, read from environment variables. Set these before running:

| Variable | Meaning |
|---|---|
| `SF_USR` | Snowflake user |
| `SF_ID` | Account identifier |
| `SF_WH` | Warehouse |
| `SF_DB` | Database |
| `SF_SC` | Schema |
| `SF_RL` | Role |
| `SF_AUTH` | Authenticator |
| `PRIVATE_KEY_PATH` | Path to the private key file |
| `PRIVATE_KEY_PASSPHRASE` | Private key passphrase |

No credentials or post-level data are stored in this repository.

## Limitations and future work

- The chi-square null hypothesis — an even three-way sentiment split, standing in for a platform with no political leaning — is a simplifying assumption. Real polling data on approval of each keyword would be a better baseline.
- Keyword matching is substring-based, so short keywords can match inside unrelated words.
- Only English-language sentiment is analyzed; translating posts in other languages would widen coverage.
- Several keywords remain below the required sample size. More data collection would allow the full keyword set to be tested.
- Planned: choosing the keyword interactively (e.g. from a dropdown) rather than running on a fixed list.

## Contributors

- Stephen Chastain
- Patrick F Callahan https://github.com/sherlock-wings/