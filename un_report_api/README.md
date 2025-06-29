# UN Country Voting Report API

This FastAPI application provides API endpoints to generate JSON reports on UN voting patterns and country rankings.

## Setup

1.  **Create Directory Structure:**
    Ensure you have the main `un_report_api` directory and an `app` subdirectory within it.

2.  **Add Data Files:**
    -   Create a directory `un_report_api/app/required_csvs/`.
    -   Copy your data CSV files into this `required_csvs` directory. The report and ranking generators expect them here.
        - `annual_scores.csv`: Crucial for reports and rankings. Contains yearly pillar scores and ranks for all countries.
        - `pairwise_similarity_yearly.csv`: Used for ally/enemy/P5/regional alignment calculations. Contains pre-calculated yearly similarity scores between all country pairs.
        - `topic_votes_yearly.csv`: Used for topic-based voting analysis in the main report.
        - `UN_Country_Region_Mapping.csv`: Provides the mapping from country ISO codes to their UN region.
        - `country_classifications_2023.csv`: Contains flags for OECD, G20, and top/bottom 50 GDP/Population status, used to enrich the rankings endpoint.

3.  **Install Dependencies:**
    Navigate to the `un_report_api` directory in your terminal and run:
    ```bash
    pip install -r requirements.txt
    ```

## Running the API

Navigate to the `un_report_api` directory (the one containing the `app` folder and `requirements.txt`).

Run the FastAPI application using Uvicorn:

```bash
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Or, if you are inside the `un_report_api/app` directory:
```bash
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Breakdown of the command:
- `python -m uvicorn`: Runs Uvicorn as a module.
- `app.main:app`: Tells Uvicorn where to find the FastAPI application instance (`app`) located in `main.py` within the `app` package.
- `--reload`: Enables auto-reloading when code changes (useful for development).
- `--host 0.0.0.0`: Makes the server accessible on your network.
- `--port 8000`: Specifies the port to run on.

## API Usage

Once the server is running, you can access:

-   **API Documentation (Swagger UI):** `http://127.0.0.1:8000/docs`
-   **Alternative Documentation (ReDoc):** `http://127.0.0.1:8000/redoc`

### Endpoint: `GET /report/{country_iso}`

Generates a comprehensive report for the specified country over a period.

**Path Parameters:**
-   `country_iso` (string, 3 characters, uppercase): The ISO3 code of the country (e.g., `USA`).

**Query Parameters:**
-   `start_year` (integer): The start year for the report period. Must be between 1946 and 2024.
-   `end_year` (integer): The end year for the report period. Must be between 1946 and 2024. Must be greater than or equal to `start_year`. The period must span at least 3 years (`end_year` - `start_year` >= 2).

**Response Highlights (includes, but not limited to):**
-   `report_metadata`: Basic information about the report query.
-   `world_average_scores_period`: Average pillar scores for all countries over the period.
-   `country_average_scores_period`: Average Pillar 1, 2, 3, and total index scores for the *selected country* over the period.
-   `index_score_analysis`: Start/end year scores and ranks for the selected country.
-   `voting_behavior_overall`: Aggregated voting statistics for the country vs. the world over the period.
-   `most_aligned_p5_member`: Identifies the UN Security Council P5 member (China, France, Russia, UK, USA) the selected country has the highest voting alignment with over the period, based on similarity scores. If the selected country is a P5 member, it shows alignment with another P5 member.
-   `least_aligned_p5_member`: The P5 member the selected country is least aligned with.
-   `regional_context`: Provides analysis of the country within its UN Region, including a list of `regional_peer_alignment` scores (how the selected country's voting record aligns with its geographic neighbors) and the `average_regional_alignment_score`.
-   `scores_timeseries`: Yearly breakdown of pillar scores and ranks for the country, including world averages per year.
-   `top_allies` / `top_enemies`: Countries most and least aligned with the selected country based on average voting similarity over the period.
-   `top_supported_topics` / `top_opposed_topics`: Topics most supported and opposed by the country.
-   `all_topic_voting`: Detailed voting record across all topics for the country vs. the world.

**Example Request:**
`http://127.0.0.1:8000/report/USA?start_year=2009&end_year=2013`

### Endpoint: `GET /rankings/{year}`

Provides yearly rankings for all countries across different pillars.

**Path Parameters:**
-   `year` (integer): The specific year for which to retrieve rankings. Must be between 1946 and 2024.

**Response Highlights:**
-   `data`: Contains rankings for the specified `year`.
    -   `pillar_1_rankings`: List of countries ranked by Pillar 1 score.
    -   `pillar_2_rankings`: List of countries ranked by Pillar 2 score.
    -   `pillar_3_rankings`: List of countries ranked by Pillar 3 score.
    -   `average_pillar_rankings`: List of countries ranked by the average of pillar scores (or total index average).
    -   Each ranking entry is enriched with the following data:
        - `country_name` and `country_code`
        - The score `value` and its global `rank`.
        - `rank_change` and `value_change` (1-year change).
        - `rank_change_10_year` and `value_change_10_year` (10-year change).
        - Classification flags: `is_oecd`, `is_g20`, `is_top_50_gdp`, `is_bottom_50_gdp`, `is_top_50_population`, `is_bottom_50_population`.
-   `message`: An optional message, e.g., if data for the previous year isn't available for change calculations.

**Example Request:**
`http://127.0.0.1:8000/rankings/2021`

**Logging:**
Requests made to the API (ISO3 code, start date, end date, or year for rankings) are logged to the console where Uvicorn is running. 