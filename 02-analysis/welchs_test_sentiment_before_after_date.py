# Perform Welch's T-Test for user inputs on interactive PowerBI dashboard.
###### need to summarize by day and create timestamp column before this runs.
#
# User selects date and keyword, function returns results for all four sentiment metrics.
#
#

import pandas as pd
from scipy.stats import ttest_ind
import numpy as np

def get_welch_test_stats(series: pd.Series, event_date_str: str):
    """
    Performs the Welch's t-test (two-sample test with unequal variance) to the mean of sentiment
    metrics BEFORE and AFTER chosen date ("event_date".)
    Date supplied by user in YYYY-MM-DD string format.
    
    """
    try:
        event_date = pd.to_datetime(event_date_str)
    except ValueError:
        raise ValueError(f"Invalid date format provided: {event_date_str}. Must be in a valid format like 'YYYY-MM-DD'.")
    # 1. 'group_a' is before chosen event_date, 'group_b' is on or after event_date. 
    group_a_mask = series.index < event_date
    group_b_mask = series.index >= event_date

    # Calculate means for interpretation
    group_a = series[group_a_mask].dropna()
    group_b = series[group_b_mask].dropna()
    
    if len(group_a) < 2 or len(group_b) < 2:
        return {
            't_stat': np.nan,
            'p_value': np.nan,
            'is_significant': False,
            'mean_before': group_a.mean() if len(group_a) > 0 else np.nan,
            'mean_after': group_b.mean() if len(group_b) > 0 else np.nan,
            'mean_difference': np.nan,
            'message': "Insufficient data for a valid t-test in one or both groups."
        }
    # 2. Perform the t-test (equal variance not assumed)
    t_stat, p_value = ttest_ind(group_a, group_b, equal_var=False)
    mean_a = group_a.mean()
    mean_b = group_b.mean()

    return {
        't_stat': t_stat,
        'p_value': p_value,
        'is_significant': p_value < 0.05, 
        'mean_before': mean_a,
        'mean_after': mean_b,
        'mean_difference': mean_b - mean_a, # Difference is calculated as (After - Before)
        'message': f"Test completed. Mean difference (After - Before): {mean_b - mean_a:.2f}"
    }



def run_welch_analysis_for_user_input(df: pd.DataFrame, target_keyword: str, event_date_str: str):
    """
    Runs the Welch's t-test on all four sentiment metrics for the specified keyword and date, 
    returning results in a dictionary.

    Assumes 'TIMESTAMP' has been created and added to dataframe.
    """
    
    metric_columns = [
        'DAILY_PCT_POSITIVE', 
        'DAILY_PCT_NEGATIVE', 
        'DAILY_POSITIVE_MENTIONS', 
        'DAILY_NEGATIVE_MENTIONS'
    ]
    
    # Filter to chosen keyword
    keyword_data = df[df['KEYWORD'] == target_keyword].copy()
    
    # Order by timestamp
    keyword_data = keyword_data.set_index('TIMESTAMP').sort_index()

    # Loop through all metrics and store results
    all_metric_results = {}
    
    for metric in metric_columns:
        if metric not in keyword_data.columns:
            all_metric_results[metric] = {"Error": f"Metric column '{metric}' not found."}
            continue
            
        time_series = keyword_data[metric]
        
        # Run the test for the current metric
        results = get_welch_test_stats(time_series, event_date_str)
        all_metric_results[metric] = results

    # 4. Final output structure
    final_output = {
        'keyword': target_keyword,
        'cutoff_date': event_date_str,
        **all_metric_results
}
    
    return final_output