import logging, io, pytz, gc, json, os
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import azure.functions as func
from azure.identity import EnvironmentCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

logging.basicConfig(level=logging.INFO)

tenant_id = os.environ["azure_tenant_id"]
client_id = os.environ["azure_client_id"]
client_secret = os.environ["azure_client_secret"]
azure_vault_url = os.environ["azure_vault_url"]

if os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
    logging.info("Using AzureCliCredential for local development.")
    credential = EnvironmentCredential()
else:
    logging.info("Using ClientSecretCredential for Azure environment.")
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
client = SecretClient(vault_url=azure_vault_url, credential=credential)
connection_string = client.get_secret("azure-connection-string").value
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client("sf-data")

app = func.FunctionApp()

@app.function_name(name="ProcessData")
@app.route(route="process", methods=["POST"])

def process(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ProcessData function triggered.")

    def expand_user_date_grid(df, date_col='CreatedDate', user_col='Email', kpi_cols=None):
        df = df.copy()

        # Ensure date column is datetime
        df[date_col] = pd.to_datetime(df[date_col]).dt.date
        # Get full date range
        min_date = df[date_col].min()
        max_date = df[date_col].max()
        full_date_range = pd.date_range(start=min_date, end=max_date).date

        # Get all unique users
        unique_users = df[user_col].dropna().unique()

        # Create full grid of user-date combinations
        full_grid = pd.MultiIndex.from_product([unique_users, full_date_range], names=[user_col, date_col])
        full_grid_df = pd.DataFrame(index=full_grid).reset_index()

        # Merge with original data
        merged_df = pd.merge(full_grid_df, df, on=[user_col, date_col], how='left')

        # Fill missing KPI columns with 0
        if kpi_cols is None:
            # Infer KPI columns as all numeric columns excluding user/date
            kpi_cols = [col for col in df.columns if col not in [user_col, date_col] and pd.api.types.is_numeric_dtype(df[col])]

        for col in kpi_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(0).astype('Int16')

        return merged_df

    # Fetch blob
    blob_name = None
    for blob in container_client.list_blobs(name_starts_with="Centrical"):
        blob_name = blob.name
        break
    
    if blob_name:
        blob_client = container_client.get_blob_client(blob_name)
        stream = blob_client.download_blob()
        df_raw = pd.read_csv(
            stream,
            parse_dates=['CreatedDate', 'Sign Up In Progress Date', 'Retainer Received'],
            dtype={'Email': 'string', 'Manager Email': 'string'}
            )
    else:
        raise FileNotFoundError("No blob found starting with 'Centrical'")


    # Memory optimization: Convert string columns to category where appropriate
    categorical_cols = ['Status', 'Status Detail', 'Litigation']
    for col in categorical_cols:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype('category')
            
    df_raw['CreatedDate'] = pd.to_datetime(df_raw['CreatedDate'])
    df_raw.drop(columns=['Intake Agent Id', 'Converted Date', 'FirstName', 'LastName', 'UserId'], inplace=True)


    # Normalize identities (ensures consistent join on Email)
    if 'Email' in df_raw.columns:
        df_raw['Email'] = df_raw['Email'].astype('string').str.strip().str.lower()
    if 'Manager Email' in df_raw.columns:
        df_raw['Manager Email'] = df_raw['Manager Email'].astype('string').str.strip().str.lower()

    # Build authoritative latest (non-null) manager per agent for a single merge at the end
    manager_map = (
        df_raw
        .dropna(subset=['Manager Email'])
        .sort_values(['Email', 'CreatedDate'])
        .groupby('Email', as_index=False)
        .tail(1)[['Email', 'Manager Email']]
    )

    # Force garbage collection
    gc.collect()

    excluded_statuses = ['Test', 'Duplicate']
    excluded_status_detail = [
        'Declined Referral', 
        'Pending Referral', 
        'Pending SME Review', 
        'Permission to RO',
        'Referral Atty - Turn Down', 
        'TD by Attorney Review - Pending Referral',
        'TD by Criteria - TD Complete', 
        'TD by SME - Call Complete', 
        'TD by SME - Call Pending'
    ]

    # base_kpi_cols still only contains the KPI names, not the universal date columns
    base_kpi_cols = [
        'DailyIntakes', 'DailySUIP', 'DailyRR',
        'WeeklyIntakes', 'WeeklySUIP', 'WeeklyRR',
        'MonthlyIntakes', 'MonthlySUIP', 'MonthlyRR',
        'YearlyIntakes', 'YearlySUIP', 'YearlyRR'
    ]


    # --- Helper function to save to Azure Blob (can also be reused) ---
    def save_csv_to_azure(container_client, df, path):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode('utf-8-sig')
        blob_client = container_client.get_blob_client(path)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info("Uploaded CSV to Azure Blob: %s", path)

    def column_rename(df, prefix):
        rename_map = {}
        for col in base_kpi_cols: # Only iterate over actual KPI columns
            if col in df.columns:
                new_col_name = ""
                if col.startswith('Daily'):
                    new_col_name = prefix + col.upper()
                elif col.startswith('Weekly'):
                    new_col_name = prefix + col.upper()
                elif col.startswith('Monthly'):
                    new_col_name = prefix + col.upper()
                elif col.startswith('Yearly'):
                    new_col_name = prefix + col.upper()
                
                if new_col_name:
                    rename_map[col] = new_col_name
            else:
                logging.debug(f"Column '{col}' not found in DataFrame for renaming.")

        df = df.rename(columns=rename_map)
        return df


    def memory_efficient_groupby_count(df, group_cols, count_col, result_name):
        # Create a minimal DataFrame for grouping
        minimal_df = df[group_cols + [count_col]].copy()
        
        # Remove any rows with NaN in grouping columns
        minimal_df = minimal_df.dropna(subset=group_cols)
        
        # Perform the groupby operation
        try:
            result = minimal_df.groupby(group_cols)[count_col].count().reset_index()
            result = result.rename(columns={count_col: result_name})
            return result
        except MemoryError:
            logging.warning(f"MemoryError during groupby for {result_name}. Trying chunked approach.")
            
            # Fallback: Process in chunks by user
            unique_users = minimal_df[group_cols[0]].unique()
            chunk_results = []
            
            for user in unique_users:
                user_df = minimal_df[minimal_df[group_cols[0]] == user]
                if not user_df.empty:
                    user_result = user_df.groupby(group_cols)[count_col].count().reset_index()
                    user_result = user_result.rename(columns={count_col: result_name})
                    chunk_results.append(user_result)
                
                # Force garbage collection after each user
                del user_df
                gc.collect()
            
            if chunk_results:
                result = pd.concat(chunk_results, ignore_index=True)
                del chunk_results
                gc.collect()
                return result
            else:
                # Return empty DataFrame with correct structure
                empty_result = pd.DataFrame(columns=group_cols + [result_name])
                return empty_result

    # Sort KPI columns based on defined order

    def process_litigation(df_input: pd.DataFrame, litigation_list: list, prefix: str) -> pd.DataFrame:
        df = df_input.copy(deep=False)

        lit_df = df[df['Litigation'].isin(litigation_list)].copy()
        lit_df = lit_df.loc[~lit_df['Status'].isin(excluded_statuses)]
        
        # Force garbage collection
        del df
        gc.collect()

        # --- Prepare Base Data for Aggregations ---
        lit_df['CreatedDate'] = pd.to_datetime(lit_df['CreatedDate']).dt.date
        # Ensure these are date objects for consistency, NaT for missing
        lit_df['SUIP Date'] = pd.to_datetime(lit_df['Sign Up In Progress Date'], errors='coerce').dt.date
        lit_df['RR Date'] = pd.to_datetime(lit_df['Retainer Received'], errors='coerce').dt.date

        # These 'FirstDayofX' are *internal* to this function for correct aggregation and merging,
        # they will not become columns in the returned combined_df in their raw form.
        lit_df['FirstDayofWeek_Temp'] = pd.to_datetime(lit_df['CreatedDate']).dt.to_period('W-SAT').apply(lambda r: r.start_time.date())
        lit_df['FirstDayofMonth_Temp'] = pd.to_datetime(lit_df['CreatedDate']).dt.to_period('M').apply(lambda r: r.start_time.date())
        # Keep FirstDayofYear_Temp as a date object (Jan 1st of that year)
        lit_df['FirstDayofYear_Temp'] = pd.to_datetime(lit_df['CreatedDate']).dt.year.apply(lambda y: datetime(y, 1, 1).date())

        # --- Calculate Daily, Weekly, Monthly, Yearly Raw Counts using memory-efficient method ---
        
        # Daily aggregations
        daily_intakes_aggs = memory_efficient_groupby_count(
            lit_df.loc[~lit_df['Status'].isin(['Referred Out'])],
            ['Email', 'CreatedDate'],
            'IntakeId',
            'DailyIntakes'
        )
        
        # Force garbage collection
        gc.collect()
        
        # SUIPs are based on SUIP Date
        suip_filtered_df = lit_df.loc[~lit_df['Status Detail'].isin(excluded_status_detail)].copy()
        daily_suip_aggs = memory_efficient_groupby_count(
            suip_filtered_df,
            ['Email', 'SUIP Date'],
            'Sign Up In Progress Date',
            'DailySUIP'
        )
        daily_suip_aggs = daily_suip_aggs.rename(columns={'SUIP Date': 'CreatedDate'})
        
        # Force garbage collection
        gc.collect()
        
        # RRs are based on RR Date
        daily_rr_aggs = memory_efficient_groupby_count(
            lit_df,
            ['Email', 'RR Date'],
            'Retainer Received',
            'DailyRR'
        )
        daily_rr_aggs = daily_rr_aggs.rename(columns={'RR Date': 'CreatedDate'})

        # Force garbage collection
        gc.collect()

        # Combine daily KPIs using outer merge to keep all relevant dates
        daily_kpis_df = pd.merge(daily_intakes_aggs, daily_suip_aggs, on=['Email', 'CreatedDate'], how='outer')
        daily_kpis_df = pd.merge(daily_kpis_df, daily_rr_aggs, on=['Email', 'CreatedDate'], how='outer')
        daily_kpis_df = daily_kpis_df.fillna(0).astype({'DailyIntakes': 'Int16', 'DailySUIP': 'Int16', 'DailyRR': 'Int16'})

        # Force garbage collection
        del daily_intakes_aggs, daily_suip_aggs, daily_rr_aggs
        gc.collect()

        # Weekly aggregation using memory-efficient method
        weekly_aggs = memory_efficient_groupby_count(
            lit_df.loc[~lit_df['Status'].isin(['Referred Out'])],
            ['Email', 'FirstDayofWeek_Temp'],
            'IntakeId',
            'WeeklyIntakes'
        )
        
        # Force garbage collection
        gc.collect()

        # For SUIP and RR week grouping
        suip_filtered_df['SUIP_Week'] = pd.to_datetime(suip_filtered_df['SUIP Date'], errors='coerce').dt.to_period('W-SAT').dt.start_time.dt.date
        lit_df['RR_Week'] = pd.to_datetime(lit_df['RR Date'], errors='coerce').dt.to_period('W-SAT').dt.start_time.dt.date

        suip_weekly_aggs = memory_efficient_groupby_count(
            suip_filtered_df,
            ['Email', 'SUIP_Week'],
            'Sign Up In Progress Date',
            'WeeklySUIP'
        )
        suip_weekly_aggs = suip_weekly_aggs.rename(columns={'SUIP_Week': 'FirstDayofWeek_Temp'})
        
        rr_weekly_aggs = memory_efficient_groupby_count(
            lit_df,
            ['Email', 'RR_Week'],
            'Retainer Received',
            'WeeklyRR'
        )
        rr_weekly_aggs = rr_weekly_aggs.rename(columns={'RR_Week': 'FirstDayofWeek_Temp'})

        # Force garbage collection
        gc.collect()

        weekly_kpis_df = pd.merge(weekly_aggs, suip_weekly_aggs, on=['Email', 'FirstDayofWeek_Temp'], how='outer')
        weekly_kpis_df = pd.merge(weekly_kpis_df, rr_weekly_aggs, on=['Email', 'FirstDayofWeek_Temp'], how='outer')
        weekly_kpis_df = weekly_kpis_df.fillna(0).astype({'WeeklyIntakes': 'Int16', 'WeeklySUIP': 'Int16', 'WeeklyRR': 'Int16'})

        # Force garbage collection
        del weekly_aggs, suip_weekly_aggs, rr_weekly_aggs
        gc.collect()

        # Monthly group labels (start of each month)
        suip_filtered_df['SUIP_Month'] = pd.to_datetime(suip_filtered_df['SUIP Date'], errors='coerce').dt.to_period('M').dt.start_time.dt.date
        lit_df['RR_Month'] = pd.to_datetime(lit_df['RR Date'], errors='coerce').dt.to_period('M').dt.start_time.dt.date
        
        # Monthly aggregation using memory-efficient method
        monthly_aggs = memory_efficient_groupby_count(
            lit_df.loc[~lit_df['Status'].isin(['Referred Out'])],
            ['Email', 'FirstDayofMonth_Temp'],
            'IntakeId',
            'MonthlyIntakes'
        )
        
        suip_monthly_aggs = memory_efficient_groupby_count(
            suip_filtered_df,
            ['Email', 'SUIP_Month'],
            'Sign Up In Progress Date',
            'MonthlySUIP'
        )
        suip_monthly_aggs = suip_monthly_aggs.rename(columns={'SUIP_Month': 'FirstDayofMonth_Temp'})
        
        rr_monthly_aggs = memory_efficient_groupby_count(
            lit_df,
            ['Email', 'RR_Month'],
            'Retainer Received',
            'MonthlyRR'
        )
        rr_monthly_aggs = rr_monthly_aggs.rename(columns={'RR_Month': 'FirstDayofMonth_Temp'})

        # Force garbage collection
        gc.collect()

        monthly_kpis_df = pd.merge(monthly_aggs, suip_monthly_aggs, on=['Email', 'FirstDayofMonth_Temp'], how='outer')
        monthly_kpis_df = pd.merge(monthly_kpis_df, rr_monthly_aggs, on=['Email', 'FirstDayofMonth_Temp'], how='outer')
        monthly_kpis_df = monthly_kpis_df.fillna(0).astype({'MonthlyIntakes': 'Int16', 'MonthlySUIP': 'Int16', 'MonthlyRR': 'Int16'})

        # Force garbage collection
        del monthly_aggs, suip_monthly_aggs, rr_monthly_aggs
        gc.collect()

        # Yearly group labels (start of each year)
        suip_filtered_df['SUIP_Year'] = pd.to_datetime(suip_filtered_df['SUIP Date'], errors='coerce').dt.to_period('Y').dt.start_time.dt.date
        lit_df['RR_Year'] = pd.to_datetime(lit_df['RR Date'], errors='coerce').dt.to_period('Y').dt.start_time.dt.date
        
        # Yearly aggregation using memory-efficient method
        yearly_aggs = memory_efficient_groupby_count(
            lit_df.loc[~lit_df['Status'].isin(['Referred Out'])],
            ['Email', 'FirstDayofYear_Temp'],
            'IntakeId',
            'YearlyIntakes'
        )
        
        suip_yearly_aggs = memory_efficient_groupby_count(
            suip_filtered_df,
            ['Email', 'SUIP_Year'],
            'Sign Up In Progress Date',
            'YearlySUIP'
        )
        suip_yearly_aggs = suip_yearly_aggs.rename(columns={'SUIP_Year': 'FirstDayofYear_Temp'})
        
        rr_yearly_aggs = memory_efficient_groupby_count(
            lit_df,
            ['Email', 'RR_Year'],
            'Retainer Received',
            'YearlyRR'
        )
        rr_yearly_aggs = rr_yearly_aggs.rename(columns={'RR_Year': 'FirstDayofYear_Temp'})

        # Force garbage collection
        gc.collect()

        yearly_kpis_df = pd.merge(yearly_aggs, suip_yearly_aggs, on=['Email', 'FirstDayofYear_Temp'], how='outer')
        yearly_kpis_df = pd.merge(yearly_kpis_df, rr_yearly_aggs, on=['Email', 'FirstDayofYear_Temp'], how='outer')
        yearly_kpis_df = yearly_kpis_df.fillna(0).astype({'YearlyIntakes': 'Int16', 'YearlySUIP': 'Int16', 'YearlyRR': 'Int16'})

        # Force garbage collection
        del yearly_aggs, suip_yearly_aggs, rr_yearly_aggs, suip_filtered_df
        gc.collect()

        # --- Create the final combined_df by strategically merging KPIs ---
        # Collect all unique (Email, CreatedDate) combinations that exist in *any* daily KPI aggregation
        # This ensures that even if only SUIP or RR occurred on a certain date, that date is included.
        all_daily_merge_keys = daily_kpis_df[['Email', 'CreatedDate']].drop_duplicates().reset_index(drop=True)

        combined_df = pd.merge(all_daily_merge_keys, daily_kpis_df, on=['Email', 'CreatedDate'], how='left')

        # Force garbage collection
        del daily_kpis_df
        gc.collect()

        # Merge Weekly KPIs ONLY where CreatedDate matches FirstDayofWeek_Temp
        # Weekly KPIs: Ensure all FirstDayofWeek_Temp dates are represented in CreatedDate
        all_weekly_dates = weekly_kpis_df[['Email', 'FirstDayofWeek_Temp']].drop_duplicates()
        all_weekly_dates = all_weekly_dates.rename(columns={'FirstDayofWeek_Temp': 'CreatedDate'})

        # Merge to ensure all weekly start dates are present in combined_df
        combined_df = pd.merge(combined_df, all_weekly_dates, on=['Email', 'CreatedDate'], how='outer')

        # Now merge the actual weekly KPI values
        weekly_display_df = weekly_kpis_df.copy()
        weekly_display_df = weekly_display_df.rename(columns={'FirstDayofWeek_Temp': 'CreatedDate'})

        combined_df = pd.merge(combined_df, weekly_display_df, 
                                on=['Email', 'CreatedDate'],
                                how='left', 
                                suffixes=('', '_weekly_temp'))
        combined_df = combined_df.drop(columns=[col for col in combined_df.columns if '_weekly_temp' in str(col)])

        # Force garbage collection
        del weekly_kpis_df, weekly_display_df
        gc.collect()

        # Monthly KPIs: Similar approach
        latest_monthly_dates = lit_df.groupby(['Email', 'FirstDayofMonth_Temp'])['CreatedDate'].max().reset_index()
        latest_monthly_dates = latest_monthly_dates.rename(columns={'CreatedDate': 'MonthlyDisplayDate'})
        
        # Ensure all FirstDayofMonth_Temp dates are represented in CreatedDate
        all_monthly_dates = monthly_kpis_df[['Email', 'FirstDayofMonth_Temp']].drop_duplicates()
        all_monthly_dates = all_monthly_dates.rename(columns={'FirstDayofMonth_Temp': 'CreatedDate'})
        combined_df = pd.merge(combined_df, all_monthly_dates, on=['Email', 'CreatedDate'], how='outer')
        monthly_display_df = pd.merge(monthly_kpis_df, latest_monthly_dates, 
                                        on=['Email', 'FirstDayofMonth_Temp'], how='left')
        monthly_display_df = monthly_display_df.rename(columns={'FirstDayofMonth_Temp': 'CreatedDate'})
        
        combined_df = pd.merge(combined_df, monthly_display_df, 
                            on=['Email', 'CreatedDate'],
                            how='left', 
                            suffixes=('', '_monthly_temp'))
        combined_df = combined_df.drop(columns=[col for col in combined_df.columns if '_monthly_temp' in str(col)])

        # Force garbage collection
        del monthly_kpis_df, monthly_display_df
        gc.collect()

        # Yearly KPIs: Similar approach
        latest_yearly_dates = lit_df.groupby(['Email', 'FirstDayofYear_Temp'])['CreatedDate'].max().reset_index()
        latest_yearly_dates = latest_yearly_dates.rename(columns={'CreatedDate': 'YearlyDisplayDate'})
        
        # Ensure all FirstDayofYear_Temp dates are represented in CreatedDate
        all_yearly_dates = yearly_kpis_df[['Email', 'FirstDayofYear_Temp']].drop_duplicates()
        all_yearly_dates = all_yearly_dates.rename(columns={'FirstDayofYear_Temp': 'CreatedDate'})
        combined_df = pd.merge(combined_df, all_yearly_dates, on=['Email', 'CreatedDate'], how='outer')
        
        # Merge yearly KPIs with their display dates
        yearly_display_df = pd.merge(yearly_kpis_df, latest_yearly_dates,
                                    on=['Email', 'FirstDayofYear_Temp'], how='left')
        yearly_display_df = yearly_display_df.rename(columns={'FirstDayofYear_Temp': 'CreatedDate'})
        
        combined_df = pd.merge(combined_df, yearly_display_df,
                            on=['Email', 'CreatedDate'],
                            how='left',
                            suffixes=('', '_yearly_temp'))
        
        # Cleanup temporary columns
        combined_df = combined_df.drop(columns=[col for col in combined_df.columns if '_yearly_temp' in str(col) or col == 'YearlyDisplayDate'])
        if 'MonthlyDisplayDate' in combined_df.columns:
            combined_df = combined_df.drop(columns=['MonthlyDisplayDate'])

        # Force garbage collection
        del yearly_kpis_df, yearly_display_df, lit_df
        gc.collect()

        # --- Apply Date-based Filters and Cleanup ---
        # Remove 90-day filter and 30-day daily KPI filter
        logging.info("[%s] No 90-day or 30-day filters applied. Processing all data from 2025-01-01.", prefix)
        
        # Filter for data from 2025-01-01 onwards
        start_date = datetime(2025, 1, 1).date()
        initial_rows_pre_date_filter = combined_df.shape[0]
        combined_df = combined_df[combined_df['CreatedDate'] >= start_date]
        rows_after_date_filter = combined_df.shape[0]
        logging.info("[%s] Shape of combined_df AFTER 2025-01-01 filter: %s", prefix, combined_df.shape)
        logging.info("[%s] Number of rows removed by 2025-01-01 filter: %s", prefix, initial_rows_pre_date_filter - rows_after_date_filter)

        # --- Remove rows with no metrics for any daily, weekly, monthly, or yearly KPI ---
        logging.info("[%s] Shape of combined_df BEFORE blank rows filter: %s", prefix, combined_df.shape)

        metric_value_cols = [
            'DailyIntakes', 'DailySUIP', 'DailyRR',
            'WeeklyIntakes', 'WeeklySUIP', 'WeeklyRR',
            'MonthlyIntakes', 'MonthlySUIP', 'MonthlyRR',
            'YearlyIntakes', 'YearlySUIP', 'YearlyRR'
        ]

        for col in metric_value_cols:
            if col in combined_df.columns:
                combined_df[col] = combined_df[col].astype(pd.Int16Dtype())
            else:
                combined_df[col] = pd.NA
                combined_df[col] = combined_df[col].astype(pd.Int16Dtype())

        initial_rows_pre_blank_filter = combined_df.shape[0]
        combined_df = combined_df[combined_df[metric_value_cols].notna().any(axis=1)]

        rows_after_blank_filter = combined_df.shape[0]
        logging.info("[%s] Shape of combined_df AFTER blank rows filter: %s", prefix, combined_df.shape)
        logging.info("[%s] Number of rows removed by blank rows filter: %s", prefix, initial_rows_pre_blank_filter - rows_after_blank_filter)

        combined_df = combined_df.reset_index(drop=True)

        # Ensure CreatedDate is a date type, it should be from the initial conversion but good to be explicit
        if 'CreatedDate' in combined_df.columns:
            combined_df['CreatedDate'] = pd.to_datetime(combined_df['CreatedDate']).dt.date

        # Apply renaming based on the prefix AFTER all processing is done
        combined_df = column_rename(combined_df, prefix)

        # Force garbage collection before returning
        gc.collect()

        return combined_df

    # Initialize Azure Blob Storage client
    try:
        body = req.get_json()
        logging.info(f"Received payload: {body}")

        logging.info("Starting script execution.")
        pi_prem_lit = ['Personal Injury', 'Premises Liability']
        wc_lit = ['Workers Compensation']
        mt_lit = ['Mass Tort']
        nh_lit = ['Nursing Home']
        pl_lit = ['Product Liability']

        # Define the order of prefixes
        litigation_configs = [
            (pi_prem_lit, "Pi_Prem__"),
            (wc_lit, "Wc_"),
            (mt_lit, "Mt_"),
            (nh_lit, "Nh_"),
            (pl_lit, "Pl_"),
        ]

        processed_dfs = []
        for lit_list, prefix in litigation_configs:
            logging.info("\n--- Processing data for litigations: %s with prefix '%s' ---", lit_list, prefix)
            processed_df = process_litigation(df_raw, lit_list, prefix)
            processed_dfs.append(processed_df)
            
            # Force garbage collection after each litigation type
            gc.collect()

        merge_keys = ['Email', 'CreatedDate', 'Manager Email']

        for df_to_merge in processed_dfs:
            # Ensure CreatedDate is a date type for consistent merging
            if 'CreatedDate' in df_to_merge.columns:
                df_to_merge['CreatedDate'] = pd.to_datetime(df_to_merge['CreatedDate'], errors='coerce').dt.date

            if 'Manager Email' not in df_to_merge.columns:
                df_to_merge['Manager Email'] = pd.NA

        if processed_dfs:
            final_df = processed_dfs[0]
            # Handle cases where Manager Email might be entirely NA in one of the initial DFs
            if final_df['Manager Email'].isna().all():
                temp_merge_keys = [k for k in merge_keys if k != 'Manager Email']
            else:
                temp_merge_keys = merge_keys

            for i in range(1, len(processed_dfs)):
                current_df = processed_dfs[i]
                if current_df['Manager Email'].isna().all():
                    current_temp_merge_keys = [k for k in merge_keys if k != 'Manager Email']
                else:
                    current_temp_merge_keys = merge_keys

                effective_merge_keys = list(set(temp_merge_keys) & set(current_temp_merge_keys))
                
                # Select columns to merge from current_df, ensuring merge keys are first
                current_df_cols_to_merge = effective_merge_keys + [col for col in current_df.columns if col not in effective_merge_keys]
                current_df_for_merge = current_df[current_df_cols_to_merge]

                final_df = pd.merge(final_df, current_df_for_merge, on=effective_merge_keys, how='outer', suffixes=('', '_drop'))
                # Remove duplicated columns from suffixes
                final_df = final_df.loc[:, ~final_df.columns.str.endswith('_drop')]
                temp_merge_keys = effective_merge_keys # Update merge keys for next iteration
                
                # Force garbage collection after each merge
                del current_df_for_merge
                gc.collect()
        else:
            logging.error("No DataFrames were processed. Exiting.")
            exit(1)

        # Clear processed_dfs to free memory
        del processed_dfs
        gc.collect()

        # Add this code after all the DataFrame merging is complete
        # Fill null values with 0 for KPI columns, but only when the specific column has at least one non-null value
        logging.info("Filling null values with 0 for KPI columns where the specific column has at least one non-null value.")

        # Group KPI columns by metric type (INTAKES, SUIP, RR) and time period (DAILY, WEEKLY, MONTHLY, YEARLY)
        kpi_groups = {}
        for col in final_df.columns:
            for prefix_val in [config[1] for config in litigation_configs]:
                if col.startswith(prefix_val):
                    kpi_suffix = col[len(prefix_val):]
                    # Check if the suffix matches an uppercase base KPI
                    if kpi_suffix in [k.upper() for k in base_kpi_cols]:
                        # Extract the time period and metric type
                        for time_period in ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY']:
                            for metric in ['INTAKES', 'SUIP', 'RR']:
                                if kpi_suffix == f"{time_period}{metric}":
                                    group_key = f"{time_period}{metric}"
                                    if group_key not in kpi_groups:
                                        kpi_groups[group_key] = []
                                    kpi_groups[group_key].append(col)
                                    break
                            else:
                                continue
                            break
                    break

        logging.info(f"Found {len(kpi_groups)} KPI groups: {list(kpi_groups.keys())}")

        # For each group, fill nulls with 0 only if that specific column has at least one non-null value
        for group_name, group_cols in kpi_groups.items():
            logging.info(f"Processing group: {group_name} with columns: {group_cols}")
            # Check each column individually
            for col in group_cols:
                # If this specific column has at least one non-null value, fill its nulls with 0
                if final_df[col].notna().any():
                    logging.info(f"Column {col} has non-null values, filling nulls with 0")
                    final_df[col] = final_df[col].fillna(0)
                else:
                    logging.info(f"Column {col} has no non-null values, leaving nulls as-is")

        # Ensure all KPI columns are Int16 type (only for columns that were filled)
        all_kpi_columns = [col for group_cols in kpi_groups.values() for col in group_cols]
        for col in all_kpi_columns:
            if col in final_df.columns:
                # Only convert to Int16 if the column has been filled (has non-null values)
                if final_df[col].notna().any():
                    final_df[col] = final_df[col].astype('Int16')

        logging.info("Successfully filled null values with 0 for KPI columns where appropriate.")

        # --- Optimized FirstDayofWeek, FirstDayofMonth, FirstDayofYear Generation ---
        logging.info("Generating universal FirstDayofWeek, FirstDayofMonth, FirstDayofYear columns.")
        if 'CreatedDate' in final_df.columns and 'Email' in final_df.columns:
            # Ensure 'CreatedDate' is in datetime format at the beginning
            final_df['CreatedDate'] = pd.to_datetime(final_df['CreatedDate'])
            # Initialize date columns with NaT for efficiency
            final_df['FirstDayofWeek'] = pd.NaT
            final_df['FirstDayofMonth'] = pd.NaT
            final_df['FirstDayofYear'] = pd.NaT
            # --- Step 1: Identify KPI columns (more efficient) ---
            weekly_kpi_cols = [col for col in final_df.columns if any('WEEKLY' in col.upper() and col.startswith(prefix) for prefix in [config[1] for config in litigation_configs])]
            monthly_kpi_cols = [col for col in final_df.columns if any('MONTHLY' in col.upper() and col.startswith(prefix) for prefix in [config[1] for config in litigation_configs])]
            yearly_kpi_cols = [col for col in final_df.columns if any('YEARLY' in col.upper() and col.startswith(prefix) for prefix in [config[1] for config in litigation_configs])]
            # --- Step 2: Compute the date groups and latest dates in a vectorized way ---
            # This is the core optimization, replacing the .iterrows() loop
            # Process Monthly KPIs
            # --- Corrected and more robust version of the optimized monthly KPI block ---
            if monthly_kpi_cols:
                has_monthly_kpi_mask = (final_df[monthly_kpi_cols] > 0).any(axis=1)
                monthly_df = final_df.loc[has_monthly_kpi_mask, ['Email', 'CreatedDate']].copy()
                if not monthly_df.empty:
                    monthly_df['MonthPeriod'] = monthly_df['CreatedDate'].dt.to_period('M')
                    latest_monthly_dates = monthly_df.groupby(['Email', 'MonthPeriod'])['CreatedDate'].transform('max')
                    refined_monthly_df_mask = (monthly_df['CreatedDate'] == latest_monthly_dates)
                    indices_to_update = monthly_df[refined_monthly_df_mask].index
                    if not indices_to_update.empty:
                        final_df.loc[indices_to_update, 'FirstDayofMonth'] = final_df.loc[indices_to_update, 'CreatedDate'].dt.to_period('M').dt.start_time.values
            # --- Corrected and more robust version of the optimized weekly KPI block ---
            if weekly_kpi_cols:
                # 1. Create a single mask for rows with any weekly KPI activity
                has_weekly_kpi_mask = (final_df[weekly_kpi_cols] > 0).any(axis=1)
                # 2. Get a temporary DataFrame for rows with weekly activity
                weekly_df = final_df.loc[has_weekly_kpi_mask, ['Email', 'CreatedDate']].copy()
                if not weekly_df.empty:
                    # 3. Create the week grouping column on the temporary DataFrame
                    weekly_df['WeekPeriod'] = weekly_df['CreatedDate'].dt.to_period('W-SAT')
                    # 4. Use transform to get the latest date for each group within the temporary DataFrame
                    latest_weekly_dates = weekly_df.groupby(['Email', 'WeekPeriod'])['CreatedDate'].transform('max')
                    # 5. Create a refined mask for the temporary DataFrame
                    # This comparison now works because both sides have the same index (from weekly_df)
                    refined_weekly_df_mask = (weekly_df['CreatedDate'] == latest_weekly_dates)
                    # 6. Use this mask to get the indices of the rows we want to update
                    indices_to_update = weekly_df[refined_weekly_df_mask].index
                    # 7. Use these indices to update the main final_df
                    if not indices_to_update.empty:
                        final_df.loc[indices_to_update, 'FirstDayofWeek'] = final_df.loc[indices_to_update, 'CreatedDate'].dt.to_period('W-SAT').dt.start_time.values
            # --- Corrected and fully optimized yearly KPI block ---
            if yearly_kpi_cols:
                # 1. Create a single mask for rows with any yearly KPI activity
                has_yearly_kpi_mask = (final_df[yearly_kpi_cols] > 0).any(axis=1)
                # 2. Get a temporary DataFrame for rows with yearly activity
                yearly_df = final_df.loc[has_yearly_kpi_mask, ['Email', 'CreatedDate']].copy()
                if not yearly_df.empty:
                    # 3. Create the year grouping column on the temporary DataFrame
                    yearly_df['Year'] = yearly_df['CreatedDate'].dt.year
                    # 4. Use transform to get the latest date for each group within the temporary DataFrame
                    latest_yearly_dates = yearly_df.groupby(['Email', 'Year'])['CreatedDate'].transform('max')
                    # 5. Create a refined mask for the temporary DataFrame
                    # This comparison now works because both sides have the same index (from yearly_df)
                    refined_yearly_df_mask = (yearly_df['CreatedDate'] == latest_yearly_dates)
                    # 6. Use this mask to get the indices of the rows we want to update
                    indices_to_update = yearly_df[refined_yearly_df_mask].index
                    # 7. Use these indices to update the main final_df
                    if not indices_to_update.empty:
                        final_df.loc[indices_to_update, 'FirstDayofYear'] = pd.to_datetime(final_df.loc[indices_to_update, 'CreatedDate'].dt.year.astype(str) + '-01-01').values
            # Clean up temporary dataframes
            del monthly_df, weekly_df, yearly_df
            gc.collect()
        else:
            logging.warning("CreatedDate or Email not found in final_df, cannot generate date columns.")

        # Force garbage collection
        gc.collect()

        # --- Expand user-date grid once (after all merges, before ordering) ---
        kpi_cols = [c for c in final_df.columns if any(t in c for t in ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'])]
        final_df = expand_user_date_grid(final_df, date_col='CreatedDate', user_col='Email', kpi_cols=kpi_cols)

        # --- Apply authoritative Manager Email mapping once at the end ---
        final_df['Email'] = final_df['Email'].astype('string').str.strip().str.lower()
        final_df = final_df.drop(columns=['Manager Email'], errors='ignore')\
        .merge(manager_map, on='Email', how='left')

        # --- Define and reorder columns ---
        # Define the exact desired order for the core columns
        ordered_cols = ['Email', 'Manager Email', 'CreatedDate', 'FirstDayofWeek', 'FirstDayofMonth', 'FirstDayofYear']

        # Define the desired order of prefixes for KPI columns
        prefix_order = ["Pi_Prem__", "Wc_", "Mt_", "Nh_", "Pl_"]  
        # Define the desired order of time periods
        time_period_order = ["DAILY", "WEEKLY", "MONTHLY", "YEARLY"]
        metric_order = ['INTAKES', 'SUIP', 'RR']  

        # Collect all KPI columns that are present in the final
        kpi_columns_present = [col for col in final_df.columns if any(col.startswith(p) for p in prefix_order) and any(t in col for t in time_period_order)]

        def sort_key(col_name):
            prefix_idx = -1
            time_idx = -1
            metric_idx = -1

            for i, p in enumerate(prefix_order):
                if col_name.startswith(p):
                    prefix_idx = i
                    suffix = col_name[len(p):]
                    break
            
            for i, t in enumerate(time_period_order):
                if t in suffix:
                    time_idx = i
                    break
            
            for i, m in enumerate(metric_order):
                if m in suffix:
                    metric_idx = i
                    break

            return (prefix_idx, time_idx, metric_idx)

        kpi_columns_present.sort(key=sort_key)

        # Combine the core columns with the sorted KPI columns
        final_column_order = ordered_cols + kpi_columns_present

        # Reorder the final DataFrame
        final_df = final_df[final_column_order]

        # Fill any remaining NA values in the final DataFrame with 0
        # This step is critical to ensure all non-date/email columns have a value
        for col in kpi_columns_present:
            final_df[col] = final_df[col].fillna(0)

        # Apply the specific column renames as in the original script
        final_df = final_df.rename(columns={'Pi_Prem__DAILYINTAKES': 'Pi_Prem_DAILYINTAKES', 'Pi_Prem__YEARLYSUIP':'Pi_Prem_YEARLYSUIP'})

        # Final garbage collection
        gc.collect()

        # Save the final result
        centrical_container = blob_service_client.get_container_client('centrical-data')
        eastern = pytz.timezone("US/Eastern")
        date = datetime.now(eastern).strftime("%Y-%m-%d_%H-%M")
        file_path = f"MM_141_CENTRICAL_AGENT_DAILY_MV_WW_{date}.csv"

        save_csv_to_azure(centrical_container, final_df, file_path)
        logging.info(f"Final DataFrame shape: {final_df.shape}")
        logging.info("Script execution completed successfully.")
        logging.info("Starting script execution.")


        return func.HttpResponse(
        body=json.dumps({"status": "success", "message": "Data processed and saved."}),
        mimetype="application/json",
        status_code=200
    )

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return func.HttpResponse(
            f"Error processing data: {e}",
            status_code=500
        )
