


import pandas as pd
import numpy as np

import os
import math
from collections import defaultdict, deque
from datetime import datetime


from config import (PATH_MASTER, PATH_TRANSACTION, PATH_OUTPUT, 
                    FILE_ITEM, FILE_BOM, FILE_POLICY, FILE_DEMAND, FILE_SUPPLY, FILE_OH,
                    SOURCE_REQUIRED_COLS
                    
)






# ------------------- AAA ----------------------------------------
def round_up_to_multiple(q, multiple):
    if multiple <= 1:
        return int(q)
    return int(((q + multiple - 1) // multiple) * multiple)
# ------------------- AAA ---------------------------------------------------
def date_range_index(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date, freq="D")


# ------------------- AAA ---------------------------------------------------
def compute_levels(bom, item_list):
    "create a list of item sorted based on bom's true level"
    
    PARENT_COL, COMP_COL = ["parent", "component"]
    
    
    ## 1.1 Create dict bom
    dict_bom = defaultdict(list)
    for _, r in bom.iterrows():
        dict_bom[r[PARENT_COL]].append(r[COMP_COL])

    ## 1.2 Find Bom level for each item
    level = {}
    queue = deque()
    for item in item_list:
        level[item] = 0
        queue.append(item)
        
    while queue:
        p = queue.popleft()
        for c in dict_bom.get(p, []):
            if c not in level or level[c] < level[p] + 1:
                level[c] = level.get(p, 0) + 1
                queue.append(c)
                
    # 2. Sorting based on bom LEVEL
    ordered = sorted(list(set(item_list)), 
                     key=lambda x: level.get(x, 0)) #parents/low lv first
    
    
    
    print("List of ALL ITEM LIST , sorted by true bom level:")
    print("------FINAL_LV: ",dict(sorted(level.items(), key=lambda x: x[1])))
    return ordered

# ------------------- AAA ---------------------------------------------------

def find_min_max_date(item_code: str, 
                      independent_demand_dict, 
                      global_dependent_dict, 
                      sr_dict):
    """
    - Task : Find the earliest and latest date across independent demand, dependent demand, and SR data
    for a given item_code.
    - Note: If all three sources are missing or contain no valid dates → return (None, None).
    - Time horizon rules: 
    """
    
    # Source configs: (expected_date_column, dict_reference)
    date_sources = [
        ("date", independent_demand_dict),
        ("req_date", global_dependent_dict),
        ("date", sr_dict),
    ]

    def safe_index(src_dict, key: str, col: str) -> pd.Index:
        """Safely extract a pandas Index of valid datetime values for the item_code."""
        df_data = src_dict.get(key)
        if not df_data:
            return pd.Index([], dtype="datetime64[ns]")

        df = pd.DataFrame(df_data)
        if col not in df.columns or df[col].dropna().empty:
            return pd.Index([], dtype="datetime64[ns]")

        dates = pd.to_datetime(df[col], errors="coerce").dropna()
        if dates.empty:
            return pd.Index([], dtype="datetime64[ns]")

        return pd.Index(dates.dt.normalize().unique())

    # Step 1: Collect all indexes
    indexes = [safe_index(src, item_code, col) for col, src in date_sources]

    # Step 2: Union all date indexes efficiently
    # pd.Index.union() keeps unique + sorted
    combined_index = indexes[0]
    for idx in indexes[1:]:
        combined_index = combined_index.union(idx)

    # Step 3: Validate and compute range
    if combined_index.empty:
        print(f"[WARN] Item '{item_code}' → no valid dates found in any source.")
        return None, None

    start_date = combined_index.min()
    end_date = combined_index.max()
    
    # valid xxx
    if pd.isna(start_date) or pd.isna(end_date):
        print(f"[WARN] Item '{item_code}' → contains only invalid date values.")
        return None, None
    
    
    return start_date, end_date



# ------------------- AAA ---------------------------------------------------

class PolicyManager: #PURE safety_policy func nằm ở file jupitor notebook
    def __init__(self, policy_dict, default_policy, valid_values):
        self.policy_dict = policy_dict
        self.default_policy = default_policy
        self.valid_values = valid_values
        
        
        
    def is_missing(self, v): 
        """Return True if the value is considered empty / invalid."""
        return (
            v is None
            or (isinstance(v, float) and math.isnan(v))
            or (isinstance(v, str) and v.strip() == "")
            or pd.isna(v)
        )

    def safe_policy(self, item):
        merged = {**self.default_policy, **self.policy_dict.get(item, {})}
        cleaned = {}
        

        
        for k, v in merged.items():
            #Handle all case: None, NaN, "nan", "", etc.
            if self.is_missing(v):
                cleaned[k] = self.default_policy[k]
                continue
        
            # String case: normalization & validation
            if isinstance(v, str):
                v_clean = v.strip().upper()
                if k in self.valid_values:
                    if v_clean in self.valid_values[k]:
                        cleaned[k] = v_clean.title() if k == "week_day" else v_clean
                    else:
                        cleaned[k] = self.default_policy[k] #value not in valid ----> just use default
                else:
                    cleaned[k] = v #key not in valid ----> maybe numeric col
            else:
                cleaned[k] = v #value not string --> maybe numeric col
                
        
        policy_name = cleaned['policy_name']
        key_cv, key_wd, key_minmax = ["cover_days", "week_day", "max_level"]
        if policy_name == "COVER_DAYS":
            cleaned[key_wd] = np.nan
            cleaned[key_minmax] = np.nan
        elif policy_name == "MIN_MAX":
            cleaned[key_cv] = np.nan
            cleaned[key_wd] = np.nan
        elif policy_name == "WEEKLY_CALENDAR":
            cleaned[key_minmax] = np.nan
            cleaned[key_cv] = np.nan
        else:
            cleaned[key_cv] = np.nan
            cleaned[key_wd] = np.nan
            cleaned[key_minmax] = np.nan
            
        
        return cleaned
        
        
    def get(self, item):
        """Public access method."""
        return self.safe_policy(item)



# ------------------- LOADER SOURCE DATA ----------------------------------------
def source_loader():
    """
    - Load all master and transaction CSV files into pandas DataFrames.
    - Returns a dictionary grouped by category.
    """
    print("------------------Start Loading-------------------")
    
    
    def _read_csv(folder, filename, parse_dates=None):
        """Internal helper for reading CSVs safely."""
        file_path = os.path.join(folder, filename)
        
        try:
            if filename.endswith(".txt"):
                df = pd.read_csv(file_path, sep="\t", parse_dates=parse_dates, encoding="utf-8-sig")
            else:
                
                df = pd.read_csv(file_path, parse_dates=parse_dates)
            print(f"[INFO] Loaded {filename:<20} → {len(df):>5,} rows")
            return df

        except FileNotFoundError:
            print(f"[WARN] File not found: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Failed to load {filename}: {e}")
            return pd.DataFrame()
        


    # Transaction-level inputs
    
    demand_order_df = _read_csv(PATH_TRANSACTION, FILE_DEMAND, ["date"])
    onhand_df = _read_csv(PATH_TRANSACTION, FILE_OH, ["date"])
    supply_order_df = _read_csv(PATH_TRANSACTION, FILE_SUPPLY, ["date"])
    # Master-level inputs
    bom_df = _read_csv(PATH_MASTER, FILE_BOM)
    item_master_df = _read_csv(PATH_MASTER, FILE_ITEM)
    policy_master_df = _read_csv(PATH_MASTER, FILE_POLICY)
    
    # Structure result in a nested dictionary for clarity
    
    data = {
        "transaction": {
            "demand_orders": demand_order_df,
            "onhand": onhand_df,
            "supply_orders": supply_order_df,
        },
        "master": {
            "bom_master": bom_df,
            "item_master": item_master_df,
            "policy_master": policy_master_df,
        }
    }
    # Validate required columns
    print("\n[INFO] Validating column integrity...")
    for group, tables in data.items():
        for name, df in tables.items():
            if df.empty:
                print(f"[WARN] {name:<15} → EMPTY DataFrame, skipped validation.")
                continue
            
            expected_cols = SOURCE_REQUIRED_COLS.get(name, [])
            actual_cols = df.columns.tolist()
            

            # Normalize column names (case-insensitive)
            expected_lower = [c.lower() for c in expected_cols]
            actual_lower = [c.lower() for c in actual_cols]

            # Detect missing and extra columns
            missing_cols = [c for c in expected_cols if c.lower() not in actual_lower]
            extra_cols = [c for c in actual_cols if c.lower() not in expected_lower]
            
            if missing_cols or extra_cols:
                print(f"[WARN] {name:<15} → Column mismatch detected:")
                if missing_cols:
                    print(f"        ├── Missing columns : {missing_cols}")
                if extra_cols:
                    print(f"        └── Extra columns   : {extra_cols}")
            else:
                print(f"[OK]   {name:<15} → Columns validated ✓")
    
    print("[INFO] ✅ Input data loaded successfully.")
    
    return data

# ------------------- COMPUTE/PREPARE SOURCE_DATA INTO INPUT_DATA ----------------------------------------
def prepare_input(list_of_df):
    
    
    demand_df = list_of_df['transaction']['demand_orders']
    onhand_df = list_of_df['transaction']['onhand']
    supply_df = list_of_df['transaction']['supply_orders']
    bom_df = list_of_df['master']['bom_master']
    item_df = list_of_df['master']['item_master']
    policy_df = list_of_df['master']['policy_master']

    
    #------------------------------------
    items = sorted(set(bom_df["parent"])
               .union(set(bom_df["component"]))
               .union(set(demand_df["item_code"])) 
               .union(set(supply_df["item_code"]))
               )
    items_ordered = compute_levels(bom = bom_df, item_list= items)
    
    #------------------------------------
    demand_by_item = defaultdict(list)
    sr_by_item = defaultdict(list)
    
    for _, row in demand_df.iterrows() :
        item_code = row["item_code"]
        demand_by_item[item_code].append({
            "item_code": item_code,
            "date": row["date"],
            "qty": float(row["qty"])
        })
    for _, row in supply_df.iterrows() :
        item_code = row["item_code"]
        sr_by_item[item_code].append({
            "item_code": item_code,
            "date": row["date"],
            "qty": float(row["qty"])
    })
    #------------------------------------
    
    onhand_dict = onhand_df.set_index("item_code").to_dict(orient="index")
    item_master_dict = item_df.set_index("item_code").to_dict(orient="index")
    policy_dict = policy_df.set_index("item_code").to_dict(orient="index")
    
    data = {
        "item_ordered": items_ordered,
        "demand_by_item": demand_by_item,
        "sr_by_item": sr_by_item,
        "onhand_dict": onhand_dict,
        "item_master_dict": item_master_dict,
        "policy_dict": policy_dict,
        "bom_df": bom_df,

    }
    
    return data


# ------------------- WRITE ----------------------------------------

def writting_result(data:dict):
    # Create timestamp folder
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(PATH_OUTPUT, timestamp)
    os.makedirs(output_dir, exist_ok=True) #create
    print(f"START WRITING RESULT AT FOLDER {PATH_OUTPUT}")
    
    # Create custom func for writing
    def _write_dict(d, prefix=""):
        for key, value in d.items():
            if isinstance(value, pd.DataFrame):
                filename = f"{prefix}{key}.csv" if not prefix else f"{prefix}_{key}.csv"
                filepath = os.path.join(output_dir, filename)
                value.to_csv(filepath, index=True)
                print(filepath)
            elif isinstance(value, dict):
                _write_dict(value, prefix=f"{prefix}{key}" if not prefix else f"{prefix}_{key}")
            else:
                print(f"[SKIP] {prefix}{key}: Not a DataFrame or dict")
                    
    # DO WRITING  
    _write_dict(data)
    
    
