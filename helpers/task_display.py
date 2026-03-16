import pandas as pd

from config import (HORIZON_MODE_CHOICE, HORIZON_MODE_LIST , 
                    DEFAULT_POLICY, 
                    BALANCE_COLS, SUM_COLS, COLS_PLANNED_ORDER, 
                    COLS_GROSS_REQUIREMENT, SOURCE_REQUIRED_COLS,
                    TODAY, HORIZON_END_DAYS)

# 01. MRP DISPLAY



def step1_add_planned_col(input_mrp_df, input_planned_df, first_index):
    
    # ---------- reindex the main dataframe to include all dates ----------
    

    
    result_df = input_mrp_df.reindex(
    pd.date_range( first_index, HORIZON_END_DAYS, freq='D' )
    )
    
    # --- aggregate planned qty by release/receipt date then turn into dict ---
    item_col, qty_col, rei_date_col, rel_date_col = COLS_PLANNED_ORDER[:4] #'item','planned_qty','receipt_date','release_date'

    
    release_map = (
        input_planned_df.groupby(rel_date_col)[qty_col]
        .sum().to_dict()
    )
    receipt_map = (
        input_planned_df.groupby(rei_date_col)[qty_col]
        .sum().to_dict()
    )
    
    # --- map value of the columns to index ---

    rei_qty_col, rel_qty_col = SUM_COLS[2:] #["Gross Req", "Scheduled Rcpt", 'planned_receipt', 'planned_release']
    
    result_df[rei_qty_col] = 0
    result_df[rel_qty_col] = 0

    result_df[rei_qty_col] = result_df.index.to_series().map(receipt_map)
    result_df[rel_qty_col] = result_df.index.to_series().map(release_map)
    # --- fill na value and sorting ---
    result_df = result_df.fillna(0).sort_index().astype(int)
    
    
    return result_df

def step2_add_balance_col(input_mrp_df, on_hand: float):
    #
    col_raw_bal, col_sr_bal, col_sr_pr_bal, col_sr_1pr_bal = BALANCE_COLS
    col_gross , col_sr, col_p_reiv, col_p_rls= SUM_COLS
    
    
    
    # add day 0 inventory at 4 balance columns
    input_mrp_df.loc[input_mrp_df.index[0], BALANCE_COLS] = on_hand
    
    # Identify first planned receipt date (if any)
    
    first_pr_date = (
    input_mrp_df.loc[input_mrp_df[col_p_reiv] > 0].index.min()
    if (input_mrp_df[col_p_reiv] > 0).any()
    else None
    )
    
    # Iterate from 2nd row onward to fill value
    
    for i in range(1, len(input_mrp_df)):
        
        # Define row and index
        prev_row = input_mrp_df.iloc[i - 1]
        curr_row = input_mrp_df.iloc[i]
        curr_idx = input_mrp_df.index[i]
        # Find component value
        gross = curr_row[col_gross]
        sched = curr_row[col_sr]
        plan_r = curr_row[col_p_reiv]
        
        add_first_pr = plan_r if curr_idx == first_pr_date else 0
        # Compute balance
        input_mrp_df.at[curr_idx, col_raw_bal] = prev_row[col_raw_bal] - gross
        input_mrp_df.at[curr_idx, col_sr_bal] = prev_row[col_sr_bal] + sched - gross
        input_mrp_df.at[curr_idx, col_sr_pr_bal] = prev_row[col_sr_pr_bal] + sched + plan_r - gross
        input_mrp_df.at[curr_idx, col_sr_1pr_bal] = prev_row[col_sr_1pr_bal] + sched + add_first_pr - gross
    
    return input_mrp_df

def step3_MRP_display(input_mrp_df):
    
    if HORIZON_MODE_CHOICE not in HORIZON_MODE_LIST:
        print("Your input MODE_HORIZON IS WRONG !!! CHOOSE AGAIN")
        return None

    # ------------
    agg_dict = {**{c: 'sum' for c in SUM_COLS},
                **{c: 'last' for c in BALANCE_COLS}}

    
    ## ------------ HANDLE BEFORE ------------
    df_before = input_mrp_df[input_mrp_df.index < TODAY].copy()
    df_before['final_index'] = 'overdue'

    df_before = (
        df_before
        .groupby('final_index', as_index=True)
        .agg(agg_dict)
    )
    df_before.index.name = None
    
    ## ------------ HANDLE AFTER ------------
    
    
    
    df_total = input_mrp_df[input_mrp_df.index >= TODAY].copy() #CÂN NHẮC TOTAL = OVERDUE + AFTER
    df_total['final_index'] = 'total'
    
    df_total = (
        df_total
        .groupby('final_index', as_index=True)
        .agg(agg_dict)
    )
    df_total.index.name = None
    
    
    if HORIZON_MODE_CHOICE == "Daily":
        df_after = input_mrp_df[input_mrp_df.index >= TODAY].copy()
        
        iso = df_after.index.isocalendar()

        df_after['final_index'] = [
            # Condition 1,2 : Monday or first index (even if not Monday) → add week marker
            f"{d.strftime('%d/%m')} ,, W{w:02d}Y{y % 100}" if d.weekday() == 0 or d == df_after.index[0]
            # Condition 3: other days → plain date
            else d.strftime('%d/%m')
            for d, w, y in zip(df_after.index, iso.week, iso.year) #BẢN CHẤT VẪN LÀ TẠO COLUMNS
        ]
        df_after['final_index'] = df_after['final_index'].astype(str) + ","
        
        df_after = df_after.set_index('final_index')
        df_after.index.name = None

    elif HORIZON_MODE_CHOICE == "Weekly": 
        df_after = input_mrp_df[input_mrp_df.index >= TODAY].copy()
        
        df_after['week_number'] = df_after.index.to_series().dt.isocalendar().week
        df_after['year'] = df_after.index.to_series().dt.isocalendar().year % 100
        
        df_after['week_label'] = (
            "Y" + df_after['year'].astype(int).astype(str)
            +"W" + df_after['week_number'].astype(int).astype(str).str.zfill(2)
        )



        df_after = (
            df_after
            .groupby('week_label', as_index=True)
            .agg(agg_dict)
        )
        df_after.index.name = None
    
    else:
        return None
    
    ## APPENDING

    result_df = pd.concat([df_before, df_total ,df_after])
    result_df = result_df.T
    result_df = result_df.reset_index().rename(columns={'index': 'MRP_element'})
    
    return result_df
    
def step4_add_info(input_mrp_df, item_code, item_desc, on_hand, item_policy_param):
    # INPUT
    
    lt = item_policy_param.get("lead_time", DEFAULT_POLICY['lead_time'])
    ss = item_policy_param.get("safety_stock", DEFAULT_POLICY["safety_stock"])
    
    policy_name = item_policy_param.get("policy_name", DEFAULT_POLICY["policy_name"]) 
    rounding = item_policy_param.get("rounding_value", DEFAULT_POLICY["rounding_value"])
    moq = item_policy_param.get("MOQ", DEFAULT_POLICY["MOQ"])
    
    cover_days = item_policy_param.get("cover_days", DEFAULT_POLICY["cover_days"])
    week_day = item_policy_param.get("week_day", DEFAULT_POLICY["week_day"])
    max_level = item_policy_param.get("max_level", DEFAULT_POLICY["max_level"])
    
    # DICT
    info_dict = {'Item Code': item_code,
             'Description': item_desc,
             'Current stock': on_hand,
             'Lead Time': lt,
             'Safety Stock': ss,
             'Order Policy': policy_name,
             'Rounding Value / MOQ': f"[{rounding}/ {moq}]",
             'Cover Days / Week Day / Max Level': f"[{cover_days}/ {week_day}/ {max_level}]"
    }
    
    # Convert dict → DataFrame

    info_df = pd.DataFrame(list(info_dict.items()), columns=['Field', 'Field Value'])
        
    # REINDEX
    if len(info_df) != len(input_mrp_df):
        print(f"Error: Unmatching columns between mrp and info")
        return

        # max_len = max(len(info_df), len(input_mrp_df))
        # info_df = info_df.reindex(range(max_len)).fillna('')
        # input_mrp_df = input_mrp_df.reindex(range(max_len)).fillna('')

    input_mrp_df = pd.concat([info_df, input_mrp_df], axis=1)

    # input_mrp_df['item_code_index'] = item_code
    # input_mrp_df = input_mrp_df.set_index('item_code_index')
    # final_mrp_df_by_item.index.name = None
    
    return input_mrp_df


# 02. FINAL GROSS DEMAND

def append_demand_df(raw_demand_df, dependent_demand_df):
    
    col_item, col_qty, col_date = COLS_GROSS_REQUIREMENT[:3] #exploded df
    col_sales_item, col_sales_date, col_sales_qty= SOURCE_REQUIRED_COLS['demand_orders'] #sales demand
    #thu tu khac nhau giua dependent va independent demand nen phai sap xep lai
        #final cols ---> exploded df
    
    independent_demand_df = raw_demand_df.rename(columns={
        col_sales_date: col_date,
        col_sales_qty: col_qty
    }).assign(
        source="Sales_Order",
        source_item=None,
        urgency_status=None,
        demand_type="Independent"
    )
    dependent_demand_df = dependent_demand_df.assign(demand_type = "Dependent")

    # reorder columns
    desired_cols = [col_item, col_date] + [col for col in dependent_demand_df.columns if col not in [col_item, col_date]]
    independent_demand_df = independent_demand_df[desired_cols]
    dependent_demand_df = dependent_demand_df[desired_cols]
    
    # final results
    final_result = pd.concat([independent_demand_df, dependent_demand_df], ignore_index=True)
    
    return final_result


# 03. ORDER RECOMMENDATION

def order_rcmd_report(planned_order_df):
    
    item_col , qty_col, receipt_date_col, release_date_col, order_type_col, urgency_col, lt_col = COLS_PLANNED_ORDER
    
    tempt = planned_order_df.copy()
    #WEEKLY REPORT--------------------------------
    iso = tempt[receipt_date_col].dt.isocalendar()
    tempt["year"] = iso["year"]
    tempt["week_num"] = iso["week"]
    tempt["week_label"] = tempt["week_num"].astype(str).str.zfill(2) # Format the desired label (e.g., W01Y22)
    tempt["week_label"] = "W" + tempt["week_label"] + "Y" + tempt["year"].astype(str).str[-2:]
    
    weekly_planned_df = (
        pd.pivot_table(
            tempt,
            values=qty_col,
            index=[item_col, order_type_col],
            columns="week_label",
            aggfunc="sum",
            fill_value=0
        )
    ).reset_index()
    week_cols = [c for c in weekly_planned_df.columns if c.startswith("W")]
    week_cols = sorted(week_cols, key=lambda x: (x[-2:], x[1:3]))
    weekly_planned_df = weekly_planned_df[[item_col, order_type_col, *week_cols]]
    #URGENT REPORT--------------------------------
    urgent_report_df = (
    planned_order_df[planned_order_df[urgency_col] == "urgent"]
    .groupby(item_col, as_index=False)
    .agg({qty_col: "sum"})
    )

    data = { "raw_planned_df" : planned_order_df,
            "urgent_report_df" : urgent_report_df,
            "weekly_planned_df" : weekly_planned_df
    }
    return data
    

