
import pandas as pd
from pandas import DatetimeIndex
from collections import defaultdict, deque
from typing import DefaultDict, Dict, List, Optional, Tuple

from config import COLS_MRP_COMPUTE, COLS_PLANNED_ORDER , COLS_GROSS_REQUIREMENT, SOURCE_REQUIRED_COLS ,TODAY
from src.helper import round_up_to_multiple, date_range_index



def item_df_initializing(item_code: str, date_index: DatetimeIndex, 
                            on_hand: float, 
                            independent_demand_dict: DefaultDict[str, List[Dict]], 
                            global_dependent_dict: DefaultDict[str, List[dict]], 
                            sr_dict :DefaultDict[str, List[dict]]) -> pd.DataFrame :
    """
    - INPUT:
        - KHUNG: item_code, columns_list, item_index, 
        - VALUE: independent_demand_dict, global_dependent_dict, sr_dict
        - VALUE: on-hand
    - TASK:
        - 1. Tạo khung table: columns + index
        - 2. Seeding day0 inventory
        - 3. Seeding Gross demand (independent + dependent)
        - 4. Seeding Schedule receipt (supply_order)

    """
    
    gross_col = COLS_MRP_COMPUTE[0] # 'Gross Req'
    sr_col = COLS_MRP_COMPUTE[1] #'Scheduled Rcpt'
    poh1_col, poh2_col = COLS_MRP_COMPUTE[2], COLS_MRP_COMPUTE[5] #'POH 1 (before LS)' , 'POH 2 (after LS)'
    
    
    
    ## initiize pd.df and seeding inventory
    result_df = pd.DataFrame(0, index = date_index, columns= COLS_MRP_COMPUTE)
    day0 = date_index[0]
    result_df.at[day0, poh1_col] = on_hand
    result_df.at[day0, poh2_col] = on_hand
    

    
    
    ## seeding GROSS INDENPEDENT DEMAND
    for rec in independent_demand_dict.get(item_code, []):
        d = rec[ SOURCE_REQUIRED_COLS['demand_orders'][1] ] #item, date, qty
        qty = rec[ SOURCE_REQUIRED_COLS['demand_orders'][2] ]
        if d in result_df.index:
            result_df.at[d, gross_col] += int(qty)
    ## seeding GROSS DEPENDENT DEMAND
    for rec in global_dependent_dict.get(item_code, []):
        d = rec[ COLS_GROSS_REQUIREMENT[2]]  #["item_code", "gross_req_qty", "req_date","source", "source_item","urgency_status"]
        qty = rec[ COLS_GROSS_REQUIREMENT[1]]
        if d in result_df.index:
            result_df.at[d, gross_col] += int(qty)
    ## seeding SCHEDULE RECEIPT
    for rec in sr_dict.get(item_code, []):
        d = rec[ SOURCE_REQUIRED_COLS['supply_orders'][1]] #item, date, qty
        qty = rec[ SOURCE_REQUIRED_COLS['supply_orders'][2]]
        if d in result_df.index:
            result_df.at[d, sr_col] += int(qty)

    return result_df



def calculate_net_requirement(tp_item_df):
    """
    Return a df of result
    - only net requirement before lot size + before safety stock
    - tp_item_df indexed by datetime.date or pd.Timestamp
    """
    
    gross_col = COLS_MRP_COMPUTE[0]
    sched_col = COLS_MRP_COMPUTE[1]
    poh1_col = COLS_MRP_COMPUTE[2] #before LS
    net1_col = COLS_MRP_COMPUTE[3] #before LS
    
    first_day = tp_item_df.index.min()
    for curr_date in tp_item_df.index:
        # 1. GET CURRENT VALUE
        gross = int(tp_item_df.at[curr_date, gross_col]) if curr_date in tp_item_df.index else 0
        sched = int(tp_item_df.at[curr_date, sched_col]) if curr_date in tp_item_df.index else 0
        
    
        # HANDLING previous_date index and previous day on-hand
        
        if curr_date == first_day:
            onhand_prev = int(tp_item_df.at[first_day, poh1_col])
        else:
            prev = curr_date - pd.Timedelta(days=1)
            # If prev not in index (shouldn't happen), fallback to first day
            if prev not in tp_item_df.index:
                prev = first_day
    
            onhand_prev = int(tp_item_df.at[prev, poh1_col])
        #------------------
        
        # Calculating net value
        available = onhand_prev + sched
        usable = max(0, available)
        
        
        
        net = 0 if usable >= gross else gross - usable

        # 2. FILL / UPDATE
        # Net Requirement
        tp_item_df.at[curr_date, net1_col] = int(net) if curr_date != first_day else 0 #set up day 0 is 0

        # POH (skip if net < 0)
        tp_item_df.at[curr_date, poh1_col] = int(max(0, usable - gross)) if curr_date != first_day else onhand_prev #set up day 0 is 0
        
        
def calculate_lot_size(tp_item_df, safety_stock=0,
                       policy_name = "L4L", policy_param: dict = None):
    #GET columns
    gross_col = COLS_MRP_COMPUTE[0]
    sched_col = COLS_MRP_COMPUTE[1]
    poh1_col = COLS_MRP_COMPUTE[2] #before LS
    net1_col = COLS_MRP_COMPUTE[3] #before LS
    
    ls_col = COLS_MRP_COMPUTE[4] #lot size decision
    poh2_col = COLS_MRP_COMPUTE[5] #after LS
    net2_col = COLS_MRP_COMPUTE[6] #after LS

    
    
    
    #L4L + FIXED CASE 
    if policy_name in ["L4L","FOQ"]:
        
        for curr_date in tp_item_df.index[1:]:
            # 0. GET value for calc
            lot_size_decision_qty = 0
            gross = int(tp_item_df.at[curr_date, gross_col])
            sched = int(tp_item_df.at[curr_date, sched_col])
            onhand_prev = int(tp_item_df.at[curr_date - pd.Timedelta(days=1), poh2_col])
            
            # 1. UPDATE VALUE + FILL ROW
            inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
            poh2_qty = max(inventory_position,0)
            
            tp_item_df.at[curr_date, poh2_col] = poh2_qty
            tp_item_df.at[curr_date, net2_col] = inventory_position
            
            # 2. Do it 2nd time only when NET OCCUR
            if inventory_position < safety_stock:
                # no safety stock then inv_pos < 0 is OK
                # yes safety stock then below safetystock is OK --> split 2 TH net2_qty nega or positive
                # COMPUTE Lot size
                lot_size_decision_qty = abs(safety_stock - inventory_position)
                ##OLD WAYS: lot_size_decision_qty = abs(inventory_position) + safety_stock if inventory_position <=0 else abs(inventory_position - safety_stock) 
                
                """handle MOQ and Rouding value"""
                
                moq = policy_param.get("MOQ",1)
                lot_size_decision_qty = max(lot_size_decision_qty,moq)
                
                rounding_value = policy_param.get("rounding_value",1)
                lot_size_decision_qty = round_up_to_multiple(lot_size_decision_qty, rounding_value)
                """----------------------------------------"""
                # UPDATE inventory_position , poh2,net2
                inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
                poh2_qty = max(inventory_position,0) #do not minus
                
                # FILL ROW
                tp_item_df.at[curr_date, ls_col] = lot_size_decision_qty
                tp_item_df.at[curr_date, poh2_col] = poh2_qty
                tp_item_df.at[curr_date, net2_col] = inventory_position

    elif policy_name == "COVER_DAYS" :
        
        last_index = tp_item_df.index.max() #xài index[-1] nếu đã sort
        n_cover_days = policy_param.get("cover_days", 7)
        
        
        for curr_date in tp_item_df.index[1:]:
            # 0. GET value for calc
            lot_size_decision_qty = 0
            gross = int(tp_item_df.at[curr_date, gross_col])
            sched = int(tp_item_df.at[curr_date, sched_col])
            onhand_prev = int(tp_item_df.at[curr_date - pd.Timedelta(days=1), poh2_col])
            # 1. UPDATE VALUE + FILL ROW
            
            inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
            poh2_qty = max(inventory_position,0)
            
            tp_item_df.at[curr_date, poh2_col] = poh2_qty
            tp_item_df.at[curr_date, net2_col] = inventory_position
            
            # 2. Do it 2nd time only when NET OCCUR
            if inventory_position < safety_stock:
                end_calc_date = min(last_index, curr_date + pd.Timedelta(days=n_cover_days))
                
                lot_size_decision_qty = 0
                for curr_calc_date in date_range_index(curr_date, end_calc_date):
                    gross2 = int(tp_item_df.at[curr_calc_date, gross_col])
                    sched2 = int(tp_item_df.at[curr_calc_date, sched_col])
                    onhand_prev2 = int(tp_item_df.at[curr_calc_date - pd.Timedelta(days=1), poh2_col])
                    
                    #first lot_size
                    fake_lot_size = 0
                    inventory_position2 = onhand_prev2 + sched2 - gross2 + fake_lot_size
                    #2nd lot_size if net occur
                    if inventory_position2 < safety_stock:   
                        fake_lot_size = abs(safety_stock - inventory_position2) 
                        inventory_position2 = onhand_prev2 + sched2 - gross2 + fake_lot_size

                    poh2_qty2 = max(inventory_position2,0)
                    
                    tp_item_df.at[curr_calc_date, poh2_col] = poh2_qty2
                    tp_item_df.at[curr_calc_date, net2_col] = inventory_position2
                    
                    

                    lot_size_decision_qty += abs(fake_lot_size)
            
                """handle MOQ and Rouding value"""
                
                moq = policy_param.get("MOQ",1)
                lot_size_decision_qty = max(lot_size_decision_qty,moq)
                
                rounding_value = policy_param.get("rounding_value",1)
                lot_size_decision_qty = round_up_to_multiple(lot_size_decision_qty, rounding_value)
                """---------------------------------"""
                # UPDATE inventory_position , poh2,net2

                inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
                
                poh2_qty = max(inventory_position,0) #do not negative
                
                # FILL ROW
                tp_item_df.at[curr_date, ls_col] = lot_size_decision_qty
                tp_item_df.at[curr_date, poh2_col] = poh2_qty
                tp_item_df.at[curr_date, net2_col] = inventory_position
                
    elif policy_name == "WEEKLY_CALENDAR":
        weekday_list = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        weekday_parameter = policy_param.get("week_day","Monday")
        weekday_order_num = weekday_list.index(weekday_parameter)
        #print(weekday_order_num)
        last_index = tp_item_df.index.max()
        
        curr_date = tp_item_df.index[1] #get day 1 - today
        
        while curr_date < last_index + pd.Timedelta(days=1): #a. k duoc de dau bang, neu khong se infinite loop b. +1 de bao gom last_index
            
            # 0. finding next review (tuesday or last_index)
            next_review_raw = curr_date + pd.offsets.Week(weekday=weekday_order_num, n = 1)
            next_review_true = min(next_review_raw, last_index + pd.Timedelta(days=1)) #+1 de bao gom last_index if last_index xảy ra
            #print(f"current {curr_date} Raw next {next_review_raw}, True next {next_review_true}")

            lot_size_decision_qty = 0
            # 1. MAIN - LOOP dateX/thứ 3 tuần này cover tới t2 tuần sau
            for curr_calc_date in date_range_index(curr_date, next_review_true - pd.Timedelta(days=1)):
                # 0. GET value for calc
                
                gross = int(tp_item_df.at[curr_calc_date, gross_col])
                sched = int(tp_item_df.at[curr_calc_date, sched_col])
                onhand_prev = int(tp_item_df.at[curr_calc_date - pd.Timedelta(days=1), poh2_col])
                
                # 1. UPDATE VALUE + FILL ROW
                
                #1st lot size
                fake_lot_size = 0
                inventory_position = onhand_prev + sched - gross + fake_lot_size
                #2nd if net occur
                
                if inventory_position < safety_stock:
                    fake_lot_size = abs(safety_stock - inventory_position) 
                    inventory_position = onhand_prev + sched - gross + fake_lot_size
                
                poh2_qty = max(inventory_position,0)
                tp_item_df.at[curr_calc_date, poh2_col] = poh2_qty
                tp_item_df.at[curr_calc_date, net2_col] = inventory_position
                
                
                lot_size_decision_qty += abs(fake_lot_size)
                #print(f"Lot size of {curr_date} is {lot_size_decision_qty}")
                
                
            # 1.5 MAIN - IF lot size > 0, recalculation the LOOP window
            if lot_size_decision_qty > 0:
                """handle MOQ and Rouding value"""
                
                moq = policy_param.get("MOQ",1)
                lot_size_decision_qty = max(lot_size_decision_qty,moq)
                
                rounding_value = policy_param.get("rounding_value",1)
                lot_size_decision_qty = round_up_to_multiple(lot_size_decision_qty, rounding_value)
                """---------------------------------"""
                
                for curr_calc_date in date_range_index(curr_date, next_review_true - pd.Timedelta(days=1)):
                    gross = int(tp_item_df.at[curr_calc_date, gross_col])
                    sched = int(tp_item_df.at[curr_calc_date, sched_col])
                    onhand_prev = int(tp_item_df.at[curr_calc_date - pd.Timedelta(days=1), poh2_col])
                    # 1. UPDATE VALUE + FILL ROW, LOT SIZE DECISION AT DAY 1 of the loop
                    if curr_calc_date == curr_date:
                        inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
                    else:
                        inventory_position = onhand_prev + sched - gross + 0
                        
                    poh2_qty = max(inventory_position,0)

                    
                    tp_item_df.at[curr_calc_date, ls_col] = lot_size_decision_qty
                    tp_item_df.at[curr_calc_date, poh2_col] = poh2_qty
                    tp_item_df.at[curr_calc_date, net2_col] = inventory_position
                    
                    lot_size_decision_qty = 0 #reset to next day UPDATING
            
            # SKIP TO NEXT REVIEW   
                # using this if có dấu bang
                # if next_review_true == curr_date:
                #     break 
        
            curr_date = next_review_true
            
    elif policy_name == "MIN_MAX":
        
        s = safety_stock
        S = policy_param.get("max_level", 100000)
        
        for curr_date in tp_item_df.index[1:]:
            # 0. GET value for calc
            lot_size_decision_qty = 0
            gross = int(tp_item_df.at[curr_date, gross_col])
            sched = int(tp_item_df.at[curr_date, sched_col])
            onhand_prev = int(tp_item_df.at[curr_date - pd.Timedelta(days=1), poh2_col])
            
            # 1. UPDATE VALUE + FILL ROW
            inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
            poh2_qty = max(inventory_position,0)
            
            tp_item_df.at[curr_date, poh2_col] = poh2_qty
            tp_item_df.at[curr_date, net2_col] = inventory_position
            
            # 2. Do it 2nd time only when NET OCCUR
            if inventory_position < s and s < S:
                lot_size_decision_qty = abs(S - inventory_position)
                
                """handle MOQ and Rouding value"""
                
                moq = policy_param.get("MOQ",1)
                lot_size_decision_qty = max(lot_size_decision_qty,moq)
                
                rounding_value = policy_param.get("rounding_value",1)
                lot_size_decision_qty = round_up_to_multiple(lot_size_decision_qty, rounding_value)
                """----------------------------------------"""
                # UPDATE inventory_position , poh2,net2
                inventory_position = onhand_prev + sched - gross + lot_size_decision_qty
                poh2_qty = max(inventory_position,0) #do not minus
                
                # FILL ROW
                tp_item_df.at[curr_date, ls_col] = lot_size_decision_qty
                tp_item_df.at[curr_date, poh2_col] = poh2_qty
                tp_item_df.at[curr_date, net2_col] = inventory_position
                
                
                #ENSURE s < S
                if s >= S:
                    raise Exception(f"Your s {s} is greater than S {S} at XXX-item(chưa tìm cách bỏ vô đc).")
                
                
def calculate_planned_order(
    item_name: str, tp_item_df: "pd.DataFrame", 
    procurement_type: str, lead_time: int ) -> "pd.DataFrame":
    """
    - What: Order recommendation for the item
    - Task: Calculates planned order quantities. (Prod Ord / Pur Ord)
    - Inputs:
        - tp_item_df: DataFrame with input column [date, lot_size_decision_qty]
        - lead_time: 
        - procurement_type: to define purchase order / production order
        - today: date-like for 'URGENT' flag (defaults to local today)
    - Returns:
        - DataFrame with columns: ["item", "planned_qty", "receipt_date", "release_date", "lead_time_days" ,"type" , "urgency"]
    """
    # --- 0. OUTPUT INITILIZATION ---
    
    ## COLS_PLANNED_ORDER = ["item", "planned_qty", "receipt_date", "release_date" ,"type" , "urgency_status", "lead_time"]
    item_col = COLS_PLANNED_ORDER[0]
    qty_col = COLS_PLANNED_ORDER[1]
    receipt_date_col = COLS_PLANNED_ORDER[2]
    release_date_col = COLS_PLANNED_ORDER[3] 
    type_col = COLS_PLANNED_ORDER[4] 
    urgency_col = COLS_PLANNED_ORDER[5]
    lead_time_cols = COLS_PLANNED_ORDER[6]
    
    lot_size_col = COLS_MRP_COMPUTE[4]
    
    ## OUTPUT
    output_df = tp_item_df.copy()
    
    
    # --- 0. COMPUTE LOGIC ---
    
    output_df[item_col] = item_name
    output_df[receipt_date_col] = output_df.index #or df.reset_index().rename(columns={'index': 'timestamp'})
    output_df[release_date_col] = output_df[receipt_date_col] - pd.to_timedelta(lead_time, unit="D")
    output_df[lead_time_cols] = lead_time
    output_df.rename(columns={lot_size_col: qty_col}, inplace = True)
    
    
    type_mapping = {
    "FNG": "Production Order",
    "ASSEMBLY": "Production Order",
    "RAW" : "Purchase Order"
    }
    
    output_df[type_col] = type_mapping.get(procurement_type, "Purchase Order")
    
    # CÁCH 2 - NHƯ CẶC, QUÁ NẶNG
    # if procurement_type in ["FNG","ASSEMBLY"]:
    #     output_df[type_col] = "Production Order"
    # else:
    #     output_df[type_col] = "Purchase Order"
    # CÁCH 3: VECTORIZE, CHẠY C speed
    # output_df["type"] = np.where(
    # output_df["procurement_type"].isin(["FNG", "ASSEMBLY"]),
    # "Production Order",
    # "Purchase Order"
    
    output_df[urgency_col] = ""
    output_df.loc[output_df[release_date_col] <= TODAY , urgency_col] = "urgent"

    
    # REORDER COLUMNS
    desired_order = [item_col, qty_col] + [col for col in output_df.columns if col not in [item_col, qty_col]]
    output_df = output_df[desired_order]

    return output_df.reset_index(drop = True) #remove date_index


def exploding_parent_item(
    item_name: str, lead_time: int,
    bom_df : "pd.DataFrame", 
    item_planned_order_df: "pd.DataFrame", scheduled_receipt = None
    ) -> "pd.DataFrame":
    """
    run only when production orders (assembly/FNG)
    - What: Exploding child's gross demand from (PARENT , non-RAW) item
    - Task: Calculating all child's qty from parent's order 
    - Inputs: planned_order dataframe following column ["item", "planned_qty", "receipt_date", "release_date", "order_type" , "urgency_status","lead_time"]
    - Returns:
        - Child-Gross Requirement DataFrame with columns: ["item", "gross_req_qty", "req_date","source", "source_item","urgency_status"]
    """
    # 0. Get columns variables
    
    item_input_col , planned_qty_col, receipt_date_col, release_date_col, order_type_col, urgency_input_col, lt_col = COLS_PLANNED_ORDER
    item_output_col, req_qty_col, req_date_col, source_col, source_item_col, urgency_output_col = COLS_GROSS_REQUIREMENT
    
    


    # 1.1. Build adjacency map for fast lookup
    
    dict_bom = defaultdict(list)
    bom_cols = SOURCE_REQUIRED_COLS['bom_master'] #parent, component, qty
    
    for product, child, qty in zip(bom_df[bom_cols[0] ], bom_df[bom_cols[1] ], bom_df[ bom_cols[2]]):
        dict_bom[product].append( (child, qty ))
    # BAD PERFORMANCE VERSION
    # for _, row in bom_df.iterrows():
    #     dict_bom[row['parent']].append( 
    #                               (row['component'], row['qty_per']) 
    #                             )
    
    # 1.2 Transform schedule receipt --> schedule release,,,  from dict(list) into pd.df
    scheduled_receipt_item = scheduled_receipt.get(item_name, [])
    
    #tempt_records = [row for rec in scheduled_receipt.values() for row in rec]
    
    scheduled_order_df = pd.DataFrame(scheduled_receipt_item)
    
    if scheduled_order_df.empty is not True: # if schedule receipt exist do below,,,, else skip this part
        scheduled_order_df[item_input_col] = scheduled_order_df["item_code"].astype(str)
        scheduled_order_df[planned_qty_col] = scheduled_order_df["qty"].astype(float)
        scheduled_order_df[receipt_date_col] = scheduled_order_df["date"]
        
        scheduled_order_df[release_date_col] = scheduled_order_df[receipt_date_col] - pd.to_timedelta(lead_time, unit='d')
        scheduled_order_df[order_type_col] = ""
        scheduled_order_df[urgency_input_col] = ""
        scheduled_order_df[lt_col] = lead_time
    
    
    
    # 1.3 Append Planned Order + Schedule Release
    sources = {
    "planned_order": item_planned_order_df,
    "scheduled_receipt": scheduled_order_df
    }
    
    df_all = pd.concat(
        [df.assign( source =name) for name, df in sources.items() ]
        
    )
    
    # 2. Row-by-row computation for PLANNED ORDER + SCHEDULED_RELEASE
    
    exploded_dict = defaultdict(list)
    exploded_list = []

    for _, row in df_all.iterrows():
        parent_item = row[item_input_col]
        parent_qty = row[planned_qty_col]
        release_date = row[release_date_col]
        urgency = row[urgency_input_col]
        source = row[source_col]
        
        # each Row item: find ALL childs in BOM ---> writing result
        for child_item, child_qty_per_parent in dict_bom.get(parent_item, []):
            
            records = {
                item_output_col: child_item,
                req_date_col: release_date,
                req_qty_col: parent_qty * child_qty_per_parent,
                source_col: source,
                source_item_col:parent_item,
                urgency_output_col: urgency
            }
            
            
            exploded_list.append(records)
            exploded_dict[child_item].append(records)
            


    exploded_df = pd.DataFrame(exploded_list)
    
    return exploded_dict, exploded_df