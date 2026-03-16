import pandas as pd
import time
from collections import defaultdict, deque


from helpers.helper import *
from helpers.task_mrp import *
from helpers.task_display import *


from config import (DEFAULT_POLICY, POLICY_VALID_VALUES,
                    COLS_MRP_COMPUTE, COLS_PLANNED_ORDER, COLS_GROSS_REQUIREMENT, SOURCE_REQUIRED_COLS,
                    TODAY
)



def mrp_computation(list_of_df):
    """Note
    - list_of_df: prepared/computed DFs , which ready for mrp computation

    """
    print("------------------Start MRP Computation-------------------")
    # INPUT PREPARE
    
    item_ordered = list_of_df['item_ordered']
    demand_by_item = list_of_df['demand_by_item']
    sr_by_item = list_of_df['sr_by_item']
    onhand_dict = list_of_df['onhand_dict']
    item_master_dict =list_of_df['item_master_dict']
    policy_dict = list_of_df['policy_dict']
    bom_df = list_of_df['bom_df']

    
    # OUTPUT INITIALIZATION
    
    planned_order_list = []
    dependent_demand_list = []
    global_dependent_demand_dict = defaultdict(list)
    tp_map = {}
    
    
    # MAIN LOOP
    for item in item_ordered:
        print(f"----------------------------- START LOOP MRP of {item} ")
        on_hand = float( onhand_dict.get(item,{}).get("qty", 0) )
        
        pm = PolicyManager(policy_dict, DEFAULT_POLICY, POLICY_VALID_VALUES)
        
        policy_param = pm.get(item)
            #sure always có dict-value pair nên search by key is safety
        policy_name = policy_param["policy_name"]
        ss_qty = policy_param["safety_stock"]
        lead_time = policy_param["lead_time"]
        procurement_type = policy_param["procurement_type"]
    
        #STEP 0: Initialization : 
        ## indexing
        start_date, end_date = find_min_max_date(item_code = item, 
                                                independent_demand_dict= demand_by_item, 
                                                global_dependent_dict = global_dependent_demand_dict, 
                                                sr_dict= sr_by_item )
            #SKIP mechanism : (state_date, end_date) = (None, None) when item no demand, no supply -----> no need to run mrp computation
        if start_date is None or end_date is None:
            print(f"SKIP LOOP {item}, because of no date_index")
            continue
            
        my_index = date_range_index(start_date - pd.Timedelta(days=1), end_date)
        
        ## tạo khung item_df (index + col) + seeding (inventory, demand[ind, dep], supply)
        tp_map[item] = item_df_initializing(item_code = item, date_index= my_index, on_hand = on_hand,
                                    independent_demand_dict= demand_by_item, 
                                    global_dependent_dict = global_dependent_demand_dict, 
                                    sr_dict= sr_by_item )
            #SKIP mechanisum: if dp_map[item] is empty df --> skip the item
        
        #STEP 1 : CALCULATION NET REQUIREMENT
        current_df = tp_map[item] #truy cap vao current DF of TP_MAP ---> xem xet có nên COPY hay không ????
        calculate_net_requirement(current_df)
        #STEP 2: FINDING LOT SIZE
        calculate_lot_size(tp_item_df=current_df, safety_stock= ss_qty, 
                           policy_name= policy_name, policy_param=policy_param)
        
        #STEP 3: DEFINE PROCUREMENT TYPE AND SCHEDULING --> Planned Order
        
        ### --- PREPARE INPUT ---
        lot_size_col = COLS_MRP_COMPUTE[4] 
        input_df = current_df.loc[:,lot_size_col].copy() #this is pd.
        
        input_df = input_df.to_frame(name = lot_size_col) #convert into pd.dataframe
        input_df = input_df[input_df[lot_size_col] > 0]
        
        ### --- RUN logic ---
            ### if table have no lot size ----> SKIP
        if input_df is None or input_df.empty:
            print(f"SKIP LOOP {item}: no supply signal (lot_size from tp_item_df)")
            continue
        tempt_planned_order_df = calculate_planned_order(item_name = item, tp_item_df= input_df, 
                                                    procurement_type= procurement_type, lead_time= lead_time )

        planned_order_list.append(tempt_planned_order_df)
        
        #STEP 4: EXPLODE QTY BASED on planned order and update the tp_df gross_requirement
        # condition : Explode if at least ONE signal exists, Skip only when BOTH are missing

        has_po = (tempt_planned_order_df is not None and not tempt_planned_order_df.empty) #no-object and empty pd.df
        has_sr = bool(sr_by_item) and any(sr_by_item.values()) #no value exists and phantom key
        if not(has_po or has_sr): #if do not have 1 or 2 = both is empty
            print(f"SKIP LOOP {item}: no supply signals (PO, SR)")
            continue
        # tên khác là calculate_child_depend_demand
        if procurement_type != "RAW": # run only when production order
            #--- get result ---
            tempt_exploded_dict, tempt_exploded_df = exploding_parent_item(item_name= item, 
                                                item_planned_order_df= tempt_planned_order_df, scheduled_receipt= sr_by_item, 
                                                bom_df = bom_df, lead_time= lead_time )
                                            

            # --- write 
            #### for display: into list of df
            dependent_demand_list.append(tempt_exploded_df)
            #### for computation: into dict of (list of dict)
            for item, records in tempt_exploded_dict.items(): #{ item_code : list of records }
                global_dependent_demand_dict[item].extend(records)
            
     
    #OUTPUT
    planned_order_df = pd.concat(planned_order_list, ignore_index= True, sort = False)
    dependent_demand_df = pd.concat(dependent_demand_list, ignore_index= True, sort = False)
    
    data = { "planned_order_df": planned_order_df,
            "dependent_demand_df": dependent_demand_df,
            "mrp_result_dict": tp_map,
        
    }

    return data



def mrp_display(list_of_df, mrp_result, raw_demand_df):
    print("------------------Start MRP Display processing-------------------")
    # INPUT PREPARE
    
    item_ordered = list_of_df['item_ordered']
    demand_by_item = list_of_df['demand_by_item']
    sr_by_item = list_of_df['sr_by_item']
    onhand_dict = list_of_df['onhand_dict']
    item_master_dict =list_of_df['item_master_dict']
    policy_dict = list_of_df['policy_dict']
    bom_df = list_of_df['bom_df']
    
    planned_order_df = mrp_result['planned_order_df']
    dependent_demand_df = mrp_result['dependent_demand_df']
    mrp_result_dict = mrp_result['mrp_result_dict']
    
    
    # 01. MRP DISPLAY ------------------------------------------------------------------------------------------
    slicing_mrp_col = COLS_MRP_COMPUTE[:2] #GR + SR
    slicing_order_col = COLS_PLANNED_ORDER[:4] #'item_code','planned_qty','receipt_date','release_date'
    
    planned_order_per_item = dict(tuple(planned_order_df.groupby(slicing_order_col[0])))
    list_result = []

        #GLOBAL INDEX for concanate each item_df
    
    min_date_planned = ( planned_order_df[ slicing_order_col[3] ].min() if not planned_order_df.empty 
                        else TODAY )
    min_date_mrp = min(
        (df.index.min() for df in mrp_result_dict.values() if not df.empty),
        default= TODAY
        )
    
    
    min_date_overall = min(
    d for d in [min_date_planned, min_date_mrp] if pd.notna(d)
    )
    
    for item in item_ordered:
        print(f"----------------------------- START LOOP of {item} ")
        
        mrp_computed_df = (mrp_result_dict
                           .get(item, pd.DataFrame(columns=slicing_mrp_col))
                           .loc[:, slicing_mrp_col].copy()
        ) #mrp_computed_df = mrp_result_dict[item].loc[:, slicing_mrp_col].copy()
        
        planned_df = (planned_order_per_item
                           .get(item, pd.DataFrame(columns=slicing_order_col))
                           .loc[:, slicing_order_col].copy()
        )
        
        if mrp_computed_df.empty and planned_df.empty: # empty = object pd.df but has no row
            print(f"empty df of computed mrp and planned of {item}, SKIP THE LOOP")
            continue  # skip item entirely
            
        on_hand = float( onhand_dict.get(item,{}).get("qty", 0) )
        item_desc = str(item_master_dict.get(item,{}).get("desc", "AAA") )
        pm = PolicyManager(policy_dict, DEFAULT_POLICY, POLICY_VALID_VALUES)
        policy_param = pm.get(item)
        
        # 1. -------  Run MRP DISPLAY COMPUTATION — returns a *new* DataFrame ------- 
            #Index of each item = all item --> for concat
            #min_index = of all_computed_mrp and planned_df
            #max_index = GLOBAL VAR
        
        result_df = step1_add_planned_col(input_mrp_df = mrp_computed_df,  
                                          input_planned_df = planned_df,
                                          first_index = min_date_overall)

        result_df = step2_add_balance_col(input_mrp_df = result_df, on_hand = on_hand)

        result_df = step3_MRP_display(input_mrp_df = result_df)
        
        if result_df is None:
            print(f"Error at step3_MRP_display of {item}, BREAK THE LOOP")
            break


        adding_info_dict = dict(item_code = item, item_desc = item_desc , 
                                on_hand = on_hand, item_policy_param = policy_param )
        result_df = step4_add_info(input_mrp_df = result_df, **adding_info_dict)
    
    
        # 2. ------- Append to list of df for FINAL CONCAT -------------
        result_df['Item Code'] = item # Tag for traceability
        result_df = result_df.set_index('Item Code')
        list_result.append(result_df)
        
        
        
    final_mrp_display_df = pd.concat(list_result, ignore_index= False)
    
    
    # 02. FINAL GROSS DEMAND ------------------------------------------------------------------------------------------
    final_demand_df = append_demand_df(raw_demand_df, dependent_demand_df)

    
    
    # 03. PLANNED ORDER ------------------------------------------------------------------------------------------
    
    order_recommendation_report = order_rcmd_report(planned_order_df)
    
    #-------
    data = {"mrp_display": final_mrp_display_df.reset_index(),
            "full_demand": final_demand_df,
            "order_recommendation_report": order_recommendation_report
    }
    return data
    
    
    

def main():
    #LOAD SOURCE
    source_data = source_loader()

    #PREPARE/ COMPUTE INPUT (for mrp computation)
    input_data = prepare_input(source_data)
    
    
    #RUN MRP computation + display/reporting
    result = mrp_computation(input_data)
    
    
    input_display = dict(list_of_df= input_data, mrp_result=result, raw_demand_df= source_data['transaction']['demand_orders'])
    display_result = mrp_display(**input_display)
    #WRITING RESULT
    

    writting_result(data = display_result)
    

if __name__ == "__main__":
    
    start_time = time.time()
    main()
    print(f"Execution time: {time.time() - start_time:.2f} seconds")
    
    