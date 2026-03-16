
from datetime import datetime
import pandas as pd

#----------------- ENV------------------------------------

TODAY = pd.Timestamp("2023-01-01")  or datetime.today() #YOU pd.Timestamp("2025-10-01") / PS pd.Timestamp("2023-01-01")

PATH_MASTER = "./data/from_PS" #"./data/master"
PATH_TRANSACTION = "./data/from_PS" #"./data/transaction"
PATH_OUTPUT = "./data/output"

FILE_ITEM = "item_master.txt"
FILE_BOM = "bom_master.txt"
FILE_POLICY = "policy_master.txt"
FILE_DEMAND = "demand_orders.txt"
FILE_SUPPLY = "supply_orders.txt"
FILE_OH = "onhand.txt"



#----------------- MRP CONTEXT------------------------------------
## date có 3 dạng: slice (source/input) --> computation ---> display
## để dơn giản hóa thì : slice (chưa add vô) = computation = display
HORIZON_DATE = { "Daily" : [30, 60, 90, 120],
                 "Weekly": [12, 24, 36, 48]
                }
HORIZON_MODE_LIST = ["Daily","Weekly"]
HORIZON_MODE_CHOICE = "Weekly"

HORIZON_DEFAULT_DAYS = 30 #COMPUTATION: default: max_index < max_default thì kéo up-to, max_date >= max_default thì keep max_date
HORIZON_END_DAYS = TODAY + pd.Timedelta(days=HORIZON_DEFAULT_DAYS)


DEFAULT_POLICY = { "procurement_type" : "RAW",
           "policy_name" : "L4L",
           "lead_time" : 1,
           "safety_stock" : 0,
           "rounding_value" : 1,
           "MOQ" : 1,
           "cover_days" : 7,
           "week_day" : "Monday",
           "max_level" : 0,
}

POLICY_VALID_VALUES = {
    "procurement_type": {"RAW", "ASSEMBLY", "FNG"},
    "policy_name": {"L4L", "FOQ", "WEEKLY_CALENDAR", "COVER_DAYS", "MIN_MAX"},
    "week_day": {"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}
}



#----------------- SOURCE AND PROCESSING VALIDATIONS------------------------------------
# COLS: Expected columns for validation

### SOURCE DATA:
SOURCE_REQUIRED_COLS = { 
    # Transaction data
    "demand_orders": ["item_code", "date", "qty"],
    "onhand": ["item_code", "date", "qty"],
    "supply_orders": ["item_code", "date", "qty"],

    # Master data
    "bom_master": ["parent", "component", "qty_per"],
    "item_master": ["item_code", "desc", "uom", "vendor", "category"],
    "policy_master": ['item_code', *list(DEFAULT_POLICY.keys())],
}



### COMPUTATION
COLS_MRP_COMPUTE = ["Gross Req", "Scheduled Rcpt", "POH 1 (before LS)", "Net Req 1 (before LS)", 
                    "Lot size","POH 2 (after LS)","Net Req 2 (after LS)" ]

COLS_PLANNED_ORDER = ["item_code", "planned_qty", "receipt_date", "release_date", "order_type" , "urgency_status","lead_time"] #FIX ORDER
COLS_GROSS_REQUIREMENT = ["item_code", "gross_req_qty", "req_date","source", "source_item","urgency_status"] #FIX ORDER

### DISPLAY
BALANCE_COLS = ['Balance','Balance (+SR)', 'Balance(+SR+PR)', "Balance(+SR+1st PR)"] #FIX ORDER
SUM_COLS = ["Gross Req", "Scheduled Rcpt", 'planned_receipt', 'planned_release'] #FIX ORDER









