from src import mrp_engine
from datetime import datetime


import time






if __name__ == "__main__":
    print("\n"*3)
    print("-"*50)
    start_clock = datetime.now()
    start = time.perf_counter()
    # RUN LOGIC
    mrp_engine.main()
    
    end = time.perf_counter()
    end_clock = datetime.now()
    print(f"Started at: {start_clock}")
    print(f"Finished at: {end_clock}")
    print(f"Execution time: {end - start:.2f} seconds")