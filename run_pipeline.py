import time
from extract import fetch_all_cities
from transform import main
from load import load_data
from etl_analysis import run_analysis
 
def run_full_pipeline():
    # 1) Extract
    raw_file = fetch_all_cities()
    time.sleep(1)
 
    # 2) Transform
    main()
 
    # 3) Load
    load_data()
 
    # 4) Analysis
    run_analysis()
 
if __name__ == "__main__":
    run_full_pipeline()
    print("🎉 ETL pipeline completed successfully.")