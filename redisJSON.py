from nfl_data_objects import NFLDataUploader, NFLDataRetriever, Analyzer
import time
                
if __name__ == "__main__":
    start_time = time.time()
    # uploader = NFLDataUploader()
    # uploader.nfl_data_to_csv(uploader.fetch_nfl_dataframe([2023]))
    # uploader.upload_csv_to_redisjson()
    # check1 = time.time()
    # print(f"\nTime taken to upload to redis: {check1 - start_time}\n")
    
    retriever = NFLDataRetriever()
    analyzer = Analyzer()
    analyzer.analysis(retriever.get_pbp_data())
    analyzer.graphical_analysis(retriever.get_pbp_data())
    
    check3 = time.time()
    print(f"\nTime taken to analyze: {check3 - start_time}\n")
