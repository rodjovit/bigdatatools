import redis
import nfl_data_py as nfl
import pandas as pd
import matplotlib.pyplot as plt

"""
This class is responsible for uploading NFL data to RedisJSON
"""
class NFLDataUploader:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_conn = redis.Redis(host=redis_host, port=redis_port)
        self.nfl = nfl

        """
        Fetches NFL data and returns a DataFrame
        
        Parameters:
        seasons (list): List of seasons to fetch data for
        
        Returns:
        pd.DataFrame: DataFrame containing NFL data
        """
    def fetch_nfl_dataframe(self, seasons):
        return pd.DataFrame(self.nfl.import_pbp_data(seasons))

        """
        Saves NFL data to a CSV file
        
        Parameters:
        data (pd.DataFrame): DataFrame containing NFL data
        
        Returns:
        None
        """    
    def nfl_data_to_csv(self, data):
        data.to_csv('pbp_data.csv', index=False)
        print(f"Data saved to pbp_data.csv successfully.")

        """
        Uploads NFL data from a CSV file to RedisJSON
        
        Parameters:
        None
        
        Returns:
        None
        """
    def upload_csv_to_redisjson(self):
        data = pd.read_csv('pbp_data.csv')
        for index, row in data.iterrows():
            # Using the index as part of the key for simplicity; consider a more meaningful key for production
            key = f"play_id:{index}"
            mapping = {k: str(v) if v is not None and not isinstance(v, bool) else "" for k, v in row.to_dict().items()}
            self.redis_conn.hset(key, mapping=mapping)
            
            
    """
    This class is responsible for retrieving NFL data from RedisJSON
    """
class NFLDataRetriever:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        """
        Displays all data in RedisJSON as a DataFrame
        
        Parameters:
        None
        
        Returns:
        None
        """
    def display_all_data_as_dataframe(self):
        data = []
        keys = self.redis_conn.keys("play_id:*")
        for key in keys:
            data.append(self.redis_conn.hgetall(key))
        # Decode byte strings to regular strings for DataFrame column names
        data = pd.DataFrame(data).applymap(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        data.columns = [col.decode('utf-8') if isinstance(col, bytes) else col for col in data.columns]
        print(data)
        
        """
        Returns all data in RedisJSON as a DataFrame
        
        Parameters:
        None
        
        Returns:
        pd.DataFrame: DataFrame containing all data in RedisJSON
        """
    def get_pbp_data(self):
        data = []
        keys = self.redis_conn.keys("play_id:*")
        for key in keys:
            data.append(self.redis_conn.hgetall(key))
        # Decode byte strings to regular strings for DataFrame column names
        data = pd.DataFrame(data).applymap(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        data.columns = [col.decode('utf-8') if isinstance(col, bytes) else col for col in data.columns]
        return data
        
    """
    This class is responsible for analyzing NFL data
    """ 
class Analyzer:

    """
    Analyzes NFL data and prints statistics
    
    Parameters:
    data (pd.DataFrame): DataFrame containing NFL data
    
    Returns:
    None
    """
    def analysis(self, data):
        header = "****************************"
        # Filter data for the Eagles
        print("\n",header)
        print("\n The Philadelphia Eagles 2023 Screen Pass Analysis\n")
        print("\n",header)
        data = pd.DataFrame(data)
        eagles23 = data[data['posteam'] == 'PHI']
        eagles23_screens = eagles23[eagles23['play_type'] == 'pass']
        eagles23_screens = eagles23_screens[eagles23_screens['season_type'] == 'REG']
        eagles23_screens = eagles23_screens[eagles23_screens['route'] == 'SCREEN']
        eagles23_screens['yards_gained'] = eagles23_screens['yards_gained'].astype(float)
        eagles23_screens['epa'] = eagles23_screens['epa'].astype(float)
        # fix data type in success column
        eagles23_screens['success'] = eagles23_screens['success'].astype(float)
        # print(data.info())
        # print(eagles23_screens.info())
        print(eagles23_screens.filter(items=['home_team','away_team','week','yards_gained','epa','success','posteam','play_type','season_type','route']))
        print("Mean of Yards Gained: ", round(eagles23_screens['yards_gained'].mean(),3))
        print("Median of Yards Gained: ", round(eagles23_screens['yards_gained'].median(), 3))
        print("Standard Deviation of Yards Gained: ", round(eagles23_screens['yards_gained'].std(),3))
        print("Min of Yards Gained: ", eagles23_screens['yards_gained'].min())
        print("Max of Yards Gained: ", eagles23_screens['yards_gained'].max())
        print("Count of Screens: ", eagles23_screens['yards_gained'].count())
        print("1st Quartile: ", eagles23_screens['yards_gained'].quantile(.25))
        print("2nd Quartile: ", eagles23_screens['yards_gained'].quantile(.50))
        print("3rd Quartile: ", eagles23_screens['yards_gained'].quantile(.75))
        print("Total Yards Gained: ", eagles23_screens['yards_gained'].sum())
        print("Total EPA of Screen Passes: ", round(eagles23_screens['epa'].sum(),3))
        print("Average EPA of Screen Passes: ", round(eagles23_screens['epa'].mean(),3))
        print("Count of Successful Screen Plays: ", eagles23_screens['success'].sum())
        print("Success Rate: ", (eagles23_screens['success'].sum()/eagles23_screens['success'].count()*100), "%")
        