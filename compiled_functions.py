
# Load all the EPC files clean and save as csvs

# needs pandas and geopandas

## Needs UPRN data
## uprn = pd.read_csv('./../Data/Ordinance_Survey/osopenuprn_202302.csv')
## uprn['UPRN'] = uprn['UPRN'].astype('float64')
import glob
import pandas as pd

def load_clean_epc(path_to_epc, path_save_cleaned, columns_keep, UPRN_data):
    #list_paths = glob.glob("../Data/Energy_Performance_Certificate/all-domestic-certificates/*")
    import glob
    import pandas as pd

    list_paths = glob.glob(path_to_epc)
    
    for i in range(len(list_paths)):
        path = list_paths[i] + '/certificates.csv'
        name = list_paths[i][-20:]
        epc = pd.read_csv(path, usecols=columns_keep) # columns = ['TOTAL_FLOOR_AREA','UPRN','LODGEMENT_DATE']
        # clean the uprn and add
        epc['UPRN'] = epc['UPRN'].astype('float64')
        epc2 = epc.merge(UPRN_data, on = 'UPRN',how = 'left')
        # clean duplicates
        epc2['LODGEMENT_DATE'] = pd.to_datetime(epc2['LODGEMENT_DATE'], format='%Y-%m-%d')
        epc2 = epc2.sort_values(by="LODGEMENT_DATE").drop_duplicates(subset=["UPRN"], keep="last")
        epc2 = epc2.loc[epc2.groupby('UPRN').LODGEMENT_DATE.idxmax()]
        # save to new folder
        epc2.to_csv(str(path_save_cleaned +  name + '.csv')) # path_save_cleaned = './../Data/Energy_Performance_Certificate/cerificates_processed/'
    
    return('done')

def load_compile_epc(path_to_cleaned):

    list_paths2 = glob.glob(path_to_cleaned) #path_to_cleaned = "./../Data/Energy_Performance_Certificate/cerificates_processed/*.csv"

    data = pd.DataFrame()
    for i in range(len(glob.glob(path_to_cleaned))):
        x = pd.read_csv(list_paths2[i])
        data = pd.concat([data,x])

    data = gpd.GeoDataFrame(data, geometry = gpd.points_from_xy(data.LONGITUDE,data.LATITUDE), crs = "EPSG:4326")

    return(data)



def extract_data_from_epc(path_to_certificates, columns, uprn_data_path, save_to_folder):
    # load paths
    uprn = pd.read_csv(uprn_data_path)
    list_paths = glob.glob(path_to_certificates)

    # foor loop to extract all the data
    for i in range(len(list_paths)):
        path = list_paths[i] + '/certificates.csv'
        #  define names
        name = list_paths[i][-20:]
        # 
        epc = pd.read_csv(path, usecols=columns)
        # correct the UPRN
        epc['UPRN'] = epc['UPRN'].astype('float64')
        # merge the data
        epc2 = epc.merge(uprn, on = 'UPRN',how = 'left')
        # drop the duplicates
        epc2['LODGEMENT_DATE'] = pd.to_datetime(epc2['LODGEMENT_DATE'], format='%Y-%m-%d')
        epc2 = epc2.sort_values(by="LODGEMENT_DATE").drop_duplicates(subset=["UPRN"], keep="last")
        epc2 = epc2.loc[epc2.groupby('UPRN').LODGEMENT_DATE.idxmax()]
        # write cleaned data
        epc2.to_csv(str(save_to_folder + name + '.csv'))


def load_all_cleaned_certificates(list_of_paths):
    list_of_paths = glob.glob(list_of_paths)
    data = pd.DataFrame()
    for i in range(len(list_of_paths)):
        x = pd.read_csv(list_of_paths[i])
        data = pd.concat([data,x])
    return(data)    


def clean_year(data):

    # extract the year of build
    data['CONSTRUCTION_AGE_BAND'] = data['CONSTRUCTION_AGE_BAND'].replace('England and Wales: ', '',regex=True).replace('before ', '',regex=True)
    data['CONSTRUCTION_AGE_BAND'] = data['CONSTRUCTION_AGE_BAND'].replace('NO DATA!', np.nan).replace('INVALID!', np.nan)
    data['CONSTRUCTION_AGE_BAND'] = data['CONSTRUCTION_AGE_BAND'].str[:4]
    data['CONSTRUCTION_AGE_BAND'] = data['CONSTRUCTION_AGE_BAND'].fillna(0).astype(int)
    data['CONSTRUCTION_AGE_BAND'] = np.where(data['CONSTRUCTION_AGE_BAND'] < 1600, np.nan, data['CONSTRUCTION_AGE_BAND'])
    return(data)

def rating_to_number(data):
    # energy rating
    data.CURRENT_ENERGY_RATING[data.CURRENT_ENERGY_RATING.isin(['INVALID!'])] = np.nan
    dict_rating = {'A':'1','B':'2','C':'3','D':'4','E':'5','F':'6','G':'7'}
    data['energy_rating_numbered'] = data['CURRENT_ENERGY_RATING'].copy()
    data['energy_rating_numbered'].replace(dict_rating, inplace=True)
    data['energy_rating_numbered'] = data.energy_rating_numbered.fillna(0).astype(int)
    data.energy_rating_numbered.unique()
    return(data)