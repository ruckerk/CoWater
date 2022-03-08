import math,sys,os,datetime, io, zipfile

import requests
from requests.structures import CaseInsensitiveDict

import pandas as pd
#import ray
#os.environ["MODIN_ENGINE"] = "ray"
#import modin.pandas as pd
#from modin.config import NPartitions

import numpy as np
from pyproj import Proj, transform, CRS
from shapely.geometry import Polygon, Point, LineString
from pyproj import Proj, Transformer, CRS


import streamlit as st
import streamlit.components.v1 as components

from tempfile import NamedTemporaryFile
import tempfile

def Pt_Distance(pt1,pt2):
    R = 6373*1000*3.28084
    lon1 = math.radians(pt1[0])
    lat1 = math.radians(pt1[1])
    lon2 = math.radians(pt2[0])
    lat2 = math.radians(pt2[1])
    dlon = lon2-lon1
    dlat = lat2-lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return(distance)

def Pt_Bearing(pt1,pt2):
    #Bearing from pt1 to pt2
    R = 6373*1000*3.28084
    lon1 = math.radians(pt1[0])
    lat1 = math.radians(pt1[1])
    lon2 = math.radians(pt2[0])
    lat2 = math.radians(pt2[1])
    X = math.cos(lat2)*math.sin(lon2-lon1)
    Y = math.cos(lat1)*math.sin(lat2)-math.sin(lat1)*math.cos(lat2)*math.cos(lon2-lon1)
    B = math.atan2(X,Y)
    B = math.degrees(B)
    return B
    
def GetKey(df,key):
    # returns list of matches to <key> in <df>.keys() as regex search
    return df.iloc[0,df.keys().str.contains('.*'+key+'.*', regex=True, case=False,na=False)].keys().to_list()

def WaterDataPull(LAT=40.5832238,LON=-104.0990673,RADIUS=10):
    
    headers = {
        'Accept': 'application/zip',
    }

    params = (
        ('mimeType', 'xlsx'),
        ('zip', 'yes'),
    )

    json_data = {
        'within': 30,
        'lat': str(LAT),
        'long': str(LON),
        'siteType': [
            'Well',
            'Subsurface',
            'Facility',
            'Aggregate groundwater use',
            'Not Assigned',
        ],
        'sampleMedia': [
            'Water',
            'water',
            'Other',
            'No media',
        ],
        'characteristicName': [
            'Fixed dissolved solids',
            'Total dissolved solids',
            'Total solids',
            'Solids',
            'Dissolved solids',
            'Total fixed solids',
            'Fixed suspended solids',
            'Percent Solids',
            'Total suspended solids',
            'Salinity',
        ],
        'startDateLo': '01-01-1900',
        'startDateHi': datetime.datetime.now().strftime("%d-%m-%Y"),
        'dataProfile': 'resultPhysChem',
        'providers': [
            'NWIS',
            'STEWARDS',
            'STORET',
        ],
    }

    #response = requests.get('https://www.waterqualitydata.us/data/Result/search', headers=headers, params=params, stream = True)
    r_data = requests.post('https://www.waterqualitydata.us/data/Result/search', headers=headers, params=params, json=json_data)
    #r_station = requests.post('https://www.waterqualitydata.us/data/Station/search', headers=headers, params=params, json=json_data)

    r_station = requests.get("https://www.waterqualitydata.us/data/Station/search?within=" + str(RADIUS) + "&lat=" +  str(LAT)+'&long=' + str(LON) + "&siteType=Well&siteType=Subsurface&siteType=Facility&siteType=Aggregate%20groundwater%20use&siteType=Not%20Assigned&sampleMedia=Water&sampleMedia=water&sampleMedia=Other&sampleMedia=No%20media&characteristicName=Total%20dissolved%20solids&characteristicName=Dissolved%20solids&characteristicName=Total%20solids&characteristicName=Total%20suspended%20solids&characteristicName=Fixed%20dissolved%20solids&characteristicName=Fixed%20suspended%20solids&characteristicName=Solids&characteristicName=Percent%20Solids&characteristicName=Total%20fixed%20solids&characteristicName=Salinity&startDateLo=01-01-1900&startDateHi=01-01-2030&mimeType=xlsx&zip=yes&providers=NWIS&providers=STEWARDS&providers=STORET")
    
    #zipfile = ZipFile(BytesIO(r.content))
    #f = zipfile.namelist()[0]
    #pd.read_excel(zipfile.open(f,mode = 'r')).keys()
    
    
    # Note: original query string below. It seems impossible to parse and
    # reproduce query strings 100% accurately so the one below is given
    # in case the reproduced version is not "correct".
    #response = requests.post('https://www.waterqualitydata.us/data/Result/search?mimeType=csv&zip=yes', headers=headers, json=json_data)
    return r_data,r_station

### MAIN SCRIPT###

st.title('Spatial query for Colorado aquifer water samples!')
st.text('Find sample data near your location.')
st.text('Built by Kurt Rucker:Li linknkedIn: https://www.linkedin.com/in/kurt-rucker')
st.text('Source Code and Docs: https://github.com/ruckerk/CoWater')

st.title('Specify Location')
LAT_IN = st.text_input("Latitude: ",39.742043)
LONG_IN = st.text_input("Longitude: ",-104.991531)
EPSG_ENTRY = st.text_input("EPSG Coordinate System: ",4269)
RUNLABEL = st.text_input("Project Label: ",'AquiferData')       

LAT_IN = float(LAT_IN)
LONG_IN = float(LONG_IN)
EPSG_ENTRY = int(EPSG_ENTRY)
                 
MINDEPTH = 000
MAXDEPTH= 6000

OUTFILE = 'WaterReport_'+RUNLABEL+'_'+datetime.datetime.now().strftime("%d%m%Y")+'.csv'

EPSG_USGS = 4269 #NAD87
#EPSG_ENTRY = 4267 # UTM
transformer = Transformer.from_crs(EPSG_ENTRY, EPSG_USGS,always_xy =True)

#LONG_IN = -104.0990673
#LAT_IN = 40.5832238

(LONG2,LAT2) = transformer.transform(LONG_IN,LAT_IN)

r1,r2 = WaterDataPull(LAT_IN,LONG_IN,25)

zf = zipfile.ZipFile(io.BytesIO(r1.content))
f = zf.namelist()[0]
df = pd.read_excel(zf.open(f,mode = 'r'))

zf = zipfile.ZipFile(io.BytesIO(r2.content))
f = zf.namelist()[0]
df2 = pd.read_excel(zf.open(f,mode = 'r'))

LOCATIONS = df2['MonitoringLocationIdentifier'].unique()

df = df.loc[(df.CharacteristicName.str.contains('solid',case=False)) & (df.CharacteristicName.str.contains('dissolve',case=False))]
df = df.loc[df['ResultMeasure/MeasureUnitCode'].str.contains('mg')==True]
df = df.loc[df['ActivityMediaSubdivisionName'].str.contains('Ground',case=False)==True]

LONG_KEYS = df.keys()[df.keys().str.contains('longitude',case=False)].to_list()
LAT_KEYS = df.keys()[df.keys().str.contains('latitude',case=False)].to_list()

df2['PTS']=list(df2[['LongitudeMeasure','LatitudeMeasure']].to_records(index=False))

df2['Distance'] = df2['PTS'].apply(Pt_Distance,pt2=(LONG2,LAT2))
df2['Bearing'] = df2['PTS'].apply(Pt_Bearing,pt2=(LONG2,LAT2))

df3 = df.merge(df2[['MonitoringLocationIdentifier','WellDepthMeasure/MeasureValue','Distance','Bearing']],left_on='MonitoringLocationIdentifier',right_on='MonitoringLocationIdentifier',how='outer')
df3 = df3.loc[df3['ResultMeasureValue'].dropna().index]

df2 = df2.loc[df2['MonitoringLocationIdentifier'].isin(df3['MonitoringLocationIdentifier'])]

#DATA = df2.loc[(df2[GetKey(df2,'depth.*value')].max(axis=1)<=MAXDEPTH) & (df2[GetKey(df2,'depth.*value')].max(axis=1)>=MINDEPTH)].index
DATA = df2.loc[:,'MonitoringLocationIdentifier'].isin(df3['MonitoringLocationIdentifier']).index

#DATA = df2.loc[DATA,'Distance'].nsmallest(50).index
DATA = df2.loc[DATA].groupby(by='MonitoringLocationIdentifier')['Distance'].min().nsmallest(1000).index
DATA = list(DATA)

#DATA = df2.loc[DATA,'MonitoringLocationIdentifier'].unique()
DATA_LOCS = df2.loc[df2['MonitoringLocationIdentifier'].isin(DATA) & df2['Distance']>0,['MonitoringLocationIdentifier','Distance','Bearing']]

OUTCOLS = ['MonitoringLocationIdentifier'
           ,'ActivityStartDate'
           ,'CharacteristicName'
           ,'ResultMeasureValue'
           ,'ResultMeasure/MeasureUnitCode']
OUTCOLS = OUTCOLS + GetKey(df3,'depth.*value')

RESULT = df3.loc[(df3['MonitoringLocationIdentifier'].isin(DATA)),OUTCOLS]
#RESULT = RESULT.drop(['Distance', 'Bearing'],axis=1)
RESULT = RESULT.merge(DATA_LOCS,how='outer',on = 'MonitoringLocationIdentifier')
#Mask = RESULT.loc[RESULT.Distance.isna()].index
#LOCS = RESULT.loc[Mask,'MonitoringLocationIdentifier']
#LOCS = LOCS.drop_duplicates()
#RESULT.loc[RESULT['MonitoringLocationIdentifier'].isin(LOCS) and RESULT['Distance']>0]
#RESULT.Distance.isna()

st.dataframe(RESULT.sort_values('Distance', ascending=True))

#RESULT.sort_values('Distance', ascending=True).to_csv(OUTFILE)

###df.MonitoringLocationIdentifier
###df2.MonitoringLocationIdentifier
##if True:
##    import matplotlib.pyplot as plt
##    #ax=df2.plot(x='LongitudeMeasure',y='LatitudeMeasure',kind = 'scatter',color = 'silver',marker='.')
##
##    Long = GetKey(df2,'Long.*Meas.*')
##    Lat = GetKey(df2,'Lat.*Meas.*')
##    m = df2.MonitoringLocationIdentifier.isin(RESULT.MonitoringLocationIdentifier)
##    ax = df2.plot(x=Long, y= Lat, kind = 'scatter', color = 'silver', marker = '.')
##    ax.scatter(x = df2.loc[m, Long], y=df2.loc[m,Lat], color = 'lightblue', marker = '.')
##    #ax.scatter(df2.loc[m,Long],df2.loc[m,Lat],color = 'grey', marker = 'o')
##
##    
##    ax.plot(LONG2,LAT2,color='tomato',marker='o')
##    ax.text(LONG2,LAT2,RUNLABEL,color='red')
##
##
### input lat lon & epsg
### input depth limit
##
### plotly radius and depth sliders showing distribution of data points
### return list of nearest sample results inside depth interval
##
### nad 83 input = EPSG4269
### nad27 EPSG 4267
##

