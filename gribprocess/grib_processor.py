import sys
sys.path.append('..')

import os
import ntpath
from gribprocess.utils_s3_grib_download import s3_gfs_download

from datetime import datetime, timedelta

from pathlib import Path

def is_hour_between(start, end, now):
    is_between = False

    is_between |= start <= now <= end
    is_between |= end < start and (start <= now or now <= end)

    return is_between


def date_decide(date0):
    hr=int(date0.strftime('%H'))
    if is_hour_between(20, 2, hr):
        date=date0- timedelta(days=1)
        date_fmt=date.strftime('%Y%m%d')
        srn='18'
    elif is_hour_between(2, 8, hr):
        date=date0
        date_fmt=date.strftime('%Y%m%d')
        srn='00'
    elif is_hour_between(8, 14, hr):
        date=date0
        date_fmt=date.strftime('%Y%m%d')
        srn='06'
    else:
        date=date0
        date_fmt=date.strftime('%Y%m%d')
        srn='12'
    return date_fmt,srn


def foldercreator(path):
   """
    creates a folder

    Parameters
    ----------
    path : folder path
            
    Returns
    -------
    creates a folder
    """
   if not os.path.exists(path):
        os.makedirs(path)

def path_leaf(path):
    """
    Get the name of a file without any extension from given path

    Parameters
    ----------
    path : file full path with extension
    
    Returns
    -------
    str
       filename in the path without extension

    """
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)



def gcs_uploader(params,gfs_180,bucket):
    filename=path_leaf(gfs_180)
    blob = bucket.blob(f'{params.startdate}{params.run}/{params.short_name}/{filename}')
    blob.upload_from_filename(gfs_180)




def variable_json_creator():
    temp={'varname':'temperature','s3_name':'temperature','label':'Temperature','shrt_name':'temp','unit':'Degree Celcius','gfs_level':'2_m','gfs_var':'tmp','gfs_fmt':'TMP','color':{'red':153,'green':0,'blue':0},'filter_string':':TMP:2 m above ground'}
    prate={'varname':'precipitation','s3_name':'precipitation','label':'Precipitation','shrt_name':'prate','unit':'mm.hr-1','gfs_level':'surface','gfs_var':'prate','gfs_fmt':'PRATE','color':{'red':0,'green':153,'blue':0},'filter_string':':PRATE:surface:'}
    wspd={'varname':'wind_speed','s3_name':'windspeed','label':'Wind speed','shrt_name':'wspd','unit':'m.s-1','gfs_level':'surface','gfs_var':'gust','gfs_fmt':'GUST','color':{'red':0,'green':153,'blue':0},'filter_string':':GUST:surface:'}
    wdir={'varname':'wind_direction','s3_name':'winddirection','label':'Wind direction','shrt_name':'wdir','unit':'angle in 360 degree','gfs_level':'10_m','gfs_var':'uvgrd','gfs_fmt':'UGRD:VGRD','color':{'red':0,'green':153,'blue':0},'filter_string':['UGRD:10 m above ground:','VGRD:10 m above ground:']}
    u_wcmpt={'varname':'wind_u_component','s3_name':'wind-vectors-uv','label':'wind_u_component','shrt_name':'u_cmpt','unit':'m.s-1','gfs_level':'10_m','gfs_var':'uvgrd','gfs_fmt':'UGRD:VGRD','color':{'red':0,'green':153,'blue':0},'filter_string':'UGRD:10 m above ground:'}
    v_wcmpt={'varname':'wind_v_component','s3_name':'wind-vectors-uv','label':'wind_v_component','shrt_name':'v_cmpt','unit':'m.s-1','gfs_level':'10_m','gfs_var':'uvgrd','gfs_fmt':'UGRD:VGRD','color':{'red':0,'green':153,'blue':0},'filter_string':'VGRD:10 m above ground:'}
    cape={'varname':'cape','s3_name':'lightning','label':'Lightning','shrt_name':'cape','unit':'cape_index_value','gfs_level':'surface','gfs_var':'cape','gfs_fmt':'CAPE','color':{'red':153,'green':0,'blue':0},'filter_string':':CAPE:surface:'}
    apcp={'varname':'precipitation','s3_name':'precipitation','label':'Precipitation','shrt_name':'apcp','unit':'mm.hr-1','gfs_level':'surface','gfs_var':'apcp','gfs_fmt':'APCP','color':{'red':0,'green':153,'blue':0},'filter_string':':APCP:surface:'}
    variable={}
    variable['temp']=[temp]
    variable['prate']=[prate]
    variable['wspd']=[wspd]
    variable['uvgrd']=[wdir,u_wcmpt,v_wcmpt]
    variable['cape']=[cape]
    variable['apcp']=[apcp]
    return variable



def grib_download_process(params,bucket):
    datefmt=params.startdate
    run=params.run
    var_name=params.short_name
    folderpath='/tmp/gfs_data/'
    date_folder=folderpath+datefmt+run+'/'
    foldercreator(date_folder)
    variable_json=variable_json_creator()
    filter_json_list=variable_json[var_name]
    for filtr_json in filter_json_list:
        if filtr_json['shrt_name']=='temp':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
            #s3_temp_upload(params,mh_region_list)
        elif filtr_json['shrt_name']=='prate':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
            #s3_prate_upload(params,mh_region_list)
        elif filtr_json['shrt_name']=='wspd':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
            #s3_wspd_upload(params,mh_region_list)
        elif filtr_json['shrt_name']=='wdir':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
            #s3_wdir_upload(params,mh_region_list)
        elif filtr_json['shrt_name']=='u_cmpt':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
            #s3_uv_wind_upload(params,mh_region_list)
        elif filtr_json['shrt_name']=='v_cmpt':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            gfs_grib_loc = Path(gfs_grib)
            print(gribfilename)
            print(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
        elif filtr_json['shrt_name']=='cape':
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
        else:
            var_folder=date_folder+filtr_json['gfs_var']+'/'
            foldercreator(var_folder)
            params.localfolder=var_folder
            params.searchString=filtr_json['filter_string']
            gribfilename=s3_gfs_download(params)
            gfs_grib=f'{params.localfolder}{gribfilename}'
            print(gribfilename)
            print(gfs_grib)
            gfs_grib_loc = Path(gfs_grib)
            if gfs_grib_loc.exists():
                gcs_uploader(params,gfs_grib,bucket)
