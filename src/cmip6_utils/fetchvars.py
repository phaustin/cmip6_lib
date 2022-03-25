def save_model(mod_id, exp_id, lev_direction):
    if mod_id == "BCC-ESM1":
        lp_exp_id = "1pctCO2"
    else:
        lp_exp_id = "piControl"

    model_path = Path('models/'+mod_id+'_'+exp_id+'.zarr')

    #get the lats and lons for our data:
    query = "variable_id=='"+lp_var_id+"' & experiment_id=='"+lp_exp_id+"' & source_id=='"+mod_id+"' & table_id=='"+lp_monthly_table+"'"
    lp_df = df_og.query(query)
    zstore_url = lp_df["zstore"].values[0]
    the_mapper=fs.get_mapper(zstore_url)
    lp_ds = xr.open_zarr(the_mapper, consolidated=True)
    
    #Cloud Data
    query = "variable_id=='"+var_id+"' & experiment_id=='"+exp_id+"' & source_id=='"+mod_id+"' & table_id=='"+monthly_table+"'"
    cloud_df = df_og.query(query)
    zstore_url = cloud_df["zstore"].values[0]
    the_mapper=fs.get_mapper(zstore_url)
    ds = xr.open_zarr(the_mapper, consolidated=True)
    #print(ds.sizes)
    lp_ds = lp_ds.reindex_like(ds, method="nearest")
    ds_water = ds.where(lp_ds.sftlf == 0.0) #only values over water
    #ds_subset = ds_water.where(ds_water.lev*lev_conversion > 0.7, drop=True)
    ds_sorted = ds_water.sortby(ds_water.lev, ascending=False) #sort lev in descending order
    if lev_direction == "up":
        ds_subset = ds_sorted.isel(lev=slice(-11, -1)) #select 11 smallest levels
    elif lev_direction == "down":
        ds_subset = ds_sorted.isel(lev=slice(0, 11)) #select 11 largest levels
    
    ds_subset = ds_subset.sel(lat=slice(21, 47),  #15-40 degrees North latitude 
                                    lon=slice(200, 243)) #and about 125 to 135 degrees west longitude
    if len(ds_subset.time) > 3000:
        ds_subset = ds_subset.isel(time=slice(0, 3000)) #250 years, so models starting in 1850 will range 1850-2100

    ds_subset.to_zarr(model_path, mode='w')


def fetch_var_approx(the_dict,df_og):
    the_keys = list(the_dict.keys())
    print(the_keys)
    key0 = the_keys[0]
    print(key0)
    print(the_dict[key0])
    hit0 = df_og[key0].str.find(the_dict[key0]) > -1
    if len(the_keys) > 1:
        hitnew = hit0
        for key in the_keys[1:]:
            hit = df_og[key].str.find(the_dict[key]) > -1
            hitnew = np.logical_and(hitnew,hit)
            print(np.sum(hitnew))
    else:
        hitnew = hit0
    df_result = df_og[hitnew]
    return df_result


def fetch_var_exact(the_dict,df_og):
    the_keys = list(the_dict.keys())
    print(the_keys)
    key0 = the_keys[0]
    print(key0)
    print(the_dict[key0])
    hit0 = df_og[key0] == the_dict[key0]
    if len(the_keys) > 1:
        hitnew = hit0
        for key in the_keys[1:]:
            hit = df_og[key] == the_dict[key]
            hitnew = np.logical_and(hitnew,hit)
            print("total hits: ",np.sum(hitnew))
    else:
        hitnew = hit0
    df_result = df_og[hitnew]
    return df_result

