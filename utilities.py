import pandas as pd
import string

# Utility: Clean and normalize patient/facility identifiers
def clean_id(val):
    if pd.isna(val):
        return ''
    val = str(val).strip().lower().replace(' ', '').lstrip('0')
    return val

def process_emr_data(df, dfbaseline, emr_df):
    # Remove rows with any blank fields in mapping
    emr_df = emr_df[(emr_df != '').all(axis=1)]

    # Map LGA, STATE, and LAMIS facility name to current df
    df['LGA2'] = df['FacilityName'].map(emr_df.set_index('Name on NMRS')['LGA'])
    df['STATE2'] = df['FacilityName'].map(emr_df.set_index('Name on NMRS')['STATE'])
    df['Name on Lamis'] = df['FacilityName'].map(emr_df.set_index('Name on NMRS')['Name on Lamis'])

    # Fill missing LGA/State and correct facility names
    df.loc[df['LGA'].isna(), 'LGA'] = df['LGA2']
    df.loc[df['State'].isna(), 'State'] = df['STATE2']
    df.loc[df['Name on Lamis'] != df['FacilityName'], 'FacilityName'] = df['Name on Lamis']

    df.drop(['LGA2', 'STATE2', 'Name on Lamis'], axis=1, inplace=True)

    # Normalize hospital numbers and unique IDs
    df['PatientHospitalNo1'] = df['PatientHospitalNo'].apply(clean_id)
    df['PatientUniqueID1'] = df['PEPID'].apply(clean_id)
    dfbaseline['Hospital Number1'] = dfbaseline['Hospital Number'].apply(clean_id)
    dfbaseline['Unique ID1'] = dfbaseline['Unique ID'].apply(clean_id)

    # Create consistent unique identifiers for both datasets
    dfbaseline['unique identifiers'] = (
        dfbaseline["LGA"].astype(str).str.lower().str.strip().str.replace(' ', '') +
        dfbaseline["Facility"].astype(str).str.lower().str.strip().str.replace(' ', '') +
        dfbaseline["Hospital Number1"] +
        dfbaseline["Unique ID1"]
    )

    df['unique identifiers'] = (
        df["LGA"].astype(str).str.lower().str.strip().str.replace(' ', '') +
        df["FacilityName"].astype(str).str.lower().str.strip().str.replace(' ', '') +
        df["PatientHospitalNo1"] +
        df["PatientUniqueID1"]
    )

    # Drop duplicates from baseline data
    dfbaseline = dfbaseline.drop_duplicates(subset=['unique identifiers'], keep=False)

    # Handle duplicate IDs in current df by appending _A, _B, etc.
    dup_mask = df.duplicated(['unique identifiers'], keep=False)
    alphabet = dict(enumerate(string.ascii_uppercase))
    df.loc[dup_mask, 'unique identifiers'] += '_' + df[dup_mask].groupby(['unique identifiers']).cumcount().map(alphabet)

    # Map TPT values from baseline to current df
    df['Radet_Date of TPT Start (yyyy-mm-dd)'] = df['unique identifiers'].map(
        dfbaseline.set_index('unique identifiers')['Date of TPT Start (yyyy-mm-dd)']
    )
    df['Radet_TPT Type'] = df['unique identifiers'].map(
        dfbaseline.set_index('unique identifiers')['TPT Type']
    )

    # Fill missing TPT fields in current dataset
    df.loc[df['First_TPT_Pickupdate'].isna(), 'First_TPT_Pickupdate'] = df['Radet_Date of TPT Start (yyyy-mm-dd)']
    df.loc[df['Current_TPT_Received'].isna(), 'Current_TPT_Received'] = df['Radet_TPT Type']

    return df