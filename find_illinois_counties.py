#!/usr/bin/env python3
"""
Find Illinois counties for testing performance fixes
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.database_manager_v3 import database_manager

def find_test_counties():
    """Find good Illinois counties for testing"""
    
    try:
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT countyfips, 
                       COUNT(*) as parcel_count,
                       MIN(ST_XMin(geometry)) as min_lon,
                       MAX(ST_XMax(geometry)) as max_lon, 
                       MIN(ST_YMin(geometry)) as min_lat,
                       MAX(ST_YMax(geometry)) as max_lat
                FROM parcels 
                WHERE statefips = '17' 
                GROUP BY countyfips 
                HAVING COUNT(*) BETWEEN 5000 AND 50000  -- Good size for testing
                ORDER BY parcel_count DESC
                LIMIT 10
            ''')
            
            print('🏘️  Illinois Counties (Good for testing):')
            print('County FIPS | Parcels   | Geographic Bounds')
            print('-' * 60)
            for row in cursor.fetchall():
                county_name = get_county_name(row[0])
                print(f'{row[0]} ({county_name:15}) | {row[1]:7,} | ({row[2]:.2f},{row[4]:.2f}) to ({row[3]:.2f},{row[5]:.2f})')
                
    except Exception as e:
        print(f"Error querying counties: {e}")

def get_county_name(fips_code):
    """Get county name from FIPS code"""
    illinois_counties = {
        '001': 'Adams',
        '003': 'Alexander', 
        '005': 'Bond',
        '007': 'Boone',
        '009': 'Brown',
        '011': 'Bureau',
        '013': 'Calhoun',
        '015': 'Carroll',
        '017': 'Cass',
        '019': 'Champaign',
        '021': 'Christian',
        '023': 'Clark',
        '025': 'Clay',
        '027': 'Clinton',
        '029': 'Coles',
        '031': 'Cook',
        '033': 'Crawford',
        '035': 'Cumberland',
        '037': 'DeKalb',
        '039': 'DeWitt',
        '041': 'Douglas',
        '043': 'DuPage',
        '045': 'Edgar',
        '047': 'Edwards',
        '049': 'Effingham',
        '051': 'Fayette',
        '053': 'Ford',
        '055': 'Franklin',
        '057': 'Fulton',
        '059': 'Gallatin',
        '061': 'Greene',
        '063': 'Grundy',
        '065': 'Hamilton',
        '067': 'Hancock',
        '069': 'Hardin',
        '071': 'Henderson',
        '073': 'Henry',
        '075': 'Iroquois',
        '077': 'Jackson',
        '079': 'Jasper',
        '081': 'Jefferson',
        '083': 'Jersey',
        '085': 'Jo Daviess',
        '087': 'Johnson',
        '089': 'Kane',
        '091': 'Kankakee',
        '093': 'Kendall',
        '095': 'Knox',
        '097': 'Lake',
        '099': 'LaSalle',
        '101': 'Lawrence',
        '103': 'Lee',
        '105': 'Livingston',
        '107': 'Logan',
        '109': 'McDonough',
        '111': 'McHenry',
        '113': 'McLean',
        '115': 'Macon',
        '117': 'Macoupin',
        '119': 'Madison',
        '121': 'Marion',
        '123': 'Marshall',
        '125': 'Mason',
        '127': 'Massac',
        '129': 'Menard',
        '131': 'Mercer',
        '133': 'Monroe',
        '135': 'Montgomery',
        '137': 'Morgan',
        '139': 'Moultrie',
        '141': 'Ogle',
        '143': 'Peoria',
        '145': 'Perry',
        '147': 'Piatt',
        '149': 'Pike',
        '151': 'Pope',
        '153': 'Pulaski',
        '155': 'Putnam',
        '157': 'Randolph',
        '159': 'Richland',
        '161': 'Rock Island',
        '163': 'St. Clair',
        '165': 'Saline',
        '167': 'Sangamon',
        '169': 'Schuyler',
        '171': 'Scott',
        '173': 'Shelby',
        '175': 'Stark',
        '177': 'Stephenson',
        '179': 'Tazewell',
        '181': 'Union',
        '183': 'Vermilion',
        '185': 'Wabash',
        '187': 'Warren',
        '189': 'Washington',
        '191': 'Wayne',
        '193': 'White',
        '195': 'Whiteside',
        '197': 'Will',
        '199': 'Williamson',
        '201': 'Winnebago',
        '203': 'Woodford'
    }
    return illinois_counties.get(fips_code, f'County-{fips_code}')

if __name__ == "__main__":
    find_test_counties()