import re
from pypdf import PdfReader
from numpy import nan
import pandas as pd
import gspread as gs
import gspread_dataframe as gd

# DECLARE FUNCTIONS -----------------------

def select_line(text: str, line_number: int):
    '''
    Helper function to select line in a re search function. 
    '''
    if line_number == 1:
        start_index = 0
        end_index = re.search(".*$", text, flags=re.M).end()
    else:
        regex_start = ".*\\n" * (line_number - 1)
        regex_end = regex_start + ".*$"

        start_index = re.search(regex_start, text, flags=re.M).end()
        end_index = re.search(regex_end, text, flags=re.M).end()

    return text[start_index:end_index]

def get_endpos(page):
    '''
    Gets end positions of each lockup number block returns dictionary.

    Used to set bounds of each block
    '''
    lunum = re.finditer(r"^\s+(?P<number>\d\d)(?= )", page, flags=re.M)

    #gets ending position of each LU block and puts it in a dictionary
    endpos = {}
    for lu in lunum:   
        endpos[int(lu.group('number'))-1] = lu.start()

    lunum = re.finditer(r"^\s+(?P<number>\d\d)(?= )", page, flags=re.M)

    #add last endpos as end of page
    *_, last = lunum 
    endpos[int(last.group('number'))] = len(page)

    return endpos

def handle_nulls(scrape_var, strip=False):
    '''
    Handles cases where regex search funds nothing and returns nan.
    '''
    if scrape_var is not None:
        if not strip:
            handled_var = scrape_var.group()
        else:
            handled_var = scrape_var.group().strip()
    else:
        handled_var = nan
    
    return handled_var

def normalize_layout(text):
    '''
    Some lockup lists contain weird spacing when run through the extract layout. 
    This function handles doubles spaces between characters. 
    Minimizing other spacing changes to maintain the usual layout quirks that aid normal regex searches.  
    '''
    formatted = re.sub(r'(?<=\w|\d|[,.]) {2}(?=\w|\d)', ' ', text)
    formatted = re.sub(r'(?<=year)(\s+)(?=old)', ' ', formatted)
    formatted = re.sub(r'(?<=Black)(\s+)(?=or)', ' ', formatted)
    formatted = re.sub(r'(?<=Hispanic)(\s+)(?=or)', ' ', formatted)
    formatted = re.sub(r'(?<=Assigned)(\s+)(?=to)', ' ', formatted)
    formatted = re.sub(r'(?<=,)(\s+)(?=\w)', ' ', formatted)

    return formatted

def scrape_page(page, quiet = True):
    '''
    Pulls all information from each lockup block on a lockup sheet page 
    
    Returns a DataFrame. 
    '''
    #reset iter
    lunum = re.finditer(r"^\s+(?P<number>\d\d)(?= )", page, flags=re.M)

    d = []

    endpos = get_endpos(page = page)

    #loop through all lock up numbers
    for lu in lunum:
        scraper_warnings = nan
        
        num = int(lu.group('number'))

        try:
            block = page[lu.start():endpos[num]] 
        except KeyError:
            print(f"WARNING: Key Error affecting {num+1} LU Block will be skipped")
            d.append(
                {
                    'lockup_number': num+1,
                    'scraper_warnings': f"KeyError, PDFReader could not find {num+1};"
                }
            )
            
            block = page[lu.start():endpos[num+1]]

        court_date = re.search("\d{2}\/\d{2}\/\d{4}", select_line(block, 3)).group()

        arrest_number = re.search("(?<=     )\d{9}(?=     )", select_line(block, 2)).group()

        age = handle_nulls(re.search("\d\d(?= year old)", select_line(block, 1)))

        gender = handle_nulls(re.search("Male|Female(?= )", select_line(block, 2)))

        race = handle_nulls(re.search("(?<=     )White|Black or African-American|Hispanic or Latino(?=[ -])", select_line(block, 2)))

        #names search based on a name regex pattern; falls back searching for everything on between the adjecent columns
        true_name = re.search(r"((?<=     )[A-Za-z.'\- ]+, [A-Za-z.'\-]+(?=     ))|([A-Za-z.'\- ]+, [A-Za-z.'\-]+[ ]?[A-Za-z.'\-]+(?=     ))", select_line(block, 1))
        if true_name is not None:
            true_name = true_name.group().strip()
        else:
            print("True name fell back to column search")
            true_name = re.search(r"(?<=\d{2}\/\d{2}\/\d{4} \d{4})[A-Za-z.'\- ,]+(?=\d\d year old)", select_line(block, 1)).group().strip()

        name = re.search(r"((?<=     )[A-Za-z.'\- ]+, [A-Za-z.'\-]+(?=     ))|([A-Za-z.'\- ]+, [A-Za-z.'\-]+[ ]?[A-Za-z.'\-]+(?=     ))", select_line(block, 2))

        if name is not None:
            name = name.group().strip()
        else:
            print("Name fell back to column search")
            name = re.search(r"(?<=\d{9})[A-Za-z.'\- ,]+(?=White|Black or African-American|Hispanic or Latino)", select_line(block, 2)).group().strip()


        assigned_defense = re.search("(?<=Assigned To: ).+\)", block)

        if assigned_defense is not None: #handles cases where there is no assigned defense
            assigned_name = re.search(".+(?= \()", assigned_defense.group()).group()
            assigned_affiliation = re.search("(?<=\().+(?=\))", assigned_defense.group()).group()
        else:
            assigned_name = nan
            assigned_affiliation = nan

        arresting_officer = re.search(r"(?P<name>[A-Za-z.'\- ]+, [A-Za-z.'\-]+|(?<=[0-9])[A-Za-z.'\- ]+)(?P<badge>[ 0-9]*)", select_line(block, 3), flags=re.M)

        arresting_officer_name = arresting_officer.group("name").strip()

        if arresting_officer.group("badge") is not None:
            arresting_officer_badge = arresting_officer.group("badge").strip()
        else:
            arresting_officer_badge = nan

        arrest_date = handle_nulls(re.search("\d{2}\/\d{2}\/\d{4} \d{4}", select_line(block, 1)))

        charges = handle_nulls(re.search("(?<=Release\n)(?s:.)*(?=Assigned To)", block, flags=re.M), strip=True)

        prosecutor = handle_nulls(re.search("(?<=^)[(USAO)(OAG)(Traffic) &]+(?=     )", select_line(block, 3), flags=re.M), strip=True)

        pdid = handle_nulls(re.search("[0-9]{6}(?=     |$)", select_line(block, 1), flags=re.M))

        ccn = handle_nulls(re.search("[0-9]{8}(?=     |$)", select_line(block, 2), flags=re.M))

        codef = handle_nulls(re.search(r"(?<=CODEF )/d{2}|(?<=CODEF)/d{2}", block))

        #searches the whole block for multiple flags that can exist anywhere in the block
        dv = re.search("(?<=     )DV(?=     |$)", block, flags=re.M)

        if dv is not None:
            dv_flag = 1
        else:
            dv_flag = 0

        si = re.search("(?<=     )SI(?=     |$)", block, flags=re.M)

        if si is not None:
            si_flag = 1
        else:
            si_flag = 0

        p = re.search("(?<=     )P(?=     |$)", block, flags=re.M)

        if p is not None:
            p_flag = 1
        else:
            p_flag = 0

        np = re.search("(?<=     )NP(?=     |$)", block, flags=re.M)

        if np is not None:
            np_flag = 1
        else:
            np_flag = 0

        if not quiet:
            print(f"""
                Pulling LU# {num}...
                age: {age}
                gender: {gender}
                race: {race}
                true name: {true_name}
                name: {name}
                attorney: {assigned_name} from {assigned_affiliation}
                arresting_officer: {arresting_officer_name} {arresting_officer_badge}
                arrest date time: {arrest_date}
                charges: {charges}
                prosecutor: {prosecutor}
                ------------------------------------------
                """)

        d.append(
            {
                'court_date': court_date,
                'lockup_number': num,
                'arrest_number': arrest_number,
                'prosecutor': prosecutor,
                'true_name': true_name,
                'name': name,
                'race': race,
                'gender': age,
                'age': age,
                'defense_name': assigned_name,
                'defense_affiliation': assigned_affiliation,
                'arresting_officer_name': arresting_officer_name,
                'arresting_officer_badge': arresting_officer_badge,
                'arrest_date': arrest_date,
                'charges': charges,
                'pdid': pdid,
                'ccn': ccn,
                'codef': codef,
                'dv_flag': dv_flag,
                'si_flag': si_flag,
                'p_flag': p_flag,
                'np_flag': np_flag,
                'scraper_warnings': scraper_warnings
            }
        )

    df = pd.DataFrame(d)
    if not quiet:
        print(df.head(10))
    return df

def scrape_fulldoc(pdf, quiet = True):
    '''
    Takes PDF and scrapes each page with scrape_page() and applies additional formatting for standardization

    Returns a concatenated DataFrame of all pages
    '''
    read_pdf = PdfReader(pdf)

    df = pd.DataFrame()

    for page in read_pdf.pages:
        raw_page_text = page.extract_text(extraction_mode="layout")

        formatted_page = normalize_layout(raw_page_text)

        df = pd.concat([df, scrape_page(formatted_page, quiet)])

    return df



def append_to_sheet(creds, df, gid):
    ws = creds.open_by_key(gid).get_worksheet(0)
    gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=False,row=ws.row_count+1,resize=False)
    print(f"Appended to Sheet: {gid}")

#TODO create function that creates the base google sheet:       def create_new_sheet(creds, )
#TODO need to consider some error handling so that we can get partials blocks
#TODO combine handle_nulls and the re.search into one wrapped function for cleanliness
#TODO consider making the loop stuff into a class object