import re
import os
from datetime import datetime
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
    Some lockup lists contain weird spacing when run through the extract layout function. 
    
    This function handles multiple spaces between characters and known repeat words to allow for normal search functions. 
    '''
    formatted = re.sub(r'(?<=\w|\d|[,.]) {2}(?=\w|\d)', ' ', text)
    formatted = re.sub(r'(?<=year)(\s+)(?=old)', ' ', formatted)
    formatted = re.sub(r'(?<=Black)(\s+)(?=or)', ' ', formatted)
    formatted = re.sub(r'(?<=or)(\s+)(?=African-American)', ' ', formatted)
    formatted = re.sub(r'(?<=or)(\s+)(?=Latino)', ' ', formatted)
    formatted = re.sub(r'(?<=Hispanic)(\s+)(?=or)', ' ', formatted)
    formatted = re.sub(r'(?<=Assigned)(\s+)(?=to)', ' ', formatted)
    formatted = re.sub(r'(?<=,)(\s+)(?=\w)', ' ', formatted)
    # ^ all the above fixes multiple spaces especially between words used to help find other info e.g. name searches
    formatted = re.sub(r'(?<=\n)\n(?=               )', '', formatted) #fixes blank lines
    formatted = re.sub(r'(?<=\n)\n(?=               )', '', formatted) #fixes additional blank lines from the fixing blank lines
    formatted = re.sub(r'—|•', '', formatted)
    #formatted = re.sub(r'(?<=     )[0-9]{6}     (?!\n)', r'[0-9]{6}\n', formatted)

    return formatted

class LockUpBlock():    
    def __init__(self, lu_number, block, errored_lu=False):
        self.lu_number = lu_number
        self.block = block

        #when errored_lu is true you can cal details individually
        if not errored_lu:
            self.get_lo_details()
            self.get_case_details()
            self.get_arrest_details()
        

    def get_lo_details(self):
        self.court_date = handle_nulls(re.search("\d{2}\/\d{2}\/\d{4}", select_line(self.block, 3)))
        self.age = handle_nulls(re.search("\d\d(?= year old)", select_line(self.block, 1)))

        self.gender = handle_nulls(re.search("Male|Female(?= )", select_line(self.block, 2)))

        self.race = handle_nulls(re.search("(?<=     )White|Black [ao]r African-American|Hispanic or Latino(?=[ -])", select_line(self.block, 2)))

        # names search based on a name regex pattern; regex patterns tries to match first without middle name then with middle name
        # allows for up to 6 spaces between the first and middle name
        # falls back searching for everything on between the adjecent columns
        self.true_name = re.search(r"((?<=     )[A-Za-z.’'\- ]+, [A-Za-z.’'\-]+(?=          ))|([A-Za-z.’'\- ]+, [A-Za-z.'\-]+[ ]{,6}[A-Za-z.'\-]+(?=     ))", select_line(self.block, 1))
        if self.true_name is not None:
            self.true_name = self.true_name.group().strip()
        else:
            self.true_name = re.search(r"(?<=\d{2}\/\d{2}\/\d{4} \d{4})[0-9A-Za-z.’'\- ,]+(?=\d\d year old)|(?<=\d{2}\/\d{2}\/\d{4}\d{4})[0-9A-Za-z.’'\- ,]+(?=\d\d year old)", select_line(self.block, 1)).group().strip()

        self.name = re.search(r"((?<=     )[A-Za-z.’'\- ]+, [A-Za-z.’'\-]+(?=     ))|([A-Za-z.’'\- ]+, [A-Za-z.’'\-]+[ ]{,6}[A-Za-z.'\-]+(?=          ))", select_line(self.block, 2))

        if self.name is not None:
            self.name = self.name.group().strip()
        else:
            print("Name fell back to column search")
            self.name = handle_nulls(re.search(r"(?<=\d{9})[A-Za-z.'\- ,]+(?=White|Black [ao]r African-American|Hispanic [ao]r Latino)", select_line(self.block, 2)), strip=True)

    def get_arrest_details(self):

        self.arrest_number = handle_nulls(re.search("(?<=     )\d{9}(?=     )", select_line(self.block, 2)))

        arresting_officer = re.search(r"(?P<name>[A-Za-z.'\- ]+, [A-Za-z.'\-]+|(?<=[0-9])[A-Za-z.'\- ]+)(?P<badge>[ 0-9]*)", select_line(self.block, 3), flags=re.M)
        
        if arresting_officer is not None: 
            if arresting_officer.group("name") is not None:
                self.arresting_officer_name = arresting_officer.group("name").strip()
            else:
                self.arresting_officer_name = nan

            if arresting_officer.group("badge") is not None:
                self.arresting_officer_badge = arresting_officer.group("badge").strip()
            else:
                self.arresting_officer_badge = nan
        else: 
            self.arresting_officer_name = nan
            self.arresting_officer_badge = nan

        self.arrest_date = handle_nulls(re.search("\d{2}\/\d{2}\/\d{4} \d{4}", select_line(self.block, 1)))

    def get_case_details(self):
        self.prosecutor = handle_nulls(re.search("(?<=^)[(USAO)(OAG)(Traffic) &]+(?=     )", select_line(self.block, 3), flags=re.M), strip=True)
        
        assigned_defense = re.search("(?<=Assigned To: ).+\)", self.block)

        if assigned_defense is not None: #handles cases where there is no assigned defense
            self.assigned_name = re.search(".+(?= \()", assigned_defense.group()).group()
            self.assigned_affiliation = re.search("(?<=\().+(?=\))", assigned_defense.group()).group()
        else:
            self.assigned_name = nan
            self.assigned_affiliation = nan

        self.charges = handle_nulls(re.search("(?<=Release\n)(?s:.)*(?=Assigned To)", self.block, flags=re.M), strip=True)

        self.pdid = handle_nulls(re.search("[0-9]{6}(?=     |$)", select_line(self.block, 1), flags=re.M))

        self.ccn = handle_nulls(re.search("[0-9]{8}(?=     |$)", select_line(self.block, 2), flags=re.M))

        self.codef = handle_nulls(re.search(r"(?<=CODEF )/d{2}|(?<=CODEF)/d{2}", self.block))

        #searches the whole block for multiple flags that can exist anywhere in the block
        dv = re.search("(?<=     )DV(?=     |$)", self.block, flags=re.M)

        if dv is not None:
            self.dv_flag = 1
        else:
            self.dv_flag = 0

        si = re.search("(?<=     )SI(?=     |$)", self.block, flags=re.M)

        if si is not None:
            self.si_flag = 1
        else:
            self.si_flag = 0

        p = re.search("(?<=     )P(?=     |$)", self.block, flags=re.M)

        if p is not None:
            self.p_flag = 1
        else:
            self.p_flag = 0

        np = re.search("(?<=     )NP(?=     |$)", self.block, flags=re.M)

        if np is not None:
            self.np_flag = 1
        else:
            self.np_flag = 0   

def scrape_page(page, quiet = True):
    '''
    Pulls all information from each lockup block on a lockup sheet page 
    
    Returns a DataFrame. 
    '''
    #reset iter
    lunum_list = [int(item.group("number")) for item in re.finditer(r"^\s+(?P<number>\d\d)(?= )", page, flags=re.M)]
    lunum = re.finditer(r"^\s+(?P<number>\d\d)(?= )", page, flags=re.M)

    d = []

    endpos = get_endpos(page = page)

    #loop through all lock up numbers
    for lu in lunum:
        scraper_warnings = nan
        
        num = int(lu.group('number'))

        try:
            block = page[lu.start():endpos[num]] 
        # sometimes just the LU number cant be read this creates an observation for the next LU and notes the error
        # then captures the data for the current LU using the endpos of the next block
        except KeyError: 
            print(f"WARNING: Key Error affecting {num+1} LU Block will be skipped")
           
            try:
                court_date_fallback = d[-1].get('court_date')
            except IndexError:
                court_date_fallback = nan

            d.append(
                {
                    'court_date': court_date_fallback,
                    'lockup_number': num+1,
                    'scraper_warnings': f"KeyError, PDFReader could not find {num+1};"
                }
            )

            block = page[lu.start():endpos[lunum_list[lunum_list.index(num)+1]]]

        # deal with leading newlines
        block = re.sub(r'\n+(?=\s{,20}(\d\d) )', '', block)

        # TODO remove
        if num == 33:
            print(block)

        CurrentLUNum = LockUpBlock(num, block)

        if not quiet:
            print(f"""
                Pulling LU# {CurrentLUNum.lu_number}...
                age: {CurrentLUNum.age}
                gender: {CurrentLUNum.gender}
                race: {CurrentLUNum.race}
                true name: {CurrentLUNum.true_name}
                name: {CurrentLUNum.name}
                attorney: {CurrentLUNum.assigned_name} from {CurrentLUNum.assigned_affiliation}
                arresting_officer: {CurrentLUNum.arresting_officer_name} {CurrentLUNum.arresting_officer_badge}
                arrest date time: {CurrentLUNum.arrest_date}
                charges: {CurrentLUNum.charges}
                prosecutor: {CurrentLUNum.prosecutor}
                ------------------------------------------
                """)

        d.append(
            {
                'court_date': CurrentLUNum.court_date,
                'lockup_number': num,
                'arrest_number': CurrentLUNum.arrest_number,
                'prosecutor': CurrentLUNum.prosecutor,
                'true_name': CurrentLUNum.true_name,
                'name': CurrentLUNum.name,
                'race': CurrentLUNum.race,
                'gender': CurrentLUNum.gender,
                'age': CurrentLUNum.age,
                'defense_name': CurrentLUNum.assigned_name,
                'defense_affiliation': CurrentLUNum.assigned_affiliation,
                'arresting_officer_name': CurrentLUNum.arresting_officer_name,
                'arresting_officer_badge': CurrentLUNum.arresting_officer_badge,
                'arrest_date': CurrentLUNum.arrest_date,
                'charges': CurrentLUNum.charges,
                'pdid': CurrentLUNum.pdid,
                'ccn': CurrentLUNum.ccn,
                'codef': CurrentLUNum.codef,
                'dv_flag': CurrentLUNum.dv_flag,
                'si_flag': CurrentLUNum.si_flag,
                'p_flag': CurrentLUNum.p_flag,
                'np_flag': CurrentLUNum.np_flag,
                'scraper_warnings': scraper_warnings
            }
        )

    df = pd.DataFrame(d)
    if not quiet:
        print(df.head(10))
    return df

def scrape_fulldoc(pdf, df=pd.DataFrame(), quiet = True, testing = False):
    '''
    Takes PDF and scrapes each page with scrape_page() and applies additional formatting for standardization

    Returns a concatenated DataFrame of all pages
    '''
    file_name = os.path.basename(pdf)
    read_pdf = PdfReader(pdf)

    for page in read_pdf.pages:
        raw_page_text = page.extract_text(extraction_mode="layout")

        formatted_page = normalize_layout(raw_page_text)
        
        if testing:
            with open("testing_output.txt", "w") as f:
                f.write(file_name + "\n\n" + formatted_page)

        df = pd.concat([df, scrape_page(formatted_page, quiet)])

    # clean df
    df['court_date'] = df['court_date'].ffill() #all lock up lists should be for one court date so we can fill na's with previous
    df['file_name'] = file_name
    df['scrape_date'] = datetime.today().strftime('%m/%d/%Y')

    return df

def append_to_sheet(creds, df, gid):
    ws = creds.open_by_key(gid).get_worksheet(0)
    gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=False,row=ws.row_count+1,resize=False)
    print(f"Appended to Sheet: {gid}")


#TODO ------->MUST BE DONE BEFORE PROD<-------- add functionality to back fill in all missing LU numbers! 
#TODO create function that creates the base google sheet:       def create_new_sheet(creds, )
#TODO combine handle_nulls and the re.search into one wrapped function for cleanliness
#TODO add comments to new stuff