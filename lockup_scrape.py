import re
from pypdf import PdfReader
import pandas as pd

# DECLARE FUNCTIONS -----------------------

#reader = PdfReader("LockupList_TEST.pdf")
#page = reader.pages[6]
#layout = page.extract_text(extraction_mode="layout")

#with open("Output.txt", "w") as text_file:
#    text_file.write(layout)

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

def handle_nulls(scrape_var):
    '''
    Handles cases where regex search funds nothing and returns "N/A".
    '''
    if scrape_var is not None:
        return scrape_var.group()
    else:
        return "N/A"


def scrape_page(page):
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
        num = int(lu.group('number'))
        print(f"Pulling LU# {num}")

        block = page[lu.start():endpos[num]] 

        court_date = re.search("\d{2}\/\d{2}\/\d{4}", select_line(block, 3)).group()

        arrest_number = re.search("(?<=     )\d{9}(?=     )", select_line(block, 2)).group()

        age = re.search("\d\d(?= year old)", select_line(block, 1)).group()
        print(f'age: {age}')

        gender = handle_nulls(re.search("Male|Female(?= )", select_line(block, 2)))
        print(f'gender: {gender}')

        race = handle_nulls(re.search("(?<=     )White|Black or African-American|Hispanic or Latino(?=[ -])", select_line(block, 2)))
        print(f"race: {race}")

        #names search based on a name regex pattern; falls back searching for everything on between the adjecent columns
        true_name = re.search(r"((?<=     )[A-Za-z.'\- ]+, [A-Za-z.'\-]+(?=     ))|([A-Za-z.'\- ]+, [A-Za-z.'\-]+[ ]?[A-Za-z.'\-]+(?=     ))", select_line(block, 1))

        if true_name is not None:
            true_name = true_name.group().strip()
        else:
            true_name = re.search(r"(?<=\d{2}\/\d{2}\/\d{4} \d{4})[A-Za-z.'\- ,]+(?=\d\d year old))", select_line(block, 1)).group().strip()

        print(f'true name: {true_name}')

        name = re.search(r"((?<=     )[A-Za-z.'\- ]+, [A-Za-z.'\-]+(?=     ))|([A-Za-z.'\- ]+, [A-Za-z.'\-]+[ ]?[A-Za-z.'\-]+(?=     ))", select_line(block, 2))

        if name is not None:
            name = name.group().strip()
        else:
            name = re.search(r"(?<=\d{9})[A-Za-z.'\- ,]+(?=White|Black or African-American|Hispanic or Latino)", select_line(block, 2)).group().strip()

        print(f'name: {name}')

        assigned_defense = re.search("(?<=Assigned To: ).+\)", block)

        if assigned_defense is not None: #handles cases where there is no assigned defense
            assigned_name = re.search(".+(?= \()", assigned_defense.group()).group()
            assigned_affiliation = re.search("(?<=\().+(?=\))", assigned_defense.group()).group()
        else:
            assigned_name = "N/A"
            assigned_affiliation = "N/A"

        print(f'attorney: {assigned_name} from {assigned_affiliation}')

        arresting_officer = re.search(r"(?P<name>[A-Za-z.'\- ]+, [A-Za-z.'\-]+|(?<=[0-9])[A-Za-z.'\- ]+)(?P<badge>[ 0-9]*)", select_line(block, 3), flags=re.M)

        arresting_officer_name = arresting_officer.group("name").strip()

        if arresting_officer.group("badge") is not None:
            arresting_officer_badge = arresting_officer.group("badge").strip()
        else:
            arresting_officer_badge = "N/A"

        print(f'arresting_officer: {arresting_officer_name} {arresting_officer_badge}')

        arrest_date = handle_nulls(re.search("\d{2}\/\d{2}\/\d{4} \d{4}", select_line(block, 1)))
        print(f'arrest date time: {arrest_date}')

        charges = re.search("(?<=Release\n)(?s:.)*(?=Assigned To)", block, flags=re.M).group().strip()
        print(f'charges: {charges}')

        prosecutor = re.search("(?<=^)[(USAO)(OAG)(Traffic) &]+(?=     )", select_line(block, 3), flags=re.M).group().strip()
        print(f'prosecutor: {prosecutor}')

        pdid = re.search("[0-9]{6}(?=     |$)", select_line(block, 1), flags=re.M).group()

        ccn = re.search("[0-9]{8}(?=     |$)", select_line(block, 2), flags=re.M).group()

        codef = handle_nulls(re.search(r"(?<=CODEF )/d{2}", block))

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

        print("------------------------------------------")

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
                'np_flag': np_flag
            }
        )

    df = pd.DataFrame(d)
    print(df.head(10))
    return df

def scrape_fulldoc(pdf):
    '''
    Takes PDF and scrapes each page with scrape_page() 

    Returns a concatenated DataFrame of all pages
    '''
    read_pdf = PdfReader(pdf)

    df = pd.DataFrame()

    for page in read_pdf.pages:
        page_text = page.extract_text(extraction_mode="layout")

        df = pd.concat([df, scrape_page(page_text)])

    return df

# MAIN -----------------------

lockup_df = scrape_fulldoc("LockupList_TEST.pdf")
