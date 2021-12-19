from pandas.core.frame import DataFrame
from pymongo import MongoClient
from pymongo.cursor import Cursor
import pandas as pd
import os
import shutil
from time import sleep

all_entreprises = [
    "LE PISTON",
    "AGC",
    "ST MICROELECTRONICS",
    "GPC",
    "MASCIR",
    "MAGHREB STEEL",
    "YAZAKI",
    "OCP",
    "STELLANTIS",
    "OCP MS",
    "MECOMAR",
    "ALTEN",
    "O'DASSIA",
    "VALEO",
    "JESA",
    "DICASTAL",
    "TE CONNECTIVITY",
    "LEONI",
]


def make_folders(entreprises) -> None:
    created = 0
    existing = 0
    try:
        os.mkdir("entreprises")
    except:
        pass
    for entreprise in entreprises:
        try:
            os.mkdir("entreprises/{}".format(entreprise))
            print(entreprise, "folder created")
            created += 1
        except:
            print(entreprise, "already exists")
            existing += 1
    print("Created", created)
    print("Existing", existing)


def db_cursor(KEY) -> Cursor:
    db = MongoClient(KEY)["fame-cv"]
    users = db["users"]
    all_data = users.find()
    return all_data


def make_valid_df(KEY) -> DataFrame:
    # A resume submission is valid if the email is valid
    # And the resume url is not null
    # And the name is not null
    # And at leat one entreprise is selected
    cursor = db_cursor(KEY)
    df = pd.DataFrame(list(cursor))
    df_valid_cv = df.copy()
    print("all {}".format(df_valid_cv.shape[0]))
    df_valid_cv = df_valid_cv.loc[df_valid_cv["resume_url"].notnull()]
    df_valid_cv = df_valid_cv.loc[df_valid_cv["name"].notnull()]
    df_valid_cv["valid"] = df_valid_cv.entreprises.apply(lambda x: len(x))
    df_valid_cv_entreprise = df_valid_cv.loc[df_valid_cv.valid > 0]
    print("valid submissions :{} from {}".format(df_valid_cv.shape[0], df.shape[0]))
    return df_valid_cv_entreprise


def strip_name(name) -> str:
    name_lower_case = name.lower()
    ans = ""
    for i in name_lower_case:
        if i != " ":
            ans += i
        else:
            ans += "_"
    return ans


def copy_cv(name, url, entreprises) -> None:
    file = url[13:]
    # fame-cv curriculum vitae folder from S3

    if os.path.isfile("./fame-cv/{}".format(file)):
        original = r"./fame-cv/{}".format(file)
        for en in entreprises:
            if original[-1] == "f":
                target = r"./entreprises/{}/{}_cv.pdf".format(en, strip_name(name))
            else:
                target = r"./entreprises/{}/{}_cv.docx".format(en, strip_name(name))
            print(name, " added ", en)
            shutil.copyfile(original, target)
    else:
        print(name, "can't be added")
        sleep(2)


def main():
    make_folders(all_entreprises)
    df = make_valid_df("""KEY""")
    i = 0
    names = []
    for index, row in df.iterrows():
        i += 1
        copy_cv(row["name"], row["resume_url"], row["entreprises"])
        names.append(row["name"])
    print("users added : {}".format(i))


main()
