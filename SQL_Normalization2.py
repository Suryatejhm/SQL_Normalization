import sqlite3
import numpy as np
import pandas as pd
from faker import Faker
import math


def create_connection(db_file, delete_db=False):
    import os

    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


conn = create_connection("non_normalized.db")
sql_statement = "select * from Students;"
df = pd.read_sql_query(sql_statement, conn)


def create_df_degrees(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_degrees' dataframe that contains only
    the degrees. See screenshot below.
    """

    # BEGIN SOLUTION
    conn = create_connection(non_normalized_db_filename)
    df = pd.read_sql_query("select * from Students;", conn)
    df_degrees_list = df["Degree"].unique()
    df_degrees = pd.DataFrame(df_degrees_list, columns=["Degree"])
    return df_degrees
    # END SOLUTION


def create_df_exams(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_exams' dataframe that contains only
    the exams. See screenshot below. Sort by exam!
    hints:
    # https://stackoverflow.com/a/16476974
    # https://stackoverflow.com/a/36108422
    """

    # BEGIN SOLUTION
    conn = create_connection(non_normalized_db_filename)
    sql_statement = "select Exams from Students;"
    df_exams = pd.read_sql_query(sql_statement, conn)

    # df_degrees = df_degrees.reset_index()
    df_exams["Exams"] = df_exams["Exams"].str.split(",")
    df_exams = df_exams.explode("Exams").reset_index(drop=True)
    df_exams[["Exam", "Year"]] = df_exams.Exams.str.split(expand=True)
    df_exams["Year"] = df_exams["Year"].apply(
        lambda x: x.replace("(", "").replace(")", "")
    )
    df_exams.drop(columns=["Exams"], axis=1, inplace=True)
    df_new = df_exams[["Exam", "Year"]]
    df_exams2 = df_exams.drop_duplicates().reset_index(drop=True)
    df_exams = df_exams2
    df_exams = df_exams.sort_values(by=["Exam"]).reset_index(drop=True)
    df_exams = df_exams.astype({"Year": "int64"})

    return df_exams
    # END SOLUTION


# create_df_exams('non_normalized.db')
df_exams = create_df_exams("non_normalized.db")


def create_df_students(non_normalized_db_filename):
    """
    Open connection to the non-normalized database and generate a 'df_students' dataframe that contains the student
    first name, last name, and degree. You will need to add another StudentID column to do pandas merge.
    See screenshot below.
    You can use the original StudentID from the table.
    hint: use .split on the column name!
    """
    conn = create_connection("non_normalized.db")
    sql_statement = "select StudentID,Name,Degree from Students;"
    df_students = pd.read_sql_query(sql_statement, conn)
    df_students[["Last_Name", "First_Name"]] = df_students.Name.str.split(
        n=1, expand=True
    )
    df_students["Last_Name"] = df_students["Last_Name"].apply(
        lambda x: x.replace(",", "")
    )

    # df_students= df_students.explode("Name").reset_index(drop=True)
    # df_students[['Last_Name','First_Name']]=df_students.Name.str.split(n=1,expand=True)
    df_students.drop(columns=["Name"], axis=1, inplace=True)
    df_students_2 = df_students[["StudentID", "First_Name", "Last_Name", "Degree"]]
    df_students = df_students_2
    return df_students
    # BEGIN SOLUTION
    # END SOLUTION


def create_df_studentexamscores(non_normalized_db_filename, df_students):
    """
    Open connection to the non-normalized database and generate a 'df_studentexamscores' dataframe that
    contains StudentID, exam and score
    See screenshot below.
    """

    # BEGIN SOLUTION
    conn = create_connection("non_normalized.db")
    sql_statement = "select StudentID,Exams,Scores from Students;"
    df_studentexamscores = pd.read_sql_query(sql_statement, conn)
    df = df_studentexamscores

    df["Exams"] = df["Exams"].str.split(", ")
    df["Scores"] = df["Scores"].str.split(", ")
    # ensuring equal number of color and origin in each cell
    df["Scores"] = df.apply(
        lambda x: x["Scores"] * len(x["Exams"])
        if len(x["Exams"]) > len(x["Scores"])
        else x["Scores"],
        axis=1,
    )
    df = df.explode(["Exams", "Scores"]).reset_index(drop=True)
    df[["Exam", "Year"]] = df.Exams.str.split(expand=True)
    df.drop(columns=["Exams", "Year"], axis=1, inplace=True)
    df.rename(columns={"Scores": "Score"}, inplace=True)
    df_new = df[["StudentID", "Exam", "Score"]]
    df_new = df_new.astype({"Score": "int64"})
    return df_new
    # END SOLUTION


def ex1(df_exams):
    # df_exams = pd.read_csv("df_exams.csv")

    df_exams = df_exams.sort_values("Year")
    df_exams.reset_index(drop=True, inplace=True)
    return df_exams


# ex1(df_exams)
def ex2(df_students):
    """
    return a df frame with the degree count
    # NOTE -- rename name the degree column to Count!!!
    """
    # BEGIN SOLUTION
    df = df_students.groupby("Degree").size().reset_index(name="Count")

    df.index = df["Degree"]
    df = df[["Count"]].sort_index(ascending=False)
    df.columns = [["Count"]]

    return df


# ex2(df_students)
def ex3(df_studentexamscores, df_exams):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the exams. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    df = pd.merge(df_studentexamscores, df_exams, on="Exam", how="left")
    df = df.groupby("Exam").mean().sort_values("Score", ascending=False)
    df["average"] = df["Score"].round(2)
    df = df[["Year", "average"]]
    df["Year"] = df["Year"].astype(int)
    # END SOLUTION
    return df


def ex4(df_studentexamscores, df_students):
    """
    return a datafram that merges df_studentexamscores and df_exams and finds the average of the degrees. Sort
    the average in descending order. See screenshot below of the output. You have to fix up the column/index names.
    Hints:
    # https://stackoverflow.com/a/45451905
    # https://stackoverflow.com/a/11346337
    # round to two decimal places
    """

    # BEGIN SOLUTION
    df = pd.merge(df_students, df_studentexamscores, how="inner", on="StudentID")
    df_ans = (
        pd.DataFrame(df.groupby(["Degree"])["Score"].mean())
        .round({"Score": 2})
        .rename(columns={"Score": "Average"})
    )

    # meanOfScore = df_studentexamscores.groupby("StudentID").mean(numeric_only=True)
    # meanOfScore = meanOfScore.reset_index()
    # df = pd.merge(df_students, meanOfScore)
    # undergraduate = pd.concat([df["Degree"], df["Score"]], axis=1)
    # SortedAverage = (
    #     undergraduate.groupby("Degree")
    #     .mean(numeric_only=True)
    #     .sort_values("Score", ascending=False)
    # )
    # df = SortedAverage.rename(columns={"Score": "Average"})
    return df_ans


def ex5(df_studentexamscores, df_students):
    """
    merge df_studentexamscores and df_students to produce the output below. The output shows the average of the top
    10 students in descending order.
    Hint: https://stackoverflow.com/a/20491748
    round to two decimal places

    """

    # BEGIN SOLUTION
    meanOfScore = df_studentexamscores.groupby("StudentID").mean(numeric_only=True)
    meanOfScore = meanOfScore.rename(columns={"Score": "average"})
    meanOfScore = meanOfScore.reset_index()
    df = pd.merge(df_students, meanOfScore).sort_values("average", ascending=False)
    df = df.head(10)
    df = df[["First_Name", "Last_Name", "Degree", "average"]]
    df["average"] = df["average"].round(2)
    return df
    # END SOLUTION


# DO NOT MODIFY THIS CELL OR THE SEED

# THIS CELL IMPORTS ALL THE LIBRARIES YOU NEED!!!


np.random.seed(0)
fake = Faker()
Faker.seed(0)


def part2_step1():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    fake = Faker()
    Faker.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    fake_names = [fake.name() for _ in range(100)]
    # fake_names = [
    #     fake_name.replace("Mr.", "").replace("Miss", "").replace("PhD", "")
    #     for fake_name in fake_names
    # ]
    fake_names = [name.strip().split(" ") for name in fake_names]
    fake_names = [[name[0], " ".join(name[1:])] for name in fake_names]
    fake_ids = [np.random.randint(1000, 9999) for _ in range(100)]
    fake_ids = [f"{name[0][0:2].lower()}{id}" for name, id in zip(fake_names, fake_ids)]

    fake_names = pd.DataFrame(fake_names)
    fake_names = fake_names[[0, 1]]

    fake_names.columns = ["first_name", "last_name"]
    fake_names["username"] = fake_ids
    # fake_names["username"].index
    # fake_names.index = fake_ids
    fake_names = fake_names[["username", "first_name", "last_name"]]

    return fake_names
    # END SOLUTION


def part2_step2():

    # ---- DO NOT CHANGE
    np.random.seed(0)
    # ---- DO NOT CHANGE

    # BEGIN SOLUTION
    mu = [35, 75, 25, 45, 45, 75, 25, 45, 35]
    scale = [9, 15, 7, 10, 5, 20, 8, 9, 10]
    max_score = [50, 100, 40, 60, 50, 100, 50, 60, 50]
    data_req = [100, 9]
    np_list = np.random.normal(mu, scale, size=data_req)
    np_list = np.clip(np_list, a_min=[0, 0, 0, 0, 0, 0, 0, 0, 0], a_max=max_score)
    name_cols = ["Hw1", "Hw2", "Hw3", "Hw4", "Hw5", "Exam1", "Exam2", "Exam3", "Exam4"]

    df = pd.DataFrame(np_list, columns=name_cols)
    df = df.round(0)

    return df
    # END SOLUTION


def part2_step3(df2_scores):
    # BEGIN SOLUTION
    df2 = df2_scores.describe().loc[["mean", "std"]].T
    df2["mean_theoretical"] = [35, 75, 25, 45, 45, 75, 25, 45, 35]
    df2["std_theoretical"] = [9, 15, 7, 10, 5, 20, 8, 9, 10]
    df2["abs_mean_diff"] = abs(df2["mean"] - df2["mean_theoretical"])
    df2["abs_std_diff"] = abs(df2["std"] - df2["std_theoretical"])

    df2 = df2.round(2)

    return df2
    # END SOLUTION


def part2_step4(
    df2_students,
    df2_scores,
):
    # BEGIN SOLUTION

    max_score = [50, 100, 40, 60, 50, 100, 50, 60, 50]
    df2_scores = (df2_scores / max_score) * 100
    df2_scores = df2_scores.round(0)

    df4 = pd.concat([df2_students, df2_scores], axis=1)

    return df4
    # END SOLUTION


def part2_step5():
    # BEGIN SOLUTION
    df_ai = pd.read_csv("part2_step5-input.csv")
    df_ai = df_ai[df_ai.isin(["AI_ISSUE"]).any(axis=1)].reset_index()

    df_ai["AI_Count"] = (df_ai == "AI_ISSUE").sum(axis=1)

    df_ai = df_ai[["username", "first_name", "last_name", "AI_Count"]]

    return df_ai
    # END SOLUTION


def part2_step6():
    # BEGIN SOLUTION
    df_ai = pd.read_csv("part2_step5-input.csv")

    df_ai = df_ai.replace("AI_ISSUE", 0)
    df_ai_hw = df_ai[["Hw1", "Hw2", "Hw3", "Hw4", "Hw5"]].astype(float)
    df_ai_exam = df_ai[["Exam1", "Exam2", "Exam3", "Exam4"]].astype(float)

    df_ai_hw = df_ai_hw.apply(lambda row: row.fillna(row.mean()), axis=1).round(0)
    df_ai_exam = df_ai_exam.apply(lambda row: row.fillna(row.mean()), axis=1).round(0)

    df_final = pd.concat([df_ai_hw, df_ai_exam], axis=1)
    df_final = pd.concat(
        [df_ai[["username", "first_name", "last_name"]], df_final], axis=1
    )

    df_final["Grade"] = (
        0.05
        * (
            df_final["Hw1"]
            + df_final["Hw2"]
            + df_final["Hw3"]
            + df_final["Hw4"]
            + df_final["Hw5"]
        )
        + 0.2 * (df_final["Exam1"] + df_final["Exam2"] + df_final["Exam3"])
        + 0.15 * (df_final["Exam4"])
    ).round(0)

    df_final["LetterGrade"] = df_final["Grade"].apply(
        lambda x: "A"
        if x >= 80
        else ("B" if x >= 70 else ("C" if x >= 50 else ("D" if x >= 40 else "F")))
    )

    mean = df_final.describe().loc[["mean", "std"]].round(0)

    df_final = pd.concat([df_final, mean], axis=0)

    return df_final
    # END SOLUTION
