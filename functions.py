import warnings
from bs4 import BeautifulSoup
import requests,json
from time import sleep
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
import os

warnings.filterwarnings("ignore")

def get_google_results(search_text, sleep_bool=True):
    # Creating request parameters
    url = 'https://google.com/search?q=' + search_text

    # sending the request,
    request_result = requests.get(url)

    # create BSoup object
    soup = BeautifulSoup(request_result.text,
                         "html.parser")

    # Find all titles in the HTML
    results = soup.find_all('h3')

    # Creating the object to be returned
    return_list = [x.getText() for x in results]

    # Sleep when needed
    if sleep_bool:
        sleep(40)
    return return_list

def retrieve_job(job_id):
    # Get enviroment variables
    ENDPOINT = os.environ["db_url"]
    PORT = os.environ["db_port"]
    USR = os.environ["db_usr"]
    DBNAME = os.environ["db_name"]
    PASSWORD = os.environ["db_pass"]

    try:
        # Connect to DB
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=PASSWORD)
        cur = conn.cursor()
        # Execute SELECT query
        cur.execute("""SELECT * FROM jobs
                    WHERE id = %s""",
                    (job_id,))
        resp = cur.fetchall()
        conn.commit()
        # Close DB connection
        cur.close()
        conn.close()
        # Return the columns
        return resp
    except Exception as e:
        # Print any error
        print("Database connection failed due to {}".format(e))
        try:
            conn.close()
        except:
            pass
        try:
            cur.close()
        except:
            pass


def update_job(job_id, status, result):
    # Get enviroment variables
    ENDPOINT = os.environ["db_url"]
    PORT = os.environ["db_port"]
    USR = os.environ["db_usr"]
    DBNAME = os.environ["db_name"]
    PASSWORD = os.environ["db_pass"]

    try:
        # Connect to DB
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=PASSWORD)
        cur = conn.cursor()
        # Execute update query
        cur.execute(
            f"""UPDATE
        jobs
        SET
        status = %s,
        result = %s
        WHERE
        id = %s""",
            (status, Json(result), job_id))
        conn.commit()
        # Close Connection
        cur.close()
        conn.close()
    except Exception as e:
        # Print any error
        print("Database connection failed due to {}".format(e))
        try:
            conn.close()
        except:
            pass
        try:
            cur.close()
        except:
            pass


def create_job():
    # Get enviroment variables
    ENDPOINT = os.environ["db_url"]
    PORT = os.environ["db_port"]
    USR = os.environ["db_usr"]
    DBNAME = os.environ["db_name"]
    PASSWORD = os.environ["db_pass"]

    try:
        # Connect to DB
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=PASSWORD)
        cur = conn.cursor()
        # Run insert query
        cur.execute(
            f"INSERT INTO jobs (status,result,created_at) VALUES (%s,%s,%s) RETURNING id;",
            ('Running', None, datetime.now()))
        id = cur.fetchone()[0]
        conn.commit()
        # Close connection
        cur.close()
        conn.close()
        # Return the job ID generated
        return id
    except Exception as e:
        # Print any errors
        print("Database connection failed due to {}".format(e))
        try:
            conn.close()
        except:
            pass
        try:
            cur.close()
        except:
            pass


