import psycopg2

DB_NAME = "news" #database name

def query1():
    """Fetch the most popular three articles of all time"""

    db = psycopg2.connect(dbname=DB_NAME)
    c = db.cursor()
    c.execute(
    """select title, count(*) as views from articles a, log l
    where l.status='200 OK' and l.path like concat('%',a.slug)
    group by a.title order by views desc limit 3
    """)
    results = c.fetchall()
    db.close()
    return results

def print_result_query1():
    """Print the most popular three articles of all time"""

    print(" 1. What are the most popular three articles of all time? ")
    for title, views in query1():
        print(" \"{}\" -- {} views".format(title, views))

def query2():
    """Fetch the most popular article authors of all time"""

    db = psycopg2.connect(dbname=DB_NAME)
    c = db.cursor()
    c.execute(
    """select au.name, count(*) as views from articles ar, authors au, log l
    where l.status='200 OK' and l.path like concat('%',ar.slug)
    and au.id = ar.author group by au.name order by views desc
    """)
    results = c.fetchall()
    db.close()
    return results

def print_result_query2():
    """Print the most popular article authors of all time"""

    print("\n 2. Who are the most popular article authors of all time? ")
    for name, views in query2():
        print(" {} -- {} views".format(name, views))

def query3():
    """Fetch the days on which more than 1% of the requests lead to errors"""

    db = psycopg2.connect(dbname=DB_NAME)
    c = db.cursor()
    c.execute(
    """
    select cast(day as date), percentage
    from
        (select errors.day, round((total_errors/total_requests)*100, 2)
         as percentage
            from
                (select substring(cast(time as text),0,11) as day,
                 cast(count(l.status) as decimal) as total_errors
                    from log l where l.status like '%404%' group by day)
                    as errors, -- subquery total errors
                (select substring(cast(time as text),0,11) as day,
                 cast(count(l.status) as decimal) as total_requests
                    from log l group by day)
                    as requests -- subquery total requests
            where errors.day = requests.day) as result
    where percentage > 1;
    """)
    results = c.fetchall()
    db.close()
    return results

def print_result_query3():
    """Print the days on which more than 1% of the requests lead to errors"""

    print("\n 3. On which days did more than 1% of requests lead to errors? ")
    for day, percentage in query3():
        print(" {:%B %d, %Y} -- {}% errors".format(day, percentage))

#printing results
print_result_query1()
print_result_query2()
print_result_query3()
