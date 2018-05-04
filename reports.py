import psycopg2

def query1():
    db = psycopg2.connect(dbname="news")
    c = db.cursor()
    c.execute("select title, count(*) as views from articles a, log l where l.status='200 OK' and l.path like concat('%',a.slug) group by a.title order by views desc limit 3")
    results = c.fetchall()
    db.close()
    return results

def print_result_query1():
    print(" 1. What are the most popular three articles of all time? ")
    for title, views in query1():
        print(" \"{}\" -- {} views".format(title, views))


print_result_query1()
