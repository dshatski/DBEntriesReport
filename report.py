import MySQLdb as mdb
import random, string
from datetime import date, datetime, timedelta
from operator import itemgetter # fast sort

def getCursor(host, user, passwd, name):
    db=None

    # connect to MySQL DB with above credentials
    # complain if unsuccessful
    # return DB cursor if successful
    try:
        db=mdb.connect(host, user, passwd, name)
        print("Connected as "+user+" to "+name+" at " +host)
    except mdb.DatabaseError as e:
        print("Could not connect: {}".format(e))

    # make sure there is a connection
    if (db!=None):
        return db.cursor()
    else:
        return None

def updateMailingTable(cursor, numEmails, maxLength):

    # random generator of emails on 10 domains
    # assume whitespaces, extra '@' or empty local parts
    # assume whitespaces, missing/extra '@' or empty domain names
    def email_generator(length):
        chars=string.ascii_letters+' '+'@'
        domains = ['','@gmail.com','@yahoo.com','@hotmail.com','@gmx.de','@google mail.com','@mail.ru','@web@de','@live.com','aol.com']
        return ''.join(random.choice(chars) for _ in range(random.randrange(length)))+random.choice(domains)
    
    # make sure table exists
    if(cursor.execute("SHOW TABLES LIKE 'mailing';")==0):
        # if not, create a table
        # complain if unsuccessfull
        try:
            cursor.execute("""CREATE TABLE mailing (
                        addr VARCHAR(255) NOT NULL
                        );""")
        except Exception as e:
            print("Could not create table: {}".format(e))

    # populate table with numEmails which local parts are 0 to maxLength long
    for i in range(numEmails):
        cursor.execute("INSERT INTO mailing (addr) VALUES (%s)", email_generator(maxLength))
    cursor.execute("COMMIT;")

def updateCountingTable(cursor, dateVar):
    new = {}
    
    # increment key counter in dic
    # create a key if used first time
    def count(dic, key):
        if (key not in dic.keys()):
            dic[key] = 1
        else:
            dic[key]+=1
        
    # make sure table exists
    if(cursor.execute("SHOW TABLES LIKE 'counting';")==0):
        # if not, create a table
        # complain if unsuccessfull
        try:
            cursor.execute("""CREATE TABLE counting (
                        day DATE,
                        domain VARCHAR(255) NOT NULL,
                        count INT
                        );""")
        except Exception as e:
            print("Could not create table: {}".format(e))

    # process new entries only
    cursor.execute("SELECT addr FROM mailing;")
    total = cursor.rowcount

    counted=0
    cursor.execute("SELECT count FROM counting;")
    for row in cursor:
        counted = counted + row[0]

    cursor.execute("SELECT addr FROM mailing LIMIT "+str(counted)+","+str(total-counted)+";")   # adding only new items is important if mailing
                                                                                                # and counting tables are updated independently
    for row in cursor:
        parsed = row[0].split('@')
        if ((len(parsed)!=2) or (parsed[0]=='') or (parsed[1]=='') or          # filter out whitespaces, multiple '@'s and
            (len(parsed[0].split(' '))!=1) or (len(parsed[1].split(' '))!=1)): # empty values using short-curcuiting
            count(new, 'corrupted') # count as incorrect email
        else:
            count(new,parsed[1]) # increment by domain name
                                 # this will still store incorrect but otherwise syntactically correct domains, like in foo@gibberish
                                 # this can be corrected by filtering out domain names which don't contain .com, .org, etc.
                                 # or by comparing the parsed[1] with a list of known domains
                                 # both filters require previous knowledge of the emails being used
    dateStamp = "{:%Y%m%d}".format(dateVar) # get date stamp in generic SQL format YYYYMMDD
    for key in new.keys():
        cursor.execute("INSERT INTO counting (day, domain, count) VALUES ("+dateStamp+", '"+key+"', "+str(new[key])+");")
    cursor.execute("COMMIT;") # update the counting SQL table

def reportTop(cursor, top, daysBack):
    # collecting count statistics per domain
    raw = {}
    cursor.execute("SELECT day, domain, count FROM counting;")
    for (day, domain, count) in cursor:
        if (domain not in raw.keys()): # new domain
            raw[domain] = [0, 0] # initialize statistics for the new domain
        raw[domain][0] += count  # total count
        if (date.today()-timedelta(daysBack)<day): # entry is no earlier than daysBack from now
            raw[domain][1] += count  # recent growth count
    if ('corrupted' in raw.keys()):
        raw.pop('corrupted', None) # remove corrupted entries, if any

    # assuming that top*O(n) is faster than ordering n items
    report=[]
    i=0
    while(i<top):
        maxDomain=None
        maxVal=-1L
        for domain in raw.keys():
            if raw[domain][0]>maxVal:
                maxVal=raw[domain][0]
                maxDomain=domain
        if (maxDomain == None): # ran out of domains
            break
        report.append((maxDomain, float(raw[maxDomain][1])/raw[maxDomain][0])) # list of tuples for quick sorting
        raw.pop(maxDomain, None) # remove the winner
        i+=1

    # print results
    print("In the last "+str(daysBack)+" days the following top "+str(top)+" domains\nshowed percentage growth compared to the total:")
    for domain, growth in sorted(report,key=itemgetter(1), reverse=True):
        print(domain+' gained '+"{:.2%}".format(growth))

cur = getCursor(host="***",
                user="***",
                passwd="***",
                name="***") # get a db cursor
if (cur!=None):
    for day in range(0,90): # emulate daily list update
        updateMailingTable(cur, 1000, 20) # update 'mailing' table with more emails
        updateCountingTable(cur, date.today()+timedelta(day)) # update 'counting' table with domain statistics;
                                                              # does not have to be as frequent as updateMailingTable
                                                              # or be called immediately after
    reportTop(cur, 5, 30) # report top 5 domains for the last 30 days
