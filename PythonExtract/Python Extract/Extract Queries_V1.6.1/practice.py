tinytuple = [123, 'john']
print (tuple)
#list[2] = 1000
#print(list)
##tuple[2]=1000
##print(tuple)
#dict = {}
#dict['one'] = "This is one"
#dict[2] = "This is two"
#tinydict = {'name': 'john', 'code': 6734, 'dept': 'sales'}
#print (dict['one'])
#print (dict[2])
#print (tinydict)
#print (tinydict.keys())
#print (tinydict.values())
#
#var1 = "Hello World!"
#var2 = "Python Programming"
#
#print ("var1[0]: ", var1[0])
#print ("var2[1:5]: ", var2[1:5])
#print ("update String :- ", var1[:6] + 'Python')
#print("My Name is %s and weight is %d kg!" %('Prasanna', 21))
#2+2
#
#a, b = 0, 1
#while a < 10:
#    print (a)
#    a, b = b, a+b
#
#var =  1/10 + 2/100 + 5/1000
#print (var)

#print('This is a string {}:'.format('INSERTED'))

#print('The {0} {0} {0}:'.format('fox','brown','quick'))

#print('The {q} {f} {b}:'.format(f='fox',b='brown',q='quick'))

#result=100/777
#result=104.12345
#print (result)
#print ("The result was {r:2.4f}".format(r=result))

#name="Jose"
#print ('Hello, his nam is {}'.format(name))
#print (f'Hello, his name is {name}')

#name ="Sam"
#age=3
#print(f'{name} is {age} years old ')

###LISTS

#my_list=[1,2,3]
#my_list=['string',1,3.5]
#print(len(my_list))
#print(my_list[0])

#mylist=['one','two','three','four']
#another_list=['five','six']

#print(mylist + another_list)
#mylist.append('six')
#print(mylist)
#mylist.append('seven')
#print(mylist)
#print(mylist.pop())
#print(mylist)
#print(mylist.pop(0))
#print(mylist)
#print(mylist.pop(-1))
#print(mylist)

#newlist=['a','e','x','b','c']
#print(newlist)
#newlist.sort()
#print(newlist)
#newlist.reverse()
#print(newlist)

#mylist=[1,1,[1,2]]
#print(mylist[2][0])

#lst=['a','b','c']
#print(lst[1:])

#price_lkp = {'apple':2.5,'orange':5.4, 'grapes':6.9}
#print(price_lkp['apple'])

#d={'key1':['a','b','c']}
#print(d['key1'][0])

#d={'key1':100,'Key2':200}
#print(d)
#d['k3']=100
#print(d)
#d['k3']=500
#print(d)
#print(d.values())
#print(d.items())

#t=(1,2,3)
#mylist=[1,2,3]
#print(type(t))
#print(len(t))
#t=('a','a','b')
#print(t.count('a'))
#print(t.count('a'))

#myset=set()
#print (myset)
#myset.add(1)
#print (myset)
#myset.add(2)
#print (myset)

#print(True)
#type(False)
#myset=set([1,1,2,3])
#print(myset)
#def make_tags(tag, word):
#make_tags('i', 'Yay')
#print (make_tags[1])

def spy_game(nums):
    code=[0,0,7,'x']
    for num in nums:
        if num ==code[0]:
            code.pop(0)
        return len(code) == 1
spy_game([1,2,4,0,0,7,5])


def count_primes(num):
    #Check for 0 or input
    if num < 2:
        return 0
    #Check for 2  or greater

    #Store Prime Numbers
    primes = [2]

    #counter to goint to the program
    x = 3
    while x < num:
        for y in range(3,x,2):
            if x%y == 0:
                x += 2
                break
        else:
            primes.append(x)
            x += 2
    print(primes)
    return len(primes)
count_primes(100)

def square(num):
    return num**2
mynums=[1,2,3,4,5]
for item in map(square,mynums):
    print(item)

def slicer(mystring):
    if len(mystring)%2 == 0:
        return 'EVEN'
    else:
        return mystring[0]
names = ['andy','Prasanna','eve']
print(list(map(slicer,names)))

def check_even(num):
    return num%2 == 0
mynums=[1,2,3,4,5,6]
print(list(filter(check_even,mynums)))

def square(num):
    result = num**2
    return result

print(square(3))

square = lambda num: num ** 2
print(square(4))

print(list(map(lambda num: num ** 2,mynums)))

#pint(list(map(lambda num: num % 2 =0,mynums)))

x = 25
def printer():
    x = 50
    return x
print(x)
print(printer())


import pandas as pd
import pyodbc

conn = pyodbc.connect('Driver={Teradata};'
                      'Server=TDPROD;'
                      'Database=TestDB;'
                      'Trusted_Connection=yes;')

sql_query = pd.read_sql_query(''' 
                              select * from TestDB.dbo.Person
                              '''
                              ,conn) # here, the 'conn' is the variable that contains your database connection information from step 2

df = pd.DataFrame(sql_query)
df.to_csv (r'C:\Users\Ron\Desktop\export_data.csv', index = False) # place 'r' before the path name to avoid any errors in the path