def printperson(data): #For reading Dictionary
    for k, v in data.items():
        print(k, ' :', v)

#mydata = {'name': 'Prasanna', 'age' :30, 'city':'OP','country':'US'}

#printperson(mydata)

userdata = {}
e = 1

while e != '0':
    userkey = input ("Enter Key : ")
    uservalue = input("Enter Value : ")
    userdata[userkey] = uservalue
    e = input('Enter 0 to exit or any key to continue :')

printperson(userdata)