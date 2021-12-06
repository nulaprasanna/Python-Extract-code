def person(**data):
    print(data)
    for k,v in data.items():
        if k == 'fname' or k == 'dname':
            print(k, '  :', v)
        elif k == 'age':
            print(k, '    :', v)
        else:
            print(k, ':', v)
    print('')




#person(fname = 'Prasanna',dname = 'Mahathi',age = 30,contact = 999999)
#fname = input("Enter your First Name: ")
#lname = input("Enter your Last Name: ")
#age = input("Enter your age: ")
#contact = input("Enter your Mobile: ")
#person (firstname= fname, lastname=lname, age=age, mobile=contact)


e=1
while e != '0': # while e!= 0 here 0 will be considered as int , so we need to covert this into string
    fname = input("Enter your First Name: ")
    lname = input("Enter your Last Name: ")
    age = input("Enter your age: ")
    contact = input("Enter your Mobile: ")
    person(firstname=fname, lastname=lname, age=age, mobile=contact)
    e = input('Enter 0 to exit or any key to continue :')
    print('')


#input()
