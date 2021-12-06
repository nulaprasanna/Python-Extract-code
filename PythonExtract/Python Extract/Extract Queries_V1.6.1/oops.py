def add_two_numbers(x, y):  # function header
    """
    Takes in two numbers and returns the sum
    parameters
        x : str
            first number
        y : str
            second number
    returns
        x+y
    """
    z = x + y
    return z  # function return
print(add_two_numbers(100,5))  # function  call


mylist = [1,2,3]




myset=set()

print(type(myset))

class Sample():
    pass
mysample = Sample()
type(mysample)

class Dog():
    def __init__(self,breed,name,spots):
        #attributes
        #we take in the argument
        #assign it as a useful self.attributename
        self.breed = breed
        self.name = name
        self.spots = spots
my_dog = Dog(breed='LAB',name='Samy',spots='no spots')
type(my_dog)
my_dog.breed
my_dog.name
my_dog.spots
type(my_dog)



class Dog():
    #class object attribute
    #same for anu instance of a class
    species = 'mammal'

    def __init__(self,breed,name,spots):
        #attributes
        #we take in the argument
        #assign it as a useful self.attributename
        self.breed = breed
        self.name = name
        self.spots = spots
#Opertaions,Actions--Methods
    def bark(self):
        print('WOOF!')
        print("WOOF! MY NAME IS {}".format(self.name))

    def bark1(self,number):
        print("WOOF! MY NAME IS {} and the number is {}".format(self.name,number))
my_dog = Dog(breed='LAB',name='Samy',spots='False')
type(my_dog)
my_dog.breed
my_dog.name
my_dog.spots
my_dog.species
my_dog.bark()
my_dog.bark1(10)



class circle:
    #Class object Attribute
    pi = 3.14

    def __init__(self,radius=1):
        self.radius = radius
    def get_cirumfrence(self):
        return self.radius * self.pi * 2
my_circle = circle(30)
my_circle.pi
my_circle.radius
my_circle.get_cirumfrence()



class circle:
    #Class object Attribute
    pi = 3.14

    def __init__(self,radius=1):
        self.radius = radius
        self.area = radius * radius * circle.pi
    def get_cirumfrence(self):
        return self.radius * circle.pi * 2
my_circle = circle(30)
my_circle.pi
my_circle.radius
my_circle.get_cirumfrence()


class animal():
    def __init__(self):
        print("ANIMAL CREATED")
    def who_am_i(self):
        print("I am animal")
    def eating(self):
        print("I am eating")
class dog(animal):
    def __init__(self):
        animal.__init__(self)
        print("Dog Created")
    def who_am_i(self):
        print("I am a dog")
    def bark(self):
        print('WOOF!')
    def eating(self):
        print("I am dog eating")

my_dog = dog()
my_dog.who_am_i()
my_dog.eating()
my_dog.bark()
