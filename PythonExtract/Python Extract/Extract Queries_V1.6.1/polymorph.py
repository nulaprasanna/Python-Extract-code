class duck():
    def sound(self):
        print("Quack Quack")
class Dog():
    def sound(self):
        print("Bhow Bhow")

class cat():
    def sound(self):
        print("Meow Meow")
def anysound(obj):
    obj.sound()

x = cat()
y = Dog()


class india():
    def capital(self):
        print("New Delhi")
    def language(self):
        print("Hindi")
def caplan(obj):
    obj.capital()
    obj.lanuage()
x= india()
caplan()

anysound(x)
