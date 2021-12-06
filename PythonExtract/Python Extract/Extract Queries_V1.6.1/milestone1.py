#print([1,2,3])

#def display(row1,row2,row3):
#    print(row1)
#    print(row2)
#    print(row3)
#row1=[1,2,3]
#row2=['','','']
#row3=['','','']
#row2[1]='x'
#display(row1,row2,row3)
#result = int(input("Please enter a value"))
#print(int(result))
#print(type(result))

#result = input("Enter a Number: ")

#def user_choice():
 #   choice='WRONG'
 #   while choice.isdigit() == 'False':
  #      choice = input("Please enter a number (0-10):")
   # return int(choice)
#user_choice()

#print(user_choice())

#some_value='100'
#print(some_value.isdigit())


game_list = [0,1,2]
def display_game(game_list):
    print("Here is the Current List :")
    print(game_list)

#display_game(game_list)

def postion_choice():
    choice = 'Wrong'
    while choice not in ['0','1','2']:
        choice = input("Pickup a position (0,1,2) :")
        if choice not in ['0','1','2']:
            print("Sorry ! Invalid Choice")
    return int(choice)
#postion_choice()

def replacement_position(game_list,position):
    user_replacemet = input("Type a string to place at postion:")
    game_list[position] = user_replacemet
    return game_list

#replacement_position(game_list,1)

def gameon_choice():
    choice='WRONG'
    while choice not in ['Y','N']:
        choice=input("Keep playing(Y or N)")
        if choice not in ['Y','N']:
            print("Sorry , i dont understand please enter (y or n)")
    if choice == 'Y':
        return True
    else:
        return False

game_on=True
game_list=[0,1,2]
while game_on:
    display_game(game_list)
    position = postion_choice()
    game_list = replacement_position(game_list,position)
    display_game(game_list)
    game_on=gameon_choice()