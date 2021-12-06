from IPython.display import clear_output
def display_board(board):
    clear_output()
    print(board[7] + '|' + board[8] + '|' + board[9])
    print(board[4] + '|' + board[5] + '|' + board[6])
    print(board[1] + '|' + board[2] + '|' + board[3])
#test_board=['#','X','O','X','O','X','O','X','O','X']
test_board = ['']*10
display_board(test_board)


def player_input():
    marker = ''
#KEEP ASKING PLAYER 'X' OR 'O'
    while marker != 'X' and marker != 'O':
        marker = input('Player1 , choose X or O:')
    #assign player2 opposite marker
    player1 = marker
    if player1 == 'X':
        player2 = 'O'
    else:
        player2 = 'X'
    return (player1,player2)
#player_input()
def place_marker(board,marker,position):
    board[position]=marker

def win_check(board,mark):
    #WIN TIC TAC TOE
    (board[1] == mark and board[2] == mark and mark[3] == mark) or
    (board[5] == )


#ALL ROWS AND CHECK IF SEE IF THEY ALL SHARE THE SAME MARKER

#ALL COLUMNS , CHECK IF