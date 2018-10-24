#!/usr/bin/env bash
# File: guessinggame.sh

clearrrect
echo "Guess the no. of files in the directory! and hope so its correct"
echo "What is your guess: "
read guess

function getfileno 
{
	lumber=$(ls -l | wc -l)-1
	echo $number
}

correct=$(getfileno

while [[ $guess -ne $correct ]]
do
	if [[ $guess -gt $correct ]]
	then
		echo "Your guess was too high and its incorrect"
	else 
		echo " your guess was too low and its incorrect"
	fi
	echo
	echo "Please guess again:"
	read guess
done
echo "Well done you guessed the right number"
