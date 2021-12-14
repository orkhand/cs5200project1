#!/bin/bash

#mongoimport -h localhost:27017 -d olympic_games -c games --drop --file ./db/games.dump

#mongoimport -h localhost:27017 -d olympic_games -c athletes --drop --file ./db/athletes.dump

# mongoimport -h localhost:27017 -d olympic_games -c sports --drop --file ./db/sports.dump

mongoimport -h localhost:27017 -d olympic_games -c sports --drop --jsonArray --file ./db/sports.json

mongoimport -h localhost:27017 -d olympic_games -c games --drop --jsonArray --file ./db/games.json

mongoimport -h localhost:27017 -d olympic_games -c athletes --drop --jsonArray --file ./db/athletes.json

