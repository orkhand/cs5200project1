### BUILD: Project I / Design & Implement a Relational Database
Team: Orkhan Dadashov, Meilin Liu
 
### 1.	Business Requirement
![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q1.BusinessRequirement.png?raw=true)

Business Rules
*	A team could participate in multiple Olympic Games
*	A team must participate in 1 Game to be recorded in the database.
* A team attending a Game must have at least 1 athlete.
* An athlete must participate in 1 Olympic Game to be recorded in the database.
* An athlete could participate in multiple events in an Olympic Games.
* An athlete could win several medals in 1 Game.
* An athlete could participate in Olympic Games for different teams, for instance Mary could participated in 1992 Summer with Team US and with Team Japan in 1996 Summer.
* A Sport type could have multiple events
* An event must belong to a Sport type.
* A Game must have multiple teams participating.

### 2. Conceptual Model in UML
![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q3.UML.png?raw=true)

Link to lucidchart: https://lucid.app/lucidchart/47f13deb-0c7c-49cc-9430-6288f6ab24e9/edit?invitationId=inv_6602d40b-2285-42b7-b5b8-753d12cb2c87&page=sAIlboIWdedJ#
### 3. Logical Model in ERD
![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q2.ERD.png?raw=true)

Link to lucidchart: https://lucid.app/lucidchart/47f13deb-0c7c-49cc-9430-6288f6ab24e9/edit?invitationId=inv_6602d40b-2285-42b7-b5b8-753d12cb2c87&page=RkZlGXn.R~cp#
### 4. Relational Schema
- Teams{teamID, country, noc}
- Attendance{teamID, gameID, athleteID}
- Athletes{athleteID, name, sex, age, height, weight}
- Participations{eventID, gameID, athleteID, medal}
- Events{eventID, eventType, sportID}
- Sports{sportID, sportsType}
- Games{gameID, season, year, city}

In the Relation Schema Teams, teamID is unique to every country, and the attribute noc is fully functional dependent on the key {teamID}, therefore, it’s in BCNF.
 
In Attendance, since all the attributes are a subset of primary key, all the FDs are trivial, therefore, it’s in BCNF.
 
In Athletes, an athlete’s name, sex, age, height, weight is all determined by athleteID, and athleteID is primary key, which is a minimal super key, so it’s in BCNF.
 
In Participations, an athlete’s participation in an event in a game can determine the result of the medal. {eventID, gameID, athleteID} -> medal and the primary key is composite key with eventID, gameID, athleteID. Therefore, it is in BCNF.
 
In Events, an eventID uniquely determines the eventType, and an event could only belong to one sport type, which is represented as sportID in this schema. Therefore, it is in BCNF.
 
In Sports, sportID determines sportsType. No other attributes are involved.
 
In Games, the season and year determines the city where a game held. {season, year} -> city. Season and year are superkeys. It’s in BCNF.

 ### 5. Show that the tables were created and conform to the constraints through screen shots or other means.
 We first show the code, which could be found in folder "Project 1 with name  CREATABLE.sql and CreatTableAndInsert.sql" and then embed images to show the invalid sql definitions would fail due to the contraints in the table.
```
 CREATE TABLE IF NOT EXISTS "Teams" (
    "teamID"    Integer,
    "country"   Text NOT NULL,
    "noc"   Text NOT NULL,
    PRIMARY KEY("teamID")
);
CREATE TABLE IF NOT EXISTS "Athletes" (
    "athleteID" Integer,
    "name"  Text NOT NULL,
    "sex"   TEXT CHECK("sex" IN ("M", "F")),
    "age"   Integer NOT NULL,
    "height"    NUMERIC NOT NULL,
    "weight"    NUMERIC NOT NULL,
    PRIMARY KEY("athleteID")
);
CREATE TABLE IF NOT EXISTS "Sports" (
    "sportID"   Integer,
    "sportsType"    Text NOT NULL,
    PRIMARY KEY("sportID")
);
CREATE TABLE IF NOT EXISTS "Games" (
    "gameID"    Integer,
    "season"    TEXT CHECK("season" IN ("Winter", "Summer")),
    "year"  Integer NOT NULL,
    "city"  Text NOT NULL,
    PRIMARY KEY("gameID")
);
CREATE TABLE IF NOT EXISTS "Events" (
    "eventID"   Integer,
    "eventType" Text NOT NULL,
    "sportID"   Integer,
    CONSTRAINT "FK_Events.sportID" FOREIGN KEY("sportID") REFERENCES "Sports"("sportID") ON DELETE CASCADE,
    PRIMARY KEY("eventID")
);
CREATE TABLE IF NOT EXISTS "Participations" (
    "eventID"   Integer,
    "gameID"    Integer,
    "medal" TEXT CHECK("medal" IN ("Gold", "Bronze", "Silver")),
    "athleteID" Integer,
    CONSTRAINT "FK_Participations.athleteID" FOREIGN KEY("athleteID") REFERENCES "Athletes"("athleteID") ON DELETE CASCADE,
    CONSTRAINT "FK_Participations.gameID" FOREIGN KEY("gameID") REFERENCES "Games"("gameID"),
    CONSTRAINT "FK_Participations.eventID" FOREIGN KEY("eventID") REFERENCES "Events"("eventID"),
    PRIMARY KEY("eventID","gameID","athleteID")
);
CREATE TABLE IF NOT EXISTS "Attendance" (
    "teamID"    Integer,
    "gameID"    Integer,
    "athleteID" Integer,
    CONSTRAINT "FK_Attendance.teamID" FOREIGN KEY("teamID") REFERENCES "Teams"("teamID"),
    CONSTRAINT "FK_Attendance.athleteID" FOREIGN KEY("athleteID") REFERENCES "Athletes"("athleteID") ON DELETE CASCADE,
    CONSTRAINT "FK_Attendance.gameID" FOREIGN KEY("gameID") REFERENCES "Games"("gameID"),
    PRIMARY KEY("teamID","gameID","athleteID")
);

```


- Testing to enter incorrect medal type

![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q5.IncorrectMedalType.png?raw=true)

- Trying to add again with the same pk 

![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q5.PKConstraint.png?raw=true)

- Testing Fk with non existing eventID 

![](https://github.ccs.neu.edu/marilynliu/CS5200DatabaseManagement/blob/main/Project1/Pictures/Q5.FKConstraint.png?raw=true)


### 6. Populate the tables with test data

  Please see olympic-games-sqlit.db for the populated table. Also, we attached the code to populate the table named “Olympic Games Analysis - Copy3.ipynb”.

### 7. Define and execute at least five queries that show your database.
```
SELECT *
FROM Athletes
INNER JOIN Participations ON Participations.athleteID = Athletes.athleteID
INNER JOIN Events ON Participations.eventID = Events.eventID
INNER JOIN Sports ON Sports.sportID = Events.sportID
WHERE Sports.sportsType = "Judo"
LIMIT 20


SELECT * 
 FROM Athletes 
 WHERE athleteID IN (SELECT athleteID 
         FROM Athletes 
         WHERE Athletes.sex = "M") 
LIMIT 20


SELECT *
FROM Athletes
GROUP BY age
HAVING age > 25


SELECT Athletes.name, Athletes.age, Participations.medal, Sports.sportsType, Games.year
FROM Athletes
INNER JOIN Participations ON Participations.athleteID = Athletes.athleteID
INNER JOIN Events ON Participations.eventID = Events.eventID
INNER JOIN Sports ON Sports.sportID = Events.sportID
INNER JOIN Games ON  Games.gameID = Participations.gameID
WHERE Participations.medal = "Gold" AND Sports.sportsType = "Swimming" AND Athletes.age > 30


SELECT Athletes.name, Athletes.age,
CASE 
WHEN Athletes.age >= 22 THEN "Old"
ELSE "Young"
END  as agecategory
FROM Athletes
LIMIT 40

-- number of athletes, teams and events in 2008
SELECT COUNT(DISTINCT Participations.eventID) AS nume_events, 
				  COUNT(DISTINCT Attendance.athleteID) AS nume_athletes, 
				  COUNT(DISTINCT Attendance.teamID) AS nume_events 
FROM Games, Attendance, Participations
WHERE Games.gameID = Attendance.gameID
		AND Participations.athleteID = Attendance.athleteID
		AND Games.year = '2008';
			
--percentage of women for medal winners in the history of Olympic Games
SELECT women_winner_num, total_winner_num , ROUND(CAST(women_winner_num AS REAL)/ CAST(total_winner_num AS REAL), 3) * 100 AS 'women_percentage(%)'
FROM ((SELECT COUNT(*) AS women_winner_num
FROM Participations, Athletes
WHERE Participations.athleteID = Athletes.athleteID 
	AND Athletes.sex = 'F' 
	AND Participations.medal NOTNULL) AS WomenWinnerNum, 
(SELECT COUNT(*) AS total_winner_num
FROM Participations, Athletes
WHERE Participations.athleteID = Athletes.athleteID 
	AND Participations.medal NOTNULL ) AS TotalWinnerNum)

--average height for athletes participated in men's swimming since 1992,roudned to 2 decimal points
SELECT ROUND(CAST (SUM(Athletes.height) AS REAL) /  COUNT(*), 2) AS average_height_for_men_swimming_since_92
FROM Athletes, Participations, Games, Sports, Events
WHERE Athletes.athleteID = Participations.athleteID 
	AND Games.gameID = Participations.gameID 
	AND Participations. eventID = Events.eventID 
	AND Events.sportID = Sports.sportID
	AND Games.year >= 1992
	AND Athletes.sex = 'M'
	AND Sports.sportsType = 'Swimming'

--Games where  average age of athletess participated in women's basketball is greater than 26 since 1956
SELECT Games.year, ROUND(CAST (SUM(Athletes.age) AS REAL) /  COUNT(*), 2) AS average_women_age_since_56
FROM Athletes, Participations, Games, Sports, Events
WHERE Athletes.athleteID = Participations.athleteID 
	AND Games.gameID = Participations.gameID 
	AND Participations. eventID = Events.eventID 
	AND Events.sportID = Sports.sportID
	AND Games.year >= 1956
	AND Athletes.sex = 'F'
	AND sportsType = 'Basketball' 
GROUP BY Games.gameID HAVING average_women_age_since_56 > 26;

```
