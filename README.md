# cs5200project1

## Contributors

- Orkhan Dadashov
    - designing the functions which we will use and also helped me to test and add + update sql qury for our node project
    - Creating Pages : Athlete, Event Sport and main page (Olympic Games)
    - All Search bars in all pages with complex several joins : main, event, sport and athlete page,their view, and all functions and query and connection to the main page
    - Delete the athlete view, and all functions and query and connection to the main page
    - Update the athlete view, and all functions and query and connection to the main page
    - Sport Gender statistics page, view, query and all connecting parts
    - testing & verifying all the functionalities



- Meilin Liu
    - creation of new athlete, it`s view, and all functions and query and connection to the main page, 
    - testing final product of the project, designing the functions which we will use and also helped me to test and add + update 
    - sql query for our node project Delete the athlete, view and all connection parts 
    - sql query for Update the athlete, view and all connection parts 
hr/>

## Project Original Proposal

Our plan is to make an application about data for Olympic athletes during past 120 years. The application will support:

Reporting athlete body info
Searching athlete by countries
CRUD athlete info
Yearly statistics
Age statics

## Project Updated Proposal

Our plan is to make an application about data for Olympic athletes during past 120 years. The application will support:

Search game by country
Athlete:
    Search by athlete name
    Create a new athlete
    Delete an athlete
    Update an athlete information
    the games athlete participated in edit page
Sports:
    Search by sport type
    Also includes Gender count per sport type
Events:
    Search by event type
    Search by sport type


<hr/>


<hr/>

## Part1 - Design & Implement a Relational Database

<hr/>
 
### Source file link  for our project from kaggle

[Olympic Games data file](https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results/version/2)

### Project documentation (PDF file ) includes: 
- The requirements document as a PDF.
- UML Class Diagram as an embedded JPG/PNG.
- ERD as an embedded JPG/PNG and URL to its LucidChart diagram.
- Definition of relational schema with proof that it is in BCNF.

### SQL files will nclude:
- Creat Table : Table creation queries
- Create Table and insert queries

### SQLite DB file:
Which will include already populated tables

### Python notebbok included :
Description : I have created this file to automate reading the excel file and creating the tables an populating it
- Python notebook
- html of the python notebook

### UML
[Project1 UML](https://lucid.app/lucidchart/47f13deb-0c7c-49cc-9430-6288f6ab24e9/edit?invitationId=inv_6602d40b-2285-42b7-b5b8-753d12cb2c87&page=sAIlboIWdedJ#)

![Project1 UML Picture](./images/Project1UML.jpeg)


### ERD 
[Project1 ERD](https://lucid.app/lucidchart/47f13deb-0c7c-49cc-9430-6288f6ab24e9/edit?viewport_loc=-596%2C4%2C3669%2C1955%2CRkZlGXn.R~cp&invitationId=inv_6602d40b-2285-42b7-b5b8-753d12cb2c87)

![Project1 ERD Picture](./images/Project1ERD.jpeg)


### DB Tables
![Project1 DB tables](./images/TablesInDb.png)

### DB Testing incorrect medal Type
- INSERT INTO Participations(eventID, gameID, medal, athleteID) VALUES(765, 30, "Copper", 1) 
![Project1 DB Medal type testing tables](./images/IncorrectMedalTypeCheck.png)

### DB Unique Constraint Check
- INSERT INTO Participations(eventID, gameID, medal, athleteID) VALUES(765, 30, "Gold", 106890) 
![Project1 DB Unique Constraint Check](./images/UniqueConstraintCheck.png)

### DB FK/ PK Constraint Check
- INSERT INTO Participations(eventID, gameID, medal, athleteID) VALUES(766, 30, "Gold", 106890)
![Project1 DB Unique FK/ PK Constraint Check](./images/FKConstraintCheck.png)

<hr/>

## Part2 - Node + Express application

<hr/>

### How to run the code ?

1) Clone the repo
2) Install the dependencies

```
npm install
```


3) Start the server

```
npm start
```

4) Point your browser to http://localhost:3000


### Application Functionalities

All pages support pagination.

#### Main Page - Olymic Games

Main Page showing all the olympic games by city where it was held.
![Main Page](./images/part2/MainPage.png)

Supports searching games by city
![Olympic games search by city](./images/part2/Search_Games_By_City.png)


#### Athletes page

Athletes Main Page, which includes Search bar for searching athlete by name, create an athlete, edit athlete information and delete athlete. 
![Athletes Main Page](./images/part2/Athletes_Page.png)

Search by Athlete Name function
![Search by Athlete Name function](./images/part2/Search_By_Athlete_Name.png)

Edit Athlete Page
![Edit Athlete Page](./images/part2/Edit_Athlete_Page.png)

Now We will demostrate edit function
We pick an Athlete
![Before Edit](./images/part2/Before_Edit_Of_Athlete.png)

We Update Athlete`s Name
![Edit Page Changing the name](./images/part2/Editing_An_Athlete.png)

An Edit result
![The name of the Athlete after edit](./images/part2/After_An_Athlete_was_Eddited.png)

Athlete edit page includes the game athlete participated
![The game Athlete participated](./images/part2/The_Game_Athlete_Participated.png)

On Athlete page creating new Athlete
![New Athlete Creation](./images/part2/Creatign_NewAthlete.png)

New Athlete creation status
![New Athlete Creation Status](./images/part2/Athlete_Creation_Status.png)

After new Athlete created
![New Athlete Created](./images/part2/Created_New_Athlete.png)

Athlete to be deleted
![Athlete to be deleted](./images/part2/Before_Deletion_Of_Athlete.png)

Deletion Status
![Deletion Status](./images/part2/Deletion_Status.png)

Deletion result
![Deletion result](./images/part2/Deletion_Result.png)

#### Sports page
Sports Main Page Which includes search by sports type  and also supports get gender statistics per Sport Type
![Sports Main Page](./images/part2/Sports_Main_Page.png)

Searching Sport By Name
![Searching Sport By Name](./images/part2/Searching_Sport_By_Name.png)

Searching Sport By Name Result
![Searching Sport By Name Result](./images/part2/Search_Result_By_Sport_Name.png)


Show sports gender statistics Result
![Show sports gender statistics Result](./images/part2/Gender_count_by_sport.png)

#### Events page

Events Page, which inludes 2 search bars.Search by event Name and Show events by sport Type

![Events Main Page](./images/part2/Events_Main_Page.png)

Search by Event Name result
![Search by Event Name result](./images/part2/Search_By_Name_Result.png)

Search by Sports Type result
![Search by Sports Type result](./images/part2/Search_By_Sports_Type_Result.png)