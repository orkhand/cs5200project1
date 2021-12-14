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