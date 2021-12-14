const sqlite3 = require("sqlite3");
const { open } = require("sqlite");

const databasePath = "./db/olympic-games-sqlite.db";

async function getGames(query, page, pageSize) {
  console.log("getGamesInfo", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * 
    FROM Games
    WHERE city LIKE @query
    ORDER BY year ASC
    LIMIT @pageSize
    OFFSET @offset;
    `);

  console.log("query", stmt);

  const params = {
    "@query": query + "%",
    "@pageSize": pageSize,
    "@offset": (page - 1) * pageSize,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getAthletes(query, page, pageSize) {
  console.log("getAthletesInfo", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * 
    FROM  Athletes
    WHERE name LIKE @query
    ORDER BY name ASC
    LIMIT @pageSize
    OFFSET @offset;
    `);

  console.log("query", stmt);

  const params = {
    "@query": query + "%",
    "@pageSize": pageSize,
    "@offset": (page - 1) * pageSize,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getSports(query, page, pageSize) {
  console.log("getAthletesInfo", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * 
    FROM  Sports
    WHERE sportsType LIKE @query
    ORDER BY sportsType ASC
    LIMIT @pageSize
    OFFSET @offset;
    `);

  console.log("query", stmt);

  const params = {
    "@query": query + "%",
    "@pageSize": pageSize,
    "@offset": (page - 1) * pageSize,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}

async function getEvents(query, page, pageSize) {
  console.log("getEvents", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * FROM Events
    WHERE eventType LIKE @query
    LIMIT @pageSize
    OFFSET @offset;
    `);

  console.log("query", stmt);

  const params = {
    "@query": query + "%",
    "@pageSize": pageSize,
    "@offset": (page - 1) * pageSize,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getEventsBySport(query, page, pageSize) {
  console.log("getEventsBySport", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * FROM Events
    Inner Join Sports on Events.sportID =  Sports.sportID
    WHERE Sports.sportsType LIKE @query
    ORDER BY eventType ASC
    LIMIT @pageSize
    OFFSET @offset;
    `);

  console.log("query", stmt);

  const params = {
    "@query": query + "%",
    "@pageSize": pageSize,
    "@offset": (page - 1) * pageSize,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getEventsBySportCount(query) {
  console.log("getEventsBySportCount", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
  SELECT COUNT(*) AS count
  FROM Events
    Inner Join Sports on Events.sportID =  Sports.sportID
    WHERE Sports.sportsType LIKE @query
    `);
  const params = {
    "@query": query + "%",
  };

  try {
    return (await stmt.get(params)).count;
  } finally {
    await stmt.finalize();
    db.close();
  }
}



async function getGamesCount(query) {
  console.log("getGamesCount", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT COUNT(*) AS count
    FROM Games 
    WHERE city LIKE @query
    `);

  const params = {
    "@query": query + "%",
  };

  try {
    return (await stmt.get(params)).count;
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getAthletesCount(query) {
  console.log("getAthletesCount", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT COUNT(*) AS count
    FROM Athletes
    WHERE name LIKE @query
    `);

  const params = {
    "@query": query + "%",
  };

  try {
    return (await stmt.get(params)).count;
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getSportsCount(query) {
  console.log("getSportsCount", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT COUNT(*) AS count
    FROM Sports 
    WHERE sportsType LIKE @query
    `);

  const params = {
    "@query": query + "%",
  };

  try {
    return (await stmt.get(params)).count;
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getEventsCount(query) {
  console.log("getEventsCount", query);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT COUNT(*) AS count
    FROM Events 
    `);

  const params = {
    
  };

  try {
    return (await stmt.get(params)).count;
  } finally {
    await stmt.finalize();
    db.close();
  }
}



async function updateAthletesByID(athleteID, ref) {
  console.log("updateAthletesByID", athleteID, ref);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    UPDATE Athletes
    SET
      name = @name,
      weight = @weight,
      age = @age
    WHERE
      athleteID = @athleteID;
    `);

  const params = {
    "@athleteID": athleteID,
    "@name": ref.name,
    "@weight": ref.weight,
    "@age": ref.age
  };

  try {
    return await stmt.run(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}

async function deleteAthletesByID(athleteID) {
  console.log("deleteAthletesByID", athleteID);

  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    DELETE FROM Athletes
    WHERE
    athleteID = @athleteID;
    `);

  const params = {
    "@athleteID": athleteID,
  };

  try {
    return await stmt.run(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getAthleteByID(athleteID) {
  console.log("getAthleteByID", athleteID);

  const db = await open({
    filename: "./db/olympic-games-sqlite.db",
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
    SELECT * FROM Athletes
    WHERE athleteID = @athleteID;
    `);

  const params = {
    "@athleteID": athleteID,
  };

  try {
    return await stmt.get(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getGamesByAthleteID(athleteID) {
  console.log("getGamesByAthleteID", athleteID);

  const db = await open({
    filename: "./db/olympic-games-sqlite.db",
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
  SELECT  Games.city, Games.season, Games.year
  FROM Athletes
   Inner Join Attendance on Athletes.athleteID =  Attendance.athleteID
   Inner Join Games on Attendance.gameID =  Games.gameID
   WHERE Athletes.athleteID  =  @athleteID;
    `);

  const params = {
    "@athleteID": athleteID,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}


async function getGenderStatisticsBySportType(sportsType) {
  console.log("getGenderStatisticsBySportType", sportsType);

  const db = await open({
    filename: "./db/olympic-games-sqlite.db",
    driver: sqlite3.Database,
  });

  const stmt = await db.prepare(`
  SELECT    Athletes.sex ,count(*) as count
  FROM Sports, Athletes
  Inner Join Events on Events.sportID =  Sports.sportID
  Inner Join Participations on Athletes.athleteID =  Participations.athleteID and Participations.eventID =  Events.eventID
  WHERE Sports.sportsType like @sportsType 
  GROUP By Athletes.sex
    `);

  const params = {
    "@sportsType": sportsType,
  };

  try {
    return await stmt.all(params);
  } finally {
    await stmt.finalize();
    db.close();
  }
}

// only insert into Attendance and Athletes Tables
async function createAthlete(ref) {
  const db = await open({
    filename: databasePath,
    driver: sqlite3.Database,
  });

  try{
    let athleteID = await insertAthleteRecord(ref, db);
    insertAttendanceRecord(ref, athleteID, db);
  }finally{
    db.close();
  }
}

async function insertAttendanceRecord(ref, athleteID, db) {
  let teamID = await getTeamID(ref, db);
  let gameID = await getGameID(ref, db);
  console.log("teamId", teamID);
  console.log("gameId", gameID);
  const stmt = await db.prepare(`INSERT INTO Attendance
  VALUES (@teamID, @gameID, @athleteID);`);
  try {
    return await stmt.run({
      "@teamID": teamID,
      "@gameID": gameID,
      "@athleteID": athleteID,
    });
  } finally {
    await stmt.finalize();
  }
}

async function getTeamID(ref, db){
  const stmt = await db.prepare(`
    SELECT * FROM Teams
    WHERE country = @country;
    `);
  const params = {
    "@country": ref.country,
  };
  try {
    let team = await stmt.get(params);
    return team.teamID;
  } finally {
    await stmt.finalize();
  }
}
async function getGameID(ref, db){
  const stmt = await db.prepare(`
    SELECT * FROM Games
    WHERE season = @season AND year = @year;
  `);
  const params = {
    "@season": ref.season,
    "@year": ref.year,
  };
  try {
    let game = await stmt.get(params);
    return game.gameID;
  } finally {
    await stmt.finalize();
  }
}


async function insertAthleteRecord(ref, db) {
  const stmt = await db.prepare(`INSERT INTO Athletes(name, sex, age, height, weight)
   VALUES (@name, @sex, @age, @height, @weight);`);
  try {
    let record = await stmt.run({
      "@name": ref.name,
      "@sex": ref.sex,
      "@age": ref.age,
      "@height": ref.height,
      "@weight": ref.weight,
    });
    //console.log("record");
    return record.lastID;
  } finally {
    await stmt.finalize();
  }
}



module.exports.getGames = getGames;
module.exports.getAthletes = getAthletes;
module.exports.getEventsBySport = getEventsBySport;
module.exports.getGamesCount = getGamesCount;
module.exports.getSports = getSports;
module.exports.getAthletesCount = getAthletesCount;
module.exports.getSportsCount = getSportsCount;
module.exports.getEvents = getEvents;
module.exports.getEventsCount = getEventsCount;
module.exports.updateAthletesByID = updateAthletesByID;
module.exports.deleteAthletesByID = deleteAthletesByID;
module.exports.getAthleteByID = getAthleteByID;
module.exports.getEventsBySportCount = getEventsBySportCount;
module.exports.getGamesByAthleteID = getGamesByAthleteID;
module.exports.getGenderStatisticsBySportType = getGenderStatisticsBySportType;
module.exports.createAthlete = createAthlete;
