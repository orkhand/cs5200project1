const { MongoClient } = require("mongodb");

async function getGames(query, page, pageSize) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const gamesCollection = db.collection("games");

    // MQL 👉 json
    let q = {};
    let m = {$match:{"city" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    result = await gamesCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
    console.log("getGames() : result", result);
  }
}

async function getGamesCount(query) {
  let db, client, result;
  
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
  
    console.log("Connected to Mongo Server");
  
    db = client.db("olympic_games");
    const gamesCollection = db.collection("games");
  
    // MQL 👉 json
    const q = {"city" : new RegExp("^" + query, "i") };
    console.log("getGamesCount() : query", q);
    result = gamesCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}



async function getSports(query, page, pageSize) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
    
    // MQL 👉 json
    let q = {};
    let m = {$match :{"sportsType" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    console.log("getSports() : query", q);
    result = await sportsCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
  }
}


async function getSportsCount(query) {
  let db, client, result;
      
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
      
    // MQL 👉 json
    const q = {"sportsType" : new RegExp("^" + query, "i") };
    console.log("getSports() : query", q);
    result = sportsCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}

async function getEvents(query, page, pageSize) {
  let db, client, result;
      
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
      
    // MQL 👉 json
    let q = {};
    console.log("getEvents() : query", q);
    let m =  { $match:{"events.eventType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let m2 = { $match : {"events.eventType": new RegExp("^" + query, "i")}};
    let p = { $project : { _id :0, eventType : "$_id"} };
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, uw, m2, g, p, s, l ];
    result = await sportsCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEvents", result.length);
  }
}
  
async function getEventsCount(query) {
  let db, client, result;
        
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
        
    console.log("Connected to Mongo Server");
        
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
        
    // MQL 👉 json
    let q = {};
    console.log("getEvents() : query", q);
    let m =  { $match:{"events.eventType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let m2 = { $match : {"events.eventType": new RegExp("^" + query, "i")}};
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw, m2, g, p ];
    result = (await sportsCollection.aggregate(q).toArray()).length;
    return result;
  } finally {
    await client.close();
    console.log("getEventsCount", result);
  }
}

async function getEventsBySport(query, page, pageSize) {
  let db, client, result;
        
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
        
    console.log("Connected to Mongo Server");
        
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
        
    // MQL 👉 json
    let q = {};
    let m =  { $match:{"sportsType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw, g, s, l, p ];// 
    console.log("q", q);
    result = await sportsCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEventsBySport",result.length);
  }
}
    
    
async function getEventsBySportCount(query) {
  let db, client, result;
          
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
          
    console.log("Connected to Mongo Server");
          
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
          
    // MQL 👉 json
    let q = {};
    let m =  { $match:{"sportsType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw,  g, p ];
    result =  await sportsCollection.aggregate(q).toArray();
    return  result.length;
  } finally {
    await client.close();
  }
}

async function getGenderStatisticsBySportType(sportId) {
  let db, client, result;
          
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
          
    console.log("Connected to Mongo Server");
          
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
          
    // MQL 👉 json
    let q = {};
    sportId = parseInt(sportId);
    console.log("sportId",sportId);
    let m =  { $match: {"participations.sportId":  sportId }};
    let g = { $group: { _id: "$sex", count : {$sum : 1}}};
    let p = { $project : { _id :0, sex : "$_id", count : 1} };
    q = [m, g , p];// 
    console.log("q", q);
    result = await athletesCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEventsBySport", result);
  }
}

async function getAthletes(query, page, pageSize) {
  let db, client, result;

  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    let q = {};
    console.log("getAthletes() : query", q);
    let m = {$match :{"name" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    result = await athletesCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
  }
}

async function getAthletesCount(query) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    const q = {"name" : new RegExp("^" + query, "i") };
    console.log("getAthletes() : query", q);
    result = athletesCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}

async function getAthleteByID(query) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    const q =  {"athleteId":  parseInt(query)};
    console.log("getAthleteById() : query", q);
    result = await athletesCollection.find(q).toArray();
    console.log("getAthleteById result: " + JSON.stringify(result));
    return result[0];
  } finally {
    await client.close();
  }
}

async function getGamesByAthleteID(query) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q = {};
    let q_int = parseInt(query);
    let m =  { $match:{"athleteId": q_int}};
    let lu = { $lookup:{"from": "games", "localField": "participations.gameId", "foreignField": "gameId", "as": "games_participated"}};
    let uw = { $unwind : "$games_participated" };
    let r = { $replaceRoot: {"newRoot": "$games_participated"}};
    let p = { $project : { _id :0, "year": 1, "season": 1, "city": 1} };
    q = [m, lu, uw, r, p];
    result = await athletesCollection.aggregate(q).toArray();
    console.log("getGamesByAthleteID() : result", result);
    return  result;
  } finally {
    await client.close();
  }
}

async function updateAthletesByID(query,ref) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q_int = parseInt(query);
    //---------------------------------------------------
    let q =  { "athleteId":  q_int, };
    
    console.log("new name: " + ref.name);
    let newValues = {$set: {"name": ref.name}};
    
    result = await athletesCollection.updateOne(q, newValues);
    console.log("updateAthletesByID() : result", JSON.stringify(result));
    return  result;
  } finally {
    await client.close();

  }
}

async function deleteAthletesByID(query) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q =  { "athleteId":  parseInt(query)};
    result = await athletesCollection.deleteOne(q);
    return  result;
  } finally {
    await client.close();
    console.log("updateAthletesByID() : result", JSON.stringify(result));
  }
}

async function createAthlete(ref) {
  let db, client;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let athleteId =  await athletesCollection.find().sort({athleteId:-1}).limit(1).toArray();
    console.log("createAthlete athleteId :", athleteId);
    athleteId = parseInt(athleteId[0].athleteId) + 1;
    ref.athleteId = athleteId;

    console.log("createAthlete",athlete);

   await athletesCollection.insertOne(athlete);
  } finally {
    await client.close();
    console.log("createAthlete() : done");
  }
}

async function createGame(ref) {
  let db, client;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const gamesCollection = db.collection("games");

    // MQL 👉 json
    let gameId = await gamesCollection.find().sort({gameId:-1}).limit(1).toArray();
    console.log("createGame gameId :", gameId);
    gameId = parseInt(gameId[0].gameId) + 1;
    ref.gameId = gameId;
    console.log("createGame game: ", ref);
    await gamesCollection.insertOne(ref);
    return gameId;
  } finally {
    await client.close();
    console.log("createAthlete() : done");
  }
}

async function getGameId(ref, db){
  const gamesCollection = db.collection("games");
  let f = {"season": ref.season, "year": ref.year};
  let p = {_id: 0, gameId: 1};
  let result = gamesCollection.find(f, p).toArray()[0].gameId;
  console.log("gameId: " + result);
  return result;
}

async function getTeamId(ref, db, gameId){
  const gamesCollection = db.collection("games");
  let q = [];
  let m = {$match: {"gameId": gameId}};
  let uw = {$unwind: {"path": "$teams"}};
  let rr = {$replaceRoot: {"newRoot": "$teams"}}; 
  let m2 = {$match: {"country": ref.country}}; 
  let p = {$project: {"teamId": 1}};
  
  q = [m, uw, rr, m2, p];
  let result = await gamesCollection.aggregate(q).toArray();
  let str = "";
  result.forEach(function(item){
    if (item != null) {
      str = item.id;
      console.log("teamId:" + str);
      return str;
    }
  });
}


module.exports.getGames = getGames;
module.exports.getGamesCount = getGamesCount;
module.exports.getSports = getSports;
module.exports.getSportsCount = getSportsCount;
module.exports.getEvents = getEvents;
module.exports.getEventsCount = getEventsCount;
module.exports.getEventsBySport = getEventsBySport;
module.exports.getEventsBySportCount = getEventsBySportCount;
module.exports.getGenderStatisticsBySportType = getGenderStatisticsBySportType;
module.exports.getAthletes = getAthletes;
module.exports.getAthletesCount = getAthletesCount;
module.exports.getAthleteByID = getAthleteByID;
module.exports.getGamesByAthleteID = getGamesByAthleteID;
module.exports.updateAthletesByID = updateAthletesByID;
module.exports.deleteAthletesByID = deleteAthletesByID;
module.exports.createAthlete = createAthlete;
module.exports.createGame = createGame;