const { MongoClient } = require("mongodb");
const { createClient } = require("redis");

const myDb = require("../db/myMongoDB.js");

async function populateRedis() {
    let client,sports, events, key, hKey, count;
    try {
      client = createClient();
      client.on("error", (err) => console.log("Redis Client Error", err));
      await client.connect();
      client.flushAll('ASYNC');
  
      console.log("connected to Redis Server");
      count = await myDb.getSportsCountRedis();
      sports = await myDb.getSports("", 1, count);
  
      for(let i = 0; i < sports.length; i++){
          hKey = `sportID:${sports[i].sportId}`
          await client.set(hKey, sports[i].sportsType + "," +sports[i].sportId);
          await client.rPush("sports", hKey);
          key = sports[i].sportsType;
          events = await myDb.getEventsBySportRedis(key);
  
          for(let j = 0; j < events.length; j++){
            await client.rPush(key, events[j].eventType);
          } 
      }
      console.log("Redis was populated!");
    } finally {
      await client.quit();
    }
  }

  module.exports.populateRedis = populateRedis;
  populateRedis();

  