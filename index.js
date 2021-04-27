const csv = require("csv-parser");
const fs = require("fs");
require("dotenv").config();
const { MongoClient } = require("mongodb");
const { CSV_FILE, MONGO_URI, COLLECTION_NAME, DB_NAME } = process.env;

async function mongoHandler(data) {
  const client = new MongoClient(MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  try {
    await client.connect();
    db = client.db(DB_NAME);

    const res = await db
      .collection(COLLECTION_NAME)
      .insertMany(data, { upsert: true });
    console.log(res);
    console.log("vola!");
  } catch (e) {
    console.error(e);
  }
}

function turnKeyToLower(obj) {
  var key,
    keys = Object.keys(obj);
  var n = keys.length;
  var newobj = {};
  while (n--) {
    key = keys[n];
    newobj[key.toLowerCase()] = obj[key];
  }
  return newobj;
}

const results = [];

fs.createReadStream(CSV_FILE)
  .pipe(csv())
  .on("data", (data) => results.push(data))
  .on("end", () => {
    const newResult = [];
    results.forEach((item) => {
      let newObj = turnKeyToLower(item);
      newResult.push(newObj);
    });
    mongoHandler(newResult);
  });
