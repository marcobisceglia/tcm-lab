// GET TEDX BY INTERNATIONAL DAY HANDLER
// handler funziona da controller

// importa il file db.js e Talk.js
const connect_to_db = require("./db");
const talk = require("./Talk");
const day = require("./Day");

// data una stringa ritorna una lista di parole filtrate dalla seconda lista
const getKeywords = (wordsListStr, wordsToRemove) =>
  wordsListStr
    .split(" ")
    .map((el) => el.toLowerCase())
    .filter((el) => !wordsToRemove.includes(el));

// importo parole che mi permettono di filtrare i nomi delle giornate internazionali
// per ottenere parole chiave
const fs = require("fs");
var file = fs.readFileSync("./words_to_remove.txt").toString();
const WORDS_TO_REMOVE = file.split("\n");

// event conterrà il payload
module.exports.get_tedx_by_international_day = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;

  // stampo il contenuto di ciò che ricevo
  console.log("Received event:", JSON.stringify(event, null, 2));

  // body conterrà il payload, quindi i parametri che passo all'api
  let body = {};
  if (event.body) {
    body = JSON.parse(event.body);
  }

  // set default
  if (!body.day) {
    callback(null, {
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not fetch ted talks. Day is null.",
    });
  }

  // collegamento al database
  connect_to_db().then(() => {
    // ogni giorno ha un array con le keywords
    let keywordsForDay = [];

    // array di tutte le keywords
    let keywords = [];

    // giorni trovati
    let days = [];

    console.log("QUERY INTERNATIONAL DAYS");
    day
      .find({ date: body.day })
      .then((response) => {
        if (response == null || response == "") {
          callback(null, {
            statusCode: 404,
            headers: { "Content-Type": "text/plain" },
            body: "Could not fetch ted talks. No events for this day.",
          });
        }

        // stampo risposta
        console.log(response);

        for (let el of response) {
          days.push(el.event);
          keywordsForDay.push(getKeywords(el.event, WORDS_TO_REMOVE));
        }

        // stampo giorni trovati
        console.log(days);

        keywordsForDay.forEach((arr) =>
          arr.forEach((word) => keywords.push(word))
        );

        // stampo tutte le parole chiave
        console.log(keywords);
      })
      .catch((err) =>
        callback(null, {
          statusCode: err.statusCode || 500,
          headers: { "Content-Type": "text/plain" },
          body: "Could not fetch international days data",
        })
      )
      .then(() => {
        console.log("QUERY TEDX TALKS");

        talk
          .find({ tags: { $in: keywords } })
          .then((response) => {
            console.log(response);

            let teds = [];
            response.forEach((el) => teds.push(el.url));

            // costruisco oggetto da restituire
            let result = {};
            result["day"] = days;
            result["tags"] = keywords;
            result["teds"] = teds;

            callback(null, {
              statusCode: 200,
              body: JSON.stringify(result),
            });
          })
          .catch((err) =>
            callback(null, {
              statusCode: err.statusCode || 500,
              headers: { "Content-Type": "text/plain" },
              body: "Could not fetch teds data",
            })
          );
      });
  });
};
