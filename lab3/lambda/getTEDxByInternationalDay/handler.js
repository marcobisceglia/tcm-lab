// GET TEDX BY INTERNATIONAL DAY HANDLER

// import del file db.js e i modelli Talk.js e Day.js
const connect_to_db = require("./db");
const talk = require("./Talk");
const day = require("./Day");

// dato il nome dell'evento, ritorna un array con tutte le parole chiave
// escludendo le parole in wordsToRemove
const getKeywords = (event, wordsToRemove) =>
  event
    .split(" ")
    .map((el) => el.toLowerCase())
    .filter((el) => !wordsToRemove.includes(el));

// event conterrà il payload
module.exports.get_tedx_by_international_day = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;

  // stampo il contenuto di ciò che ricevo
  console.log("Received event:", JSON.stringify(event, null, 2));

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
    // conterranno tutte le giornate internazionali
    // e le parole chiave trovate
    let keywords = [];
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

        // stampo info
        console.log("International Days objects", response);

        // importo le parole da rimuovere dai nomi delle giornate internazionali
        // per ottenere le keywords
        const fs = require("fs");
        var file = fs.readFileSync("./words_to_remove.txt").toString();
        const WORDS_TO_REMOVE = file.split("\n");

        // response contiene gli oggetti dei giorni trovati
        // estraggo le parole chiave da ogni giorno
        response.forEach((dayObj) => {
          days.push(dayObj.event);
          getKeywords(dayObj.event, WORDS_TO_REMOVE).forEach((word) => {
            // una giornata internazionale potrebbe contenere la stessa
            // keyword già inserita per un'altra
            if (!keywords.includes(word)) {
              keywords.push(word);
            }
          });
        });

        // stampo info
        console.log("International Days", days);
        console.log("Keywords", keywords);
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
            let teds = [];
            response.forEach((ted) => teds.push(ted.url));

            console.log("TEDx Talks URLs", teds);

            // costruisco oggetto da restituire
            let result = {};
            result["days"] = days;
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
