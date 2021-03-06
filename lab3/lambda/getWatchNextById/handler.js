// GET WATCH NEXT BY ID HANDLER
// handler funziona da controller

// importa il file db.js e Talk.js
const connect_to_db = require("./db");
const talk = require("./Talk");

// event conterrà il payload
module.exports.get_watch_next_by_id = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;

  // stampo il contenuto di ciò che ricevo
  console.log("Received event:", JSON.stringify(event, null, 2));

  // body conterrà il payload, quindi i parametri che passo all'api
  let body = {};
  if (event.body) {
    body = JSON.parse(event.body);
  }

  // set default
  if (!body.id) {
    callback(null, {
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not fetch the watch next talks. Id is null.",
    });
  }

  // collegamento al database
  // .find(array: el) ritorna un array con solo el all'interno
  connect_to_db().then(() => {
    console.log("=> get_all watch next talks");
    talk
      .find({ _id: body.id })
      .then((response) => {
        callback(null, {
          statusCode: 200,
          body: JSON.stringify({
            name: response[0].title,
            watch_next: response[0].watch_next,
          }),
        });
      })
      .catch((err) =>
        callback(null, {
          statusCode: err.statusCode || 500,
          headers: { "Content-Type": "text/plain" },
          body: "Could not fetch the watch next talks.",
        })
      );
  });
};
