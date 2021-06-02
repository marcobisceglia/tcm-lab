// day funziona da model

// importo libreria mongoose per usare mongodb
const mongoose = require("mongoose");

// definisco lo schema della giornata internazionale
const day_schema = new mongoose.Schema(
  {
    date: String,
    event: String,
    url: String,
    doc: String,
    url_doc: String,
  },
  { collection: "international_days_data" }
);

module.exports = mongoose.model("day", day_schema);
