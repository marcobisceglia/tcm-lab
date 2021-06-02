// talk funziona da model

// importo libreria mongoose per usare mongodb
const mongoose = require("mongoose");

// definisco lo schema del talk
const talk_schema = new mongoose.Schema(
  {
    _id: String,
    title: String,
    url: String,
    watch_next: Array,
  },
  { collection: "tedx_data" }
);

module.exports = mongoose.model("talk", talk_schema);
