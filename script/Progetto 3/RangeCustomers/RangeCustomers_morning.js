const mongoose = require('mongoose');

const fila_morning = new mongoose.Schema({
    title: String,
    url: String,
    details: String,
}, { collection: 'ted_in_fila_morning' });

module.exports = mongoose.model('fila_morning', fila_morning);

