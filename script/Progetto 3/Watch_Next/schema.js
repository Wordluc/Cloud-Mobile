const mongoose = require('mongoose');

const ted = new mongoose.Schema({
    title: String,
    url: String,
    details: String,
    watch_next: Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('ted', ted);