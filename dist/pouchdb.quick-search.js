"use strict";
import mapReduce from "pouchdb-mapreduce-no-ddocs";
Object.keys(mapReduce).forEach(function(key) {
  exports[key] = mapReduce[key];
});
import utils from "./pouch-utils";
import lunr from "lunr";
import uniq from "uniq";
var PouchPromise = utils.PouchPromise;
import stringify from "json-stable-stringify";
var indexes = {};
var TYPE_TOKEN_COUNT = "a";
var TYPE_DOC_INFO = "b";
function add(left, right) {
  return left + right;
}
function getTokenStream(text, index) {
  return index.pipeline.run(lunr.tokenizer(text));
}
function getText(fieldBoost, doc) {
  var text;
  if (!fieldBoost.deepField) {
    text = doc[fieldBoost.field];
  } else {
    text = doc;
    for (var i = 0, len = fieldBoost.deepField.length; i < len; i++) {
      if (Array.isArray(text)) {
        text = text.map(handleNestedObjectArrayItem(fieldBoost, fieldBoost.deepField.slice(i)));
      } else {
        text = text && text[fieldBoost.deepField[i]];
      }
    }
  }
  if (text) {
    if (Array.isArray(text)) {
      text = text.join(" ");
    } else if (typeof text !== "string") {
      text = text.toString();
    }
  }
  return text;
}
function handleNestedObjectArrayItem(fieldBoost, deepField) {
  return function(one) {
    return getText(utils.extend({}, fieldBoost, {
      deepField
    }), one);
  };
}
function createMapFunction(fieldBoosts, index, filter, db) {
  return function(doc, emit) {
    if (isFiltered(doc, filter, db)) {
      return;
    }
    var docInfo = [];
    for (var i = 0, len = fieldBoosts.length; i < len; i++) {
      var fieldBoost = fieldBoosts[i];
      var text = getText(fieldBoost, doc);
      var fieldLenNorm;
      if (text) {
        var terms = getTokenStream(text, index);
        for (var j = 0, jLen = terms.length; j < jLen; j++) {
          var term = terms[j];
          var value = fieldBoosts.length > 1 ? i : void 0;
          emit(TYPE_TOKEN_COUNT + term, value);
        }
        fieldLenNorm = Math.sqrt(terms.length);
      } else {
        fieldLenNorm = 0;
      }
      docInfo.push(fieldLenNorm);
    }
    emit(TYPE_DOC_INFO + doc._id, docInfo);
  };
}
exports.search = utils.toPromise(function(opts, callback) {
  var pouch = this;
  opts = utils.extend(true, {}, opts);
  var q = opts.query || opts.q;
  var mm = "mm" in opts ? parseFloat(opts.mm) / 100 : 1;
  var fields = opts.fields;
  var highlighting = opts.highlighting;
  var includeDocs = opts.include_docs;
  var destroy = opts.destroy;
  var stale = opts.stale;
  var limit = opts.limit;
  var build = opts.build;
  var skip = opts.skip || 0;
  var language = opts.language || "en";
  var filter = opts.filter;
  if (Array.isArray(fields)) {
    var fieldsMap = {};
    fields.forEach(function(field) {
      fieldsMap[field] = 1;
    });
    fields = fieldsMap;
  }
  var fieldBoosts = Object.keys(fields).map(function(field) {
    var deepField = field.indexOf(".") !== -1 && field.split(".");
    return {
      field,
      deepField,
      boost: fields[field]
    };
  });
  var index = indexes[language];
  if (!index) {
    index = indexes[language] = lunr();
    if (Array.isArray(language)) {
      index.use(global.lunr["multiLanguage"].apply(this, language));
    } else if (language !== "en") {
      index.use(global.lunr[language]);
    }
  }
  var indexParams = {
    language,
    fields: fieldBoosts.map(function(x) {
      return x.field;
    }).sort()
  };
  if (filter) {
    indexParams.filter = filter.toString();
  }
  var persistedIndexName = "search-" + utils.MD5(stringify(indexParams));
  var mapFun = createMapFunction(fieldBoosts, index, filter, pouch);
  var queryOpts = {
    saveAs: persistedIndexName
  };
  if (destroy) {
    queryOpts.destroy = true;
    return pouch._search_query(mapFun, queryOpts, callback);
  } else if (build) {
    delete queryOpts.stale;
    queryOpts.limit = 0;
    pouch._search_query(mapFun, queryOpts).then(function() {
      callback(null, { ok: true });
    }).catch(callback);
    return;
  }
  var queryTerms = uniq(getTokenStream(q, index));
  if (!queryTerms.length) {
    return callback(null, { total_rows: 0, rows: [] });
  }
  queryOpts.keys = queryTerms.map(function(queryTerm) {
    return TYPE_TOKEN_COUNT + queryTerm;
  });
  if (typeof stale === "string") {
    queryOpts.stale = stale;
  }
  pouch._search_query(mapFun, queryOpts).then(function(res) {
    if (!res.rows.length) {
      return callback(null, { total_rows: 0, rows: [] });
    }
    var total_rows = 0;
    var docIdsToFieldsToQueryTerms = {};
    var termDFs = {};
    res.rows.forEach(function(row) {
      var term = row.key.substring(1);
      var field = row.value || 0;
      if (!(term in termDFs)) {
        termDFs[term] = 1;
      } else {
        termDFs[term]++;
      }
      if (!(row.id in docIdsToFieldsToQueryTerms)) {
        var arr = docIdsToFieldsToQueryTerms[row.id] = [];
        for (var i = 0; i < fieldBoosts.length; i++) {
          arr[i] = {};
        }
      }
      var docTerms = docIdsToFieldsToQueryTerms[row.id][field];
      if (!(term in docTerms)) {
        docTerms[term] = 1;
      } else {
        docTerms[term]++;
      }
    });
    if (queryTerms.length > 1) {
      Object.keys(docIdsToFieldsToQueryTerms).forEach(function(docId) {
        var allMatchingTerms = {};
        var fieldsToQueryTerms = docIdsToFieldsToQueryTerms[docId];
        Object.keys(fieldsToQueryTerms).forEach(function(field) {
          Object.keys(fieldsToQueryTerms[field]).forEach(function(term) {
            allMatchingTerms[term] = true;
          });
        });
        var numMatchingTerms = Object.keys(allMatchingTerms).length;
        var matchingRatio = numMatchingTerms / queryTerms.length;
        if (Math.floor(matchingRatio * 100) / 100 < mm) {
          delete docIdsToFieldsToQueryTerms[docId];
        }
      });
    }
    if (!Object.keys(docIdsToFieldsToQueryTerms).length) {
      return callback(null, { total_rows: 0, rows: [] });
    }
    var keys = Object.keys(docIdsToFieldsToQueryTerms).map(function(docId) {
      return TYPE_DOC_INFO + docId;
    });
    var queryOpts2 = {
      saveAs: persistedIndexName,
      keys,
      stale
    };
    return pouch._search_query(mapFun, queryOpts2).then(function(res2) {
      var docIdsToFieldsToNorms = {};
      res2.rows.forEach(function(row) {
        docIdsToFieldsToNorms[row.id] = row.value;
      });
      var rows = calculateDocumentScores(queryTerms, termDFs, docIdsToFieldsToQueryTerms, docIdsToFieldsToNorms, fieldBoosts);
      return rows;
    }).then(function(rows) {
      total_rows = rows.length;
      return typeof limit === "number" && limit >= 0 ? rows.slice(skip, skip + limit) : skip > 0 ? rows.slice(skip) : rows;
    }).then(function(rows) {
      if (includeDocs) {
        return applyIncludeDocs(pouch, rows);
      }
      return rows;
    }).then(function(rows) {
      if (highlighting) {
        return applyHighlighting(pouch, opts, rows, fieldBoosts, docIdsToFieldsToQueryTerms);
      }
      return rows;
    }).then(function(rows) {
      callback(null, { total_rows, rows });
    });
  }).catch(callback);
});
function calculateDocumentScores(queryTerms, termDFs, docIdsToFieldsToQueryTerms, docIdsToFieldsToNorms, fieldBoosts) {
  var results = Object.keys(docIdsToFieldsToQueryTerms).map(function(docId) {
    var fieldsToQueryTerms = docIdsToFieldsToQueryTerms[docId];
    var fieldsToNorms = docIdsToFieldsToNorms[docId];
    var queryScores = queryTerms.map(function(queryTerm) {
      return fieldsToQueryTerms.map(function(queryTermsToCounts, fieldIdx) {
        var fieldNorm = fieldsToNorms[fieldIdx];
        if (!(queryTerm in queryTermsToCounts)) {
          return 0;
        }
        var termDF = termDFs[queryTerm];
        var termTF = queryTermsToCounts[queryTerm];
        var docScore = termTF / termDF;
        var queryScore = 1 / termDF;
        var boost = fieldBoosts[fieldIdx].boost;
        return docScore * queryScore * boost / fieldNorm;
      }).reduce(add, 0);
    });
    var maxQueryScore = 0;
    queryScores.forEach(function(queryScore) {
      if (queryScore > maxQueryScore) {
        maxQueryScore = queryScore;
      }
    });
    return {
      id: docId,
      score: maxQueryScore
    };
  });
  results.sort(function(a, b) {
    return a.score < b.score ? 1 : a.score > b.score ? -1 : 0;
  });
  return results;
}
function applyIncludeDocs(pouch, rows) {
  return PouchPromise.all(rows.map(function(row) {
    return pouch.get(row.id);
  })).then(function(docs) {
    docs.forEach(function(doc, i) {
      rows[i].doc = doc;
    });
  }).then(function() {
    return rows;
  });
}
function applyHighlighting(pouch, opts, rows, fieldBoosts, docIdsToFieldsToQueryTerms) {
  var pre = opts.highlighting_pre || "<strong>";
  var post = opts.highlighting_post || "</strong>";
  return PouchPromise.all(rows.map(function(row) {
    return PouchPromise.resolve().then(function() {
      if (row.doc) {
        return row.doc;
      }
      return pouch.get(row.id);
    }).then(function(doc) {
      row.highlighting = {};
      docIdsToFieldsToQueryTerms[row.id].forEach(function(queryTerms, i) {
        var fieldBoost = fieldBoosts[i];
        var fieldName = fieldBoost.field;
        var text = getText(fieldBoost, doc);
        Object.keys(queryTerms).forEach(function(queryTerm) {
          var regex = new RegExp("(" + queryTerm + "[a-z]*)", "gi");
          var replacement = pre + "$1" + post;
          text = text.replace(regex, replacement);
          row.highlighting[fieldName] = text;
        });
      });
    });
  })).then(function() {
    return rows;
  });
}
function isFiltered(doc, filter, db) {
  try {
    return !!(filter && !filter(doc));
  } catch (e) {
    db.emit("error", e);
    return true;
  }
}
if (typeof window !== "undefined" && window.PouchDB) {
  window.PouchDB.plugin(exports);
}
