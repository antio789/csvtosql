const sqlite3 = require('sqlite3');
const db = new sqlite3.Database('substrate_demo.db');
const Papa = require('papaparse');
const fs = require('fs');
function addPretreatmentArticles (file){
    fs.readFile(file, "utf8", async (err, data) => {
        if (err) {
            console.error("Error reading CSV file:", err);
            throw err;
        }

        // Parse CSV
        const parsedData = Papa.parse(data, {
            header: true,         // Treat first row as headers
            dynamicTyping: true   // Convert numbers automatically
        });

        try {
            let count = 0;
            let nArticles = parsedData.data.length;
            for (const row of parsedData.data) {
                process.stdout.write(`\r working on pretreatment row: ${count}, progress at ${Math.floor((count++ / nArticles) * 1000) / 10}%`);
                //console.log(`working on row number: ${count++}`)
                if (row["doi link"]) {
                    const article = await check_article(row);
                    if (!article) {
                        //console.log(article, row["doi link"]);
                        //Article is not in the database
                        await insert_article(row);
                    }

                    const cat = await check_duplicate("pretreatment_fields", "name", row["pretreatment category"]);
                    if (!cat) {
                        await insert_field("pretreatment_fields", row["pretreatment category"]).catch(err => {
                            throw new Error(`could not add pretreatment CATEGORY with title ${row.Title}`, {cause: err})
                        });
                    }
                    await linkArticlePretreatment(row["doi link"], row["pretreatment category"]);

                    //insert pretreatment type(subcategory) and link field with upper category
                    const type = await check_duplicate("pretreatment_fields", "name", row["pretreatment type"]);
                    if (!type) {
                        await insert_field("pretreatment_fields", row["pretreatment type"]).catch(err => {
                            throw new Error(`could not add pretreatment TYPE with title ${row.Title}`, {cause: err})
                        });
                    }
                    await linkArticlePretreatment(row["doi link"], row["pretreatment type"]);
                    await linkPretreatmentFields(row["pretreatment category"], row["pretreatment type"]).catch(err => {
                        throw new Error(`could not link a pretreatment category with a type ${row.Title}`, {cause: err})
                    });

                    await insert_field("substrate_category", row["substrate category"]).catch(err => {
                        throw new Error(`could not add new substrate category with title ${row.Title}`, {cause: err})
                    });

                    //add results.

                    const result = await add_articleResults(row["doi link"], row).catch(err => {
                        throw new Error(`could not add results on row: ${row.Title}`, {cause: err})
                    });
                    if (!result) console.log(`results ignored for article: ${row["Title"]}`);

                    //await add_articlesubstrate(row["doi link"], row).catch(err => { throw new Error(`could not add substrate to article with title ${row.Title}`, { cause: err }) });


                } else {
                    console.log(`no DOI on row with title: ${row.Title}`)
                }
            }
        } catch (e) {
            console.log(e);
        }

    });
}

function addReactorArticles(file){
    fs.readFile(file, "utf8", async (err, data) => {
        if (err) {
            console.error("Error reading CSV file:", err);
            return;
        }
        const parsedData = Papa.parse(data, {
            header: true,         // Treat first row as headers
            dynamicTyping: true   // Convert numbers automatically
        });
        try {
            let count = 0;
            let nArticles = parsedData.data.length;
            for (const row of parsedData.data) {
                process.stdout.write(`\r working on reactor row: ${count}, progress at ${Math.floor((count++ / nArticles) * 1000) / 10}%`);
                if (row["doi link"]) {
                    const article = await check_article(row);
                    if (!article) {
                        //console.log(article, row["doi link"]);
                        //Article is not in the database
                        await insert_article(row);
                    }
                    const cat = await check_duplicate("reactors", "name", row["reactor type"]);
                    if (!cat) {
                        await insert_field("reactors", row["reactor type"]).catch(err => {
                            throw new Error(`could not add pretreatment CATEGORY with title ${row.Title}`, {cause: err})
                        });
                    }
                    await linkArticleReactor(row["doi link"], row["reactor type"]);
                    if (row["reactor subtype"] != null) {
                        const type = await check_duplicate("reactors", "name", row["reactor subtype"]);
                        if (!type) {
                            await insert_field("reactors", row["reactor subtype"]).catch(err => {
                                throw new Error(`could not add pretreatment TYPE with title ${row.Title}`, {cause: err})
                            });
                            await linkReactorFields(row["reactor type"], row["reactor subtype"]).catch(err => {
                                throw new Error(`could not link a pretreatment category with a type ${row.Title}`, {cause: err})
                            });
                        }
                        await linkArticleReactor(row["doi link"], row["reactor subtype"]);
                    }
                    await insert_field("substrate_category", row["substrate category"]).catch(err => {
                        throw new Error(`could not add new substrate category with title ${row.Title}`, {cause: err})
                    });

                    const result = await add_ReactorArticleData(row["doi link"], row).catch(err => {
                        throw new Error(`could not add results on row: ${row.Title}`, {cause: err})
                    });
                    if (!result) console.log(`results ignored for article: ${row["Title"]}`);

                }
                else {
                    console.log(`no DOI on row with title: ${row.Title}`)
                }

            }

        }catch (e) {
            console.log(e);
        }


    });
}

function add_articleResults(doi, row) {
    return new Promise(async function (res, reject) {
        const articleID = await getIDfromDOI(doi).catch(err => console.log(err));
        const catID = await getIDfromSubCategory(row["substrate category"]).catch(err => console.log(err));
        const preCatID = await getPretreatmentID(row["pretreatment category"]).catch(err => console.log(err));
        const preTypeID = await getPretreatmentID(row["pretreatment type"]).catch(err => console.log(err));
        const values = [];
        let isResults = false;
        for (const key in row) {
            if (key === "substrate name") isResults = true;
            if (isResults) {
                values.push(row[key]);
            }
        }
        if (values.every(value => value === null)) {
            console.log(`All values are null for row: ${row["Title"]}`);
            res(null);
        }
        else {
            db.serialize(() => {
                db.run(`INSERT OR IGNORE INTO article_content ("substrate name","substrate type",TS,VS,TC,TN,"C/N",cellulose,"hemi-cellulose",lignin,"article_id","category_id","precat_id","pretype_id","pretreatment detail") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [...values, articleID.id,catID.id,preCatID.id,preTypeID.id,row["pretreatment detail"]],
                    function (err) {
                        if (err) return reject(err);  // Reject if there's an error
                        res(this.lastID || null); // Resolve lastID or null if ignored
                    });
            })
        }
    })
}

function add_ReactorArticleData(doi, row) {
    return new Promise(async function (res, reject) {
        const articleID = await getIDfromDOI(doi).catch(err => console.log(err));
        const catID = await getIDfromSubCategory(row["substrate category"]).catch(err => console.log(err));
        const reactorID = await getReactorID(row["reactor type"]).catch(err => console.log(err));
        db.serialize(() => {
            db.run(`INSERT OR IGNORE INTO article_content ("substrate name","substrate type","article_id","category_id","reactorcat_id","reactor detail") VALUES (?,?,?,?,?,?)`, [row["substrate"],row["substrate category"], articleID.id,catID.id,reactorID.id,row["reactor detail"]],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    res(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function linkArticlePretreatment(doi, field) {
    return new Promise(async function (resolve, reject) {
        const articleID = await getIDfromDOI(doi).catch(err => console.log(err));
        const fieldID = await getPretreatmentID(field).catch(err => console.log(err));
        db.serialize(() => {
            db.run(`INSERT INTO article_pretreatment (article_id,field_id) VALUES (?,?)`, [articleID.id, fieldID.id],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    resolve(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function linkArticleReactor(doi, field) {
    return new Promise(async function (resolve, reject) {
        const articleID = await getIDfromDOI(doi).catch(err => console.log(err));
        const fieldID = await getReactorID(field).catch(err => console.log(err));
        db.serialize(() => {
            db.run(`INSERT INTO article_reactor (article_id,field_id) VALUES (?,?)`, [articleID.id, fieldID.id],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    resolve(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function getReactorID(field_name) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT "id" FROM reactors WHERE name = ?`, [field_name], (err, row) => {
                if (err) { return reject(err); }
                if (!row) { return reject(`row not found: ${field_name}`) }
                resolve(row);
            });
        })
    })
}

function insert_field(table, value) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.run(`INSERT OR IGNORE INTO "${table}" (name) VALUES (?)`, [value],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    resolve(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function linkPretreatmentFields(parent, child) {
    return new Promise(async function (resolve, reject) {
        const parentID = await getPretreatmentID(parent).catch(err => console.log(err));
        const childID = await getPretreatmentID(child).catch(err => console.log(err));
        db.serialize(() => {
            db.run(`INSERT OR IGNORE INTO pretreatment_field_tree (parent,child) VALUES (?,?)`, [parentID.id, childID.id],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    resolve(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function linkReactorFields(parent, child) {
    return new Promise(async function (resolve, reject) {
        const parentID = await getReactorID(parent).catch(err => console.log(err));
        const childID = await getReactorID(child).catch(err => console.log(err));
        db.serialize(() => {
            db.run(`INSERT OR IGNORE INTO reactor_tree (parent,child) VALUES (?,?)`, [parentID.id, childID.id],
                function (err) {
                    if (err) return reject(err);  // Reject if there's an error
                    resolve(this.lastID || null); // Resolve lastID or null if ignored
                });
        })
    })
}

function getIDfromDOI(doi) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT id FROM Articles WHERE doi = ?`, [doi], (err, rows) => {
                if (err) { return reject(err); }
                resolve(rows);
            });
        })
    })
}

function getIDfromSubCategory(name) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT id FROM substrate_category WHERE name = ?`, [name], (err, rows) => {
                if (err) { return reject(err); }
                resolve(rows);
            });
        })
    })
}

function getPretreatmentID(field_name) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT "id" FROM pretreatment_fields WHERE name = ?`, [field_name], (err, row) => {
                if (err) { return reject(err); }
                if (!row) { return reject(`row not found: ${field_name}`) }
                resolve(row);
            });
        })
    })
}

function insert_article(row) {
    return new Promise(function (resolve, reject) {
        if (row["doi link"]) {
            db.serialize(() => {
                db.run(`INSERT INTO Articles (title,abstract,doi,publication_year) VALUES ("${row.Title}", "${row.Abstract}", "${row["doi link"]}", "${row["Publication Year"]}")`,
                    function (err) {
                        if (!this.lastID) {
                            console.log(row["doi link"]);
                            return reject(err);
                        }
                        resolve(this.lastID);
                    });
            })
        }
        else { reject(`error with article missing data ${row.Title}, ${row["doi link"]}`) }
    })
}

function check_duplicate(table, field, row) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT "${field}" FROM "${table}" WHERE "${field}" = "${row}"`, (err, rows) => {
                if (err) { return reject(err); }
                resolve(rows);
            });
        })
    })
}

function check_article(row) {
    return new Promise(function (resolve, reject) {
        db.serialize(() => {
            db.get(`SELECT doi FROM Articles WHERE doi = "${row["doi link"]}"`, (err, rows) => {
                if (err) { return reject(err); }
                resolve(rows);
            });
        })
    })
}
/*
fs.readFile("sql-pretreatment.csv", "utf8", (err, data) => {
    if (err) {
        console.error(err);
        return;
    }

    Papa.parse(data, {
        header: true,
        dynamicTyping: true,
        complete: function (results) {
            console.log(results.data);
        }
    });
});
*/

addPretreatmentArticles("sql-pretreatment.csv");
//addReactorArticles("reactors final organised.csv");