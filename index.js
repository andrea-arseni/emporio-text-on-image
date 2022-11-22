const aws = require("aws-sdk");
const mysql = require("mysql2/promise");
const sharp = require("sharp");
const s3 = new aws.S3({ apiVersion: "2006-03-01" });

let connection = null;

mysql
    .createConnection({
        host: process.env.RDS_HOSTNAME,
        user: process.env.RDS_USERNAME,
        password: process.env.RDS_PASSWORD,
        database: process.env.RDS_DB_NAME,
    })
    .then((con) => (connection = con));

exports.handler = async (event) => {
    // connect to mysql
    if (!connection) {
        connection = await mysql.createConnection({
            host: process.env.RDS_HOSTNAME,
            user: process.env.RDS_USERNAME,
            password: process.env.RDS_PASSWORD,
            database: process.env.RDS_DB_NAME,
        });
    }

    // get idImmobile, se non c'è throw error
    if (!event.pathParameters || !event.pathParameters.idImmobile)
        return throwError("Parametro idImmobile obbligatorio");
    const { idImmobile } = event.pathParameters;

    // se non è corretto throw error
    if (isNaN(idImmobile) || idImmobile <= 0)
        return throwError(
            "Parametro idImmobile non corretto, deve essere un numero positivo"
        );

    // get tipologia and colore
    if (!event.body)
        return throwError("Necessario avere il corpo della richiesta");
    const reqBody = JSON.parse(event.body);

    if (!reqBody.tipologia || !reqBody.colore)
        return throwError(
            "Il corpo della richiesta deve contenere i campi 'tipologia' e 'colore'"
        );
    const { tipologia, colore } = reqBody;

    // if body incorrect throw error
    if (tipologia !== "venduto" && tipologia !== "affittato")
        return throwError(
            "Tipologia non corretta: può essere solo 'venduto' o 'affittato'"
        );
    if (colore !== "red" && colore !== "blue")
        return throwError(
            "Colore non corretto: può essere solo 'red' o 'blue'"
        );

    // check esistenza immobile, se non c'è throw error
    const immobile = await retrieveImmobile(idImmobile);
    if (!immobile)
        return throwError(`Immobile non trovato. Impossibile procedere.`);

    // check che esista un file con idImmobile and nome 0 o 1, se non c'è throw error
    const originalFileDB = await checkForFirstPhoto(idImmobile, tipologia);
    if (!originalFileDB)
        return throwError(
            `Impossibile segnalare immobile "${tipologia}" senza almeno una foto`
        );

    // get ref
    const ref = originalFileDB.codice_bucket.split("/")[1];

    // get bucket file, se non esiste throw error
    const file = await readFileFromS3("signed", originalFileDB.codice_bucket);
    if (!file) return throwError("Foto non trovata, operazione annullata");

    // inizializza sharp
    const image = await sharp(file);

    // retrieve watermark
    const key = `emporio/${tipologia}-${colore}.png`;
    const fileScritta = await readFileFromS3("original", key);
    if (!fileScritta)
        return throwError("Scritta non trovata, operazione annullata");

    // check dimension, if too little case enlarge
    const { width } = await image.metadata();
    if (width < 1000) await image.resize({ width: 1000 });

    // composite
    await image.composite([{ input: fileScritta /* , gravity: "south" */ }]);

    const fileElaborato = await image.toBuffer();

    const codiceBucketFileConTesto = `immobili/${ref}/done.png`;

    // write new file on signed photos S3 bucket
    params = {
        Bucket: process.env.BUCKET_NAME_SIGNED,
        Key: codiceBucketFileConTesto,
        Body: fileElaborato,
        ContentType: process.env.CONTENT_TYPE,
    };

    await s3.upload(params).promise();

    const deletePreviousDoneRecord = `DELETE FROM \`file\` WHERE immobile = ${idImmobile} AND codice_bucket = '${codiceBucketFileConTesto}'  `;
    const createRecord = `INSERT INTO \`file\` (immobile, tipologia, nome, codice_bucket) VALUES (${idImmobile}, 'FOTO', '0', '${codiceBucketFileConTesto}')`;
    const createLog = `INSERT INTO \`log\` (immobile, azione, data) VALUES (${idImmobile}, 'Immobile concluso', '${new Date().toISOString()}')`;

    await connection.query("START TRANSACTION");
    try {
        await connection.execute(deletePreviousDoneRecord);
        await connection.execute(createRecord);
        await connection.execute(createLog);
    } catch (e) {
        console.log(e);
        await connection.query("ROLLBACK");
        return throwError("Errore nelle query di aggiornamento");
    }
    await connection.commit();

    const response = {
        statusCode: 200,
        headers: {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        body: "Firma avvenuta con successo",
    };
    return response;
};

const throwError = (message) => {
    return {
        statusCode: 400,
        headers: {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        body: message,
        isBase64Encoded: false,
    };
};

const readFileFromS3 = async (type, Key) => {
    const params = {
        Bucket:
            type === "original"
                ? process.env.BUCKET_NAME_ORIGINAL
                : process.env.BUCKET_NAME_SIGNED,
        Key,
    };
    const { Body } = await s3.getObject(params).promise();
    return Body ? Body : null;
};

const retrieveImmobile = async (idImmobile) => {
    const queryImmobile = `SELECT * FROM immobile WHERE id = ${idImmobile}`;
    const result = await connection.execute(queryImmobile);
    return result[0][0] ? result[0][0] : null;
};

const retrieveFile = async (idImmobile) => {
    let fotoQuery = `SELECT * FROM file WHERE immobile = ${idImmobile} AND tipologia = 'FOTO'`;
    const results = await connection.execute(fotoQuery);
    const listaFiles = results[0];
    if (!listaFiles || listaFiles.length === 0) return;
    listaFiles.sort((a, b) => +a.nome - b.nome);
    return listaFiles[0];
};

const checkForFirstPhoto = async (idImmobile) => {
    let firstPhoto = await retrieveFile(idImmobile);
    return firstPhoto;
};
