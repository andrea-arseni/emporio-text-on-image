const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const sharp = require('sharp');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });

let connection = null;

mysql.createConnection({
    host     : process.env.RDS_HOSTNAME,
    user     : process.env.RDS_USERNAME,
    password : process.env.RDS_PASSWORD,
    database : process.env.RDS_DB_NAME 
}).then(con=>connection = con);

exports.handler = async (event) => {
    
    // connect to mysql
    if(!connection){
        connection = await mysql.createConnection({
            host     : process.env.RDS_HOSTNAME,
            user     : process.env.RDS_USERNAME,
            password : process.env.RDS_PASSWORD,
            database : process.env.RDS_DB_NAME 
        })
    }
    
    // get idImmobile, se non c'è throw error
    if(!event.pathParameters || !event.pathParameters.idImmobile) 
    return throwError("Parametro idImmobile obbligatorio");
    const { idImmobile } = event.pathParameters;

    // se non è corretto throw error
    if(isNaN(idImmobile) || idImmobile <= 0) 
    return throwError("Parametro idImmobile non corretto, deve essere un numero positivo");

    // get tipologia and colore
    if(!event.body) return throwError("Necessario avere il corpo della richiesta");
    const reqBody = JSON.parse(event.body);

    if(!reqBody.tipologia || !reqBody.colore) return throwError("Il corpo della richiesta deve contenere i campi 'tipologia' e 'colore'");
    const { tipologia, colore } = reqBody;
    
    // if body incorrect throw error
    if(tipologia!=='venduto' && tipologia!=='affittato') return throwError("Tipologia non corretta: può essere solo 'venduto' o 'affittato'");
    if(colore!=='verde' && colore!=='bianco') return throwError("Colore non corretto: può essere solo 'verde' o 'bianco'");
    
    // check che esista un file con idImmobile and nome 0, se c'è throw error
    const retrieveFirstPhoto = `SELECT codice_bucket FROM file WHERE immobile = ${idImmobile} AND nome = '1'`;
    const results = await connection.execute(retrieveFirstPhoto); 
    if(!results[0][0]) return throwError(`Impossibile segnalare immobile ${tipologia} senza almeno una foto`);

    const originalFileDB = results[0][0];

    // get ref
    const ref = originalFileDB.codice_bucket.split('/')[0];

    // get bucket file, se non esiste throw error
    const file = await readFileFromS3(originalFileDB.codice_bucket);

    // inizializza sharp
    const image = sharp(file);

    // add dark layer
    image.modulate({
    brightness: 0.7,
    saturation: 0.7
    });

    // check dimension in too little case enlarge
    const {width, height} = await image.metadata();

    // se una dimensione è < 700 prendi la più piccola e la porti a 700
    if(width<700 || height<700){
        const options = width <= height ? { width: 700 } : {height: 700};
        await image.resize(options);
    }

    // retrieve other file
    const key = `emporio/${tipologia}-${colore}.png`;
    const scritta = await readFileFromS3(key);

    // composite 
    image.composite([{ input: scritta}]);

    const fileElaborato = await image.toBuffer();

    // write new file on signed photos S3 bucket
    params = {
        Bucket: process.env.BUCKET_NAME,
        Key: ref+'/done.png',
        Body: fileElaborato,
        ContentType: process.env.CONTENT_TYPE
    }

    await s3.upload(params).promise();

    // update name 0 to name 21 + save newFile as name 0 and code done.png 
    const updateFirstPhoto = `UPDATE \`file\` SET nome = '21' WHERE immobile = ${idImmobile} AND nome = '1'`;
    const createRecord = `INSERT INTO \`file\` (immobile, tipologia, nome, codice_bucket) VALUES (${idImmobile}, 'FOTO', '1', 'done.png')`;

    await connection.query('START TRANSACTION');
    try{
        await connection.execute(updateFirstPhoto);
        await connection.execute(createRecord);
    }catch(e){
        await connection.query('ROLLBACK');
        throwError('Errore nelle query di aggiornamento');
    }
    await connection.commit();
    
    // TODO implement
    const response = {
        statusCode: 200,
        body: "Firma avvenuta con successo",
    };
    return response;
};

const throwError = (message)=>{
    return {
        "statusCode": 400,
        "body": message,
        "isBase64Encoded": false
    }
}

const readFileFromS3 = async(Key)=> {
    const params = {
        Bucket: process.env.BUCKET_NAME,
        Key,
    };
    try{
        const {Body} = await s3.getObject(params).promise();
        return Body;
    }catch(e){
        throwError('File non trovato, operazione annullata');
    }
}