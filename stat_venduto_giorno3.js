// gross_amount : importo VALORE (campo amount2 su tabelle db)
// net_amount : importo IPARTS (campo amount1 su tabelle db)

// Consolidamento statistiche venduto divisione gomme (da documenti fiscali restituiti da Valore per il periodo desiderato)
// DATABASE SVILUPPO/TEST
const Fetch = require('fetch');
const mysql = require('mysql2/promise');
//const md5 = require('md5');

const source_host = 'localhost';
const source_db = 'dati_servizi';
const source_user = 'dba';
const source_password = 'P1pp0';

async function main() {
    try {
      const source_connection = await mysql.createConnection({
        host: source_host,
        user: source_user,
        password: source_password,
        database: source_db,
      });
  
      // Rest of your code...
  
      source_connection.end(); // Close the database connection when done
    } catch (err) {
      console.error('Error connecting to database:', err);
    }
  }
  
  main();
  

// Set
const setDeposito = new Set([-809]);
const setNuovo = new Set([-801, -802, -803]);
const setNuovoMP = new Set([-825]);
const setUsato = new Set([-125, -192, -193, -194, -557, -790, -792, -793]);
const setUsatoMP = new Set([-645]);

// Se si modifica, cambiare valori select anche in componente react per gestione tabella mappature
const des_servizi = ['PACK SERVIZI/DEPOSITI (numero servizi)', 'MONTAGGI GOMME (numero servizi)', 'MONTAGGI MECCANICA (numero servizi)', 'TAGLIANDI', 'REVISIONI VEICOLO', 'REVISIONI IMPIANTO GAS', 'CONTROLLI IMPIANTO GAS', 'RIPARAZIONI (numero servizi)', 'CONVERGENZE (numero servizi)', 'BILANCIATURE (numero servizi)', 'OLIO/LIQUIDI', 'ALTRO (GOMME)', 'ALTRO (MECCANICA)', 'RITIRO AUTO'];

var paramDate = process.argv.length > 2 ? process.argv[2] : null;
var dt = paramDate != null ? parseInt(paramDate) : getCurrentDateNum();
var param_data = paramDate.substr(6,2) + '-' + paramDate.substr(4, 2) + '-' + paramDate.substr(0, 4);
var param_data_rev = paramDate.substr(0, 4) + '-' + paramDate.substr(4, 2) + '-' + paramDate.substr(6, 2);

var uriApiValore = `http://192.168.0.106/ValoreStats/webs/contabilita/documenti-giorno.aspx?sede=DIVISIONE GOMME&da_data=${param_data}&a_data=${param_data}`;

if (isNaN(dt)) {
        console.log("Missing argument");
        exit(0);
}
// Esecuzione Promise principale
	init().then((data) => {
		let promiseMapServizi = getMapServizi();
		promiseMapServizi.then(([mapServiziData]) => {
		  processaDatiGestionali(data, mapServiziData);
		}).catch((error) => {
		  console.error('Error retrieving mapServiziData:', error);
		});
	  }).catch((error) => {
		console.error('Error initializing:', error);
	  });

function processaDatiGestionali(data, mapServiziData) {

    if (mapServiziData.length !=1 || data.length == undefined) {
      console.error("Mappatura servizi iParts mancante oppure nessun dato restituito da Valore");
      return;
    }

    let aServizi = mapServiziData[0].mapping_data.mapping_data;

    elabora(data, aServizi);
}

function elabora(data, aServizi) {

      let mapOrdini = new Map();

      let mapOrdiniProm = fillMapGommeImportiOrdini(data, mapOrdini);
      Promise.all(mapOrdiniProm).then(() => { // Attende completamento selezione operatori ordine

        // controllo ordini non assegnati
        for (const[k, v] of mapOrdini) {
            if (v.operatore == ' NON ASSEGNATO')
                console.warn('Ordine non assegnato: ', k);
        };

        let valValore=0;
        let qta = 0;
        let map = new Map();
        let mapUtenti = new Map();
        let insProm = new Array();
        let indice_servizio = -1;

        // let totaleControllo = 0;
        
        mapUtenti.set(' NON ASSEGNATO', {vector: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], stat: {num_gomme: 0, imp_gomme: 0.0, imp_servizi: 0.0}});

        data.forEach((line) => {
           if ((line.tipodoc == 'Scontrino' || line.tipodoc == 'Fattura') && line.ordine != 0 /*  && line.Quantita > 0 */ ) {

                let isgomma = setNuovo.has(line.cod_ania) || setNuovoMP.has(line.cod_ania) || setUsato.has(line.cod_ania) || setUsatoMP.has(line.cod_ania) || setDeposito.has(line.cod_ania);
                let isservizio = line.cod_ania == null || line.cod_ania == 0;
                let imp = line.lordo;


		// TEMP DEBUG
		// console.warn(imp);
		// totaleControllo += imp;


                let v = mapOrdini.get(line.ordine);
	        if (v == undefined) {
	            console.error('Valore mappa ordini inesistente!: ', line.ordine);
	        } else {
	        
	            if (isgomma) {
	                v.imp_gomme+=imp;
	                v.num_gomme++;
	            }

	            if (isservizio) {
	                v.isservizio = true;
       	       	        v.imp_servizi+=imp;
	            }

                    mapOrdini.set(line.ordine, v);

			// Calcoli riga
	        	qta = line.Quantita;
			valValore = line.lordo;

			// Pre-controllo descrizione per raggruppamento 'speciale' - DISABILITATO PERCHE' DOVREBBERO ESSERE A POSTO I GRUPPI ANIA DEI MATERIALI

	// E.R. 26/4/2023 - controllo diretto codice ania - OVERRIDE KEY *************************************
			let codAnia = line.cod_ania;
			let key = line.desc_ania;

	                let val = null;
			if (setNuovo.has(codAnia))
			  key = "GOMME NUOVE";
			else if (setNuovoMP.has(codAnia))
			  key = "GOMME NUOVE MEZZI P.";
			else if (setUsato.has(codAnia))
			  key = "GOMME USATE";
			else if (setUsatoMP.has(codAnia))
			  key = "GOMME USATE MEZZI P.";
			else if (setDeposito.has(codAnia)) {
			  key = "GOMME A DEPOSITO";
			}
			if (isservizio) {
	                  let primo = true;
     			  let idservizio = line.cartellino; // API Corrado, modifica richiesta anche a iParts
                          let found = false;
			  aServizi.forEach(elem => {
                            if (elem.cod == idservizio) {
                                found = true;

			  	key = elem.gruppo;
			  	indice_servizio = des_servizi.indexOf(key);
				val = map.get(key);
	                        if (val == undefined) {
	        	        // Inizializza valori
                      	            val = {group: key, net_amount: 0, gross_amount: 0, qty: 0, qtyIP: 0, details: new Array() };
		                } 
	        	        //  Accumula valori
                	        val.gross_amount += primo ? valValore : 0;
//			        val.qty += qta;
			        val.qty += 1;

			        val.details.push({item_descr: line.descri, net_amount: 0, gross_amount: primo ? valValore : 0, qty: 1, num_ord: line.ordine});

			        // Inserisci o aggiorna gruppo
			        map.set(key, val);

                                //console.log(key, val);

				// Aggiorna mappa operatori
				// Incrementa indicatori operatore
				let mapVal = mapUtenti.get(v.operatore);
                                if (mapVal == undefined) {
                                    mapVal = {vector: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], stat: {num_gomme: 0, imp_gomme: 0.0, imp_servizi: 0.0}}
        			}

			        if (indice_servizio > -1) {
			            mapVal.vector[indice_servizio]++;
			            // mapVal.imp_servizi += primo ? valValore : 0; // il valore dei servizi per operatore è accumulato confrontando il corrispondente operatore nella mappa ordini
				}

				mapUtenti.set(v.operatore, mapVal);
				primo = false;
			    }
			  });
			  if (!found) {
			  	key = ' SENZA CODICE ANIA';
				val = map.get(key);
	                        if (val == undefined) {
	        	        // Inizializza valori
                      	            val = {group: key, net_amount: 0, gross_amount: 0, qty: 0, qtyIP: 0, details: new Array() };
		                } 
	        	        //  Accumula valori
                	        val.gross_amount += valValore;
			        val.qty += qta;
			        val.details.push({item_descr: line.descri, net_amount: 0, gross_amount: valValore, qty: qta, num_ord: line.ordine});
			        console.log(val);
       			        // Inserisci o aggiorna gruppo
			        map.set(key, val);
			  }
			} else { // Ania reali

	                    val = map.get(key);

	                    if (val == undefined) {
	                        // Inizializza valori

	                        val = {group: key, net_amount: 0, gross_amount: valValore, qty: qta, qtyIP: 0, details: new Array() };
	                    } else {
	                       //  Accumula valori
	                            val.gross_amount += valValore;
	                            val.qty += qta;
	                    }

		            val.details.push({item_descr: line.descri, net_amount: 0, gross_amount: valValore, qty: qta, num_ord: line.ordine});

		            // Inserisci o aggiorna gruppo
		            map.set(key, val);

		        }
                }
	   }
        });
// terza
	let countGommeIP = 0, valGommeIP = 0.0;
	let countGommeDeposito = 0, valGommeDeposito = 0.0;

// Calcola totali gomme da mappa ordini (Valore)
        let val_servizi = 0;
        let tot_gomme = 0, val_gomme=0, tot_gomme_non_mont = 0, val_gomme_non_mont = 0;
        for (const[k, v] of mapOrdini) {
            if (v.isservizio) {
                tot_gomme += v.num_gomme;
                val_gomme += v.imp_gomme;
                val_servizi += v.imp_servizi;
            } else {
                tot_gomme_non_mont += v.num_gomme;
                val_gomme_non_mont += v.imp_gomme;
            }

	    let valOperatore = mapUtenti.get(v.operatore);
	    if (valOperatore == undefined)
                valOperatore = {vector: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], stat: {num_gomme: 0, imp_gomme: 0.0, imp_servizi: 0.0}};

	    valOperatore.stat.num_gomme += v.num_gomme;
	    valOperatore.stat.imp_gomme += v.imp_gomme;
	    valOperatore.stat.imp_servizi += v.imp_servizi;

	    mapUtenti.set(v.operatore, valOperatore);

        }
// Aggiunge totali su mappa.

        map.set("TOTALE GOMME VENDUTE E MONTATE", {group: "TOTALE GOMME VENDUTE E MONTATE", net_amount: valGommeIP, gross_amount: val_gomme, qty: tot_gomme, qtyIP: countGommeIP, details: new Array() });
        map.set("TOTALE GOMME VENDUTE NON MONTATE", {group: "TOTALE GOMME VENDUTE NON MONTATE", net_amount: valGommeIP, gross_amount: val_gomme_non_mont, qty: tot_gomme_non_mont, qtyIP: countGommeIP, details: new Array() });

        mapToDb(dt, map);
        //console.log(map);
       	mapUtentiToDb(dt, mapUtenti);
	//console.log(mapUtenti);

        // console.warn('Totale controllo', totaleControllo);

      });

}
//seconda
function init() {
	let p = documentiValore(encodeURI(uriApiValore));
	return p;
  }
  
  function getMapServizi() {
	return new Promise((resolve, reject) => {
	  let sql = "SELECT * FROM `mapping-servizi-stat` WHERE cod=0";
	  connection.query(sql, (error, results) => {
		if (error) {
		  reject(error);
		  return;
		};
	});
  });

}
		resolve(results);


function getCurrentDateNum() {
	return new Date().toISOString().split("T")[0].replaceAll("-", "");
}

function documentiValore(uri) {
let p = new Promise((complete, fail) => {
       	Fetch.fetchUrl(uri, {method: 'GET', headers: { 'Content-Type' : 'application/json'}}, (error, meta, body) => {
       	  if(error)
            fail({errmsg: error});
          else {
            complete(JSON.parse(body));
          }
        });
});
return p;
}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
function documentiIParts(uri) {
let p = new Promise((complete, fail) => {
       	Fetch.fetchUrl(uri, {method: 'GET', headers: { 'Content-Type' : 'application/json'}}, (error, meta, body) => {
       	  if(error)
            fail({errmsg: error});
          else {
            complete(JSON.parse(body));
          }
        });
});
return p;
}

function mapToDb(dt, map) {

	for (const [k, v] of map) {
		let actualParam = `(${dt}, '${k}', ${v.net_amount}, ${v.gross_amount}, ${v.qty}, ${v.qtyIP})`;
  
		connection.query(
		  'REPLACE INTO `daily-stat-map` (num_day, grp, amount1, amount2, qty, qtyIP) VALUES ' +
			actualParam,
		  (error, results) => {
			if (error) {
			  console.error(error);
			  return;
			}
  
			let idx = 1;
			v.details.forEach((item) => {
			  let subActualParam = `(${dt}, '${k}', ${idx}, '${item.item_descr}', ${item.gross_amount}, ${item.qty}, ${item.num_ord})`;
  
			  connection.query(
				'REPLACE INTO `daily-stat-map-det` (num_day, grp, idx, subgrp, amount, qty, num_ord) VALUES ' +
				  subActualParam,
				(error, results) => {
				  if (error) {
					console.error(error);
				  }
				}
			  );
  
			  idx++;
			});
		  }
		);
	  }
  
	  connection.end(); // Close the database connection after all queries are executed
	};

function mapUtentiToDb(dt, map) {

	for (const [k, v] of map) {
		let actualParam = `(${dt}, '${k}', ${v.vector[0]}, ${v.vector[1]}, ${v.vector[2]}, ${v.vector[3]}, ${v.vector[4]}, ${v.vector[5]}, ${v.vector[6]}, ${v.vector[7]}, ${v.vector[8]}, ${v.vector[9]}, ${v.vector[10]}, ${v.vector[11]}, ${v.vector[12]}, ${v.vector[13]}, ${v.stat.num_gomme}, ${v.stat.imp_gomme}, ${v.stat.imp_servizi})`;
  
		connection.query(
		  'REPLACE INTO `daily-user-svc` (num_day, user, counter0, counter1, counter2, counter3, counter4, counter5, counter6, counter7, counter8, counter9, counter10, counter11, counter12, counter13, num_gomme, imp_gomme, imp_servizi) VALUES ' +
			actualParam,
		  (error, results) => {
			if (error) {
			  console.error(error);
			}
		  }
		);
	  }
	};
  
//quarta
// 23/3/2023 E.R. - calcolo numero gomme e relativo importo per statistica operatori

function fillMapGommeImportiOrdini(data, map) {

let pArr = new Array();
    data.forEach((line) => {
        if ((line.tipodoc == 'Scontrino' || line.tipodoc == 'Fattura') && line.ordine != 0 /* && line.Quantita > 0 */ ) {

                let p = getPrimoOperatoreOrdine(line.ordine);
                p.then(data => {
                    if (data.length > 0) {
                    	map.set(line.ordine, {num_gomme: 0, imp_gomme: 0.0, imp_servizi: 0.0, isservizio: false, operatore: data[0].utente});
                    	}
                    else
                    	map.set(line.ordine, {num_gomme: 0, imp_gomme: 0.0, imp_servizi: 0.0, isservizio: false, operatore: ' NON ASSEGNATO'});
                });
                pArr.push(p);
        }
    });
    return pArr;

}
function getPrimoOperatoreOrdine(id_ordine) {
	return new Promise((resolve, reject) => {
	  let sql = `SELECT ord, utente FROM LavorazioniUtenteGiorno WHERE num_day=${dt} AND ord='${id_ordine}'`;
  
	  connection.query(sql, (error, results) => {
		if (error) {
		  console.error(error);
		  reject(error);
		  return;
		}
  
		resolve(results);
	  });
	});
  }
  